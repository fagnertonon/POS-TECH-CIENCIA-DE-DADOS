"""AWS Glue 5.1: snapshot Bronze aprovado -> sete tabelas Silver em Parquet.

Nao consulta BigQuery nem une historicos. Le somente os sete target_uri do
manifesto Bronze escolhido. As saidas so sao publicadas como um conjunto quando
manifest.json SUCCEEDED existe; arquivos de uma tentativa sem manifesto nao sao
Silver publicada. O Catalog opcional so e atualizado apos validar as sete saidas;
seus metadados nao possuem transacao conjunta e consumidores usam o manifesto.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import reduce
import hashlib
import json
import logging
import re
import sys
import time
import uuid
from urllib.parse import urlsplit

VERSION = "1.0.0"
TABLES = ("dicionario", "municipio", "uf", "meta_alfabetizacao_brasil",
          "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "alunos")
DEFAULT_BRONZE_MANIFEST = (
    "s3://fiap-tc-fase2-2026/auditoria/bronze/ingestion_date=2026-10-20/"
    "run_id=jr_cdfd2933220ec75b45c04518c6e5afe93eb98915f3b88328f8bde8d7183d3700/manifest.json"
)
KEYS = {
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
    "municipio": ["ano", "id_municipio", "serie", "rede"],
    "uf": ["ano", "sigla_uf", "serie", "rede"],
    "meta_alfabetizacao_brasil": ["ano", "rede"],
    "meta_alfabetizacao_uf": ["ano", "sigla_uf", "rede"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio", "rede"],
    "alunos": ["ano", "id_aluno"],
}
REQUIRED = {
    "dicionario": ["id_tabela", "nome_coluna", "chave", "valor", "cobertura_temporal"],
    "municipio": ["ano", "id_municipio", "serie", "rede", "taxa_alfabetizacao", "media_portugues"]
                 + [f"proporcao_aluno_nivel_{i}" for i in range(9)],
    "uf": ["ano", "sigla_uf", "serie", "rede", "taxa_alfabetizacao", "media_portugues"]
          + [f"proporcao_aluno_nivel_{i}" for i in range(9)],
    "alunos": ["ano", "id_aluno", "id_municipio", "id_escola", "serie", "rede", "presenca",
               "preenchimento_caderno", "alfabetizado", "peso_aluno", "proficiencia"],
}
for _name in TABLES:
    if _name.startswith("meta_"):
        REQUIRED[_name] = list(KEYS[_name]) + ["taxa_alfabetizacao"] + [
            f"meta_alfabetizacao_{year}" for year in range(2024, 2031)]

LOG = logging.getLogger("fiap_silver")
F = Window = StorageLevel = None  # carregados apenas no runtime Spark/Glue


@dataclass(frozen=True)
class Config:
    job_name: str
    run_id: str
    bucket: str
    prefix: str
    audit_prefix: str
    bronze_manifest_uri: str
    limiar_join: float
    alunos_partitions: int
    database_silver: str
    register_catalog: bool


def parse_s3_uri(uri: str) -> tuple[str, str]:
    value = urlsplit(uri)
    if value.scheme != "s3" or not value.netloc or not value.path.strip("/") or value.query or value.fragment:
        raise ValueError(f"URI S3 invalida: {uri}")
    key = value.path.lstrip("/")
    if any(piece in {"", ".", ".."} for piece in key.rstrip("/").split("/")):
        raise ValueError("URI S3 tem componentes invalidos")
    return value.netloc, key


def clean_prefix(value: str) -> str:
    value = value.strip("/")
    if not value or not re.fullmatch(r"[A-Za-z0-9_./=-]+", value):
        raise ValueError("Prefixo S3 invalido")
    if any(piece in {"", ".", ".."} for piece in value.split("/")):
        raise ValueError("Prefixo S3 invalido")
    return value


def parse_bool(value: str) -> bool:
    if value.lower() in {"true", "1"}:
        return True
    if value.lower() in {"false", "0"}:
        return False
    raise ValueError("REGISTER_CATALOG deve ser true ou false")


def parse_config(argv: list[str]) -> Config:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--JOB_NAME", required=True)
    parser.add_argument("--JOB_RUN_ID", default="")
    parser.add_argument("--BUCKET", default="fiap-tc-fase2-2026")
    parser.add_argument("--PREFIX", default="silver/inep")
    parser.add_argument("--AUDIT_PREFIX", default="auditoria/silver")
    parser.add_argument("--BRONZE_MANIFEST_URI", default=DEFAULT_BRONZE_MANIFEST)
    parser.add_argument("--LIMIAR_JOIN", type=float, default=0.95)
    parser.add_argument("--ALUNOS_PARTITIONS", type=int, default=4)
    parser.add_argument("--DATABASE_SILVER", default="tech_challenge_fase2_silver")
    parser.add_argument("--REGISTER_CATALOG", default="true")
    args, _internal = parser.parse_known_args(argv)
    run_id = args.JOB_RUN_ID or ("slv_" + uuid.uuid4().hex)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", run_id):
        raise ValueError("JOB_RUN_ID invalido")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", args.BUCKET):
        raise ValueError("BUCKET invalido")
    prefix, audit_prefix = clean_prefix(args.PREFIX), clean_prefix(args.AUDIT_PREFIX)
    if prefix == audit_prefix or prefix.startswith(audit_prefix + "/") or audit_prefix.startswith(prefix + "/"):
        raise ValueError("Dados e auditoria devem ter prefixos separados")
    if not 0 < args.LIMIAR_JOIN <= 1 or not 1 <= args.ALUNOS_PARTITIONS <= 16:
        raise ValueError("LIMIAR_JOIN/ALUNOS_PARTITIONS fora dos limites")
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,127}", args.DATABASE_SILVER):
        raise ValueError("DATABASE_SILVER invalido")
    parse_s3_uri(args.BRONZE_MANIFEST_URI)
    return Config(args.JOB_NAME, run_id, args.BUCKET, prefix, audit_prefix,
                  args.BRONZE_MANIFEST_URI, args.LIMIAR_JOIN, args.ALUNOS_PARTITIONS,
                  args.DATABASE_SILVER, parse_bool(args.REGISTER_CATALOG))


def validate_bronze_manifest(manifest: dict) -> dict[str, dict]:
    """Contrato real do batch Bronze: table_metrics, nao a uniao de diretorios."""
    if manifest.get("status") != "SUCCEEDED" or manifest.get("is_full_batch") is not True:
        raise ValueError("Bronze precisa de manifesto SUCCEEDED e is_full_batch=true")
    if manifest.get("all_counts_match") is not True:
        raise ValueError("Bronze sem todas as contagens validadas")
    run_id = manifest.get("run_id", "")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", run_id):
        raise ValueError("run_id Bronze invalido")
    entries = manifest.get("table_metrics")
    if not isinstance(entries, list) or len(entries) != len(TABLES):
        raise ValueError("Manifesto Bronze precisa das sete table_metrics")
    result = {entry.get("table"): entry for entry in entries if isinstance(entry, dict)}
    if set(result) != set(TABLES) or len(result) != len(entries):
        raise ValueError("Conjunto Bronze incompleto ou repetido")
    for name, entry in result.items():
        if entry.get("status") != "SUCCEEDED" or entry.get("counts_match") is not True:
            raise ValueError(f"Bronze {name} nao validada")
        count = entry.get("target_count")
        if type(count) is not int or count <= 0 or count != entry.get("source_count"):
            raise ValueError(f"Bronze {name} tem contagens invalidas")
        if entry.get("run_id") != run_id:
            raise ValueError(f"Bronze {name} pertence a outra execucao")
        _bucket, key = parse_s3_uri(entry.get("target_uri", ""))
        if f"/run_id={run_id}/" not in "/" + key or not key.endswith("/"):
            raise ValueError(f"Bronze {name} nao aponta ao snapshot declarado")
        if f"/{name}/" not in "/" + key:
            raise ValueError(f"Bronze {name} aponta a outra entidade")
    return result


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def put_json(s3, bucket: str, key: str, value: dict) -> None:
    s3.put_object(Bucket=bucket, Key=key, ContentType="application/json; charset=utf-8",
                  Body=(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8"),
                  IfNoneMatch="*")


class Quality:
    def __init__(self):
        self.results: list[dict] = []

    def add(self, table, rule, measured, limit, passed, detail="", blocking=True):
        value = {"table": table, "rule": rule, "type": "blocking" if blocking else "informational",
                 "measured": measured, "limit": limit, "passed": bool(passed), "detail": detail}
        self.results.append(value)
        LOG.info(json.dumps({"quality": value}, ensure_ascii=False))
        return value

    def enforce(self, table=None):
        failed = [r for r in self.results if r["type"] == "blocking" and not r["passed"]
                  and (table is None or r["table"] == table)]
        if failed:
            raise RuntimeError("Qualidade bloqueou publicacao: " + "; ".join(
                f"{r['table']}.{r['rule']}={r['measured']}" for r in failed))


def empty_to_null(name):
    value = F.trim(F.col(name).cast("string"))
    return F.when(value == "", F.lit(None).cast("string")).otherwise(value)


def bad_count(expression):
    return F.sum(F.when(expression, 1).otherwise(0)).cast("long")


def normalize(df, name, quality):
    missing = sorted(set(REQUIRED[name] + ["data_ingestao"]) - set(df.columns))
    if missing:
        raise ValueError(f"{name}: colunas Bronze ausentes: {missing}")
    from pyspark.sql.types import StringType
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, empty_to_null(field.name))
    if "ano" in df.columns:
        # Bronze possui ano long. Evita truncamento silencioso em eventual mudanca de tipo.
        df = df.withColumn("_ano_original", F.col("ano").cast("string"))
        df = df.withColumn("ano", F.expr("try_cast(ano as int)"))
    expressions = [F.count(F.lit(1)).alias("total")]
    if "ano" in df.columns:
        expressions.append(bad_count(F.col("_ano_original").isNotNull() & (
            F.col("ano").isNull() | ~F.col("_ano_original").rlike(r"^[0-9]{4}$"))).alias("invalid_ano"))
    if "id_municipio" in df.columns:
        expressions.append(bad_count(F.col("id_municipio").isNotNull() & (
            ~F.col("id_municipio").rlike(r"^[0-9]{1,7}$") | (F.expr("try_cast(id_municipio as bigint)") == 0)
        )).alias("invalid_municipio"))
    if "sigla_uf" in df.columns:
        df = df.withColumn("sigla_uf", F.upper(F.col("sigla_uf")))
        expressions.append(bad_count(F.col("sigla_uf").isNotNull() &
                                    ~F.col("sigla_uf").rlike(r"^[A-Z]{2}$")).alias("invalid_uf"))
    stats = df.agg(*expressions).first().asDict()
    for col in ("invalid_ano", "invalid_municipio", "invalid_uf"):
        if col in stats:
            quality.add(name, col, int(stats[col] or 0), 0, (stats[col] or 0) == 0)
    quality.enforce(name)
    if "id_municipio" in df.columns:
        df = df.withColumn("id_municipio", F.lpad(F.col("id_municipio"), 7, "0"))
    return df.drop("_ano_original"), int(stats["total"])


def integrity(df, name, quality, *, label="output"):
    keys = KEYS[name]
    null_key = reduce(lambda left, right: left | right, [F.col(c).isNull() for c in keys])
    expressions = [F.count(F.lit(1)).alias("total"), bad_count(null_key).alias("null_keys")]
    if name == "alunos":
        expressions.append(bad_count(F.col("id_municipio").isNull()).alias("null_municipio"))
    for column in df.columns:
        if column == "taxa_alfabetizacao" or column.startswith("meta_alfabetizacao_") or column.startswith("proporcao_aluno_nivel_"):
            value = F.col(column)
            expressions.append(bad_count(value.isNotNull() & (F.isnan(value) | (value < 0) | (value > 100)))
                               .alias("bad_rate__" + column))
    stats = df.agg(*expressions).first().asDict()
    total = int(stats.pop("total"))
    quality.add(name, label + ":nonempty", total, 1, total > 0)
    for rule, measured in stats.items():
        quality.add(name, label + ":" + rule, int(measured or 0), 0, (measured or 0) == 0)
    quality.enforce(name)
    return total


def deduplicate(df, name, source_count, quality):
    keys = KEYS[name]
    # A mesma chave de dicionario nao pode ser reinterpretada por uma escolha de linha.
    if name == "dicionario":
        conflicts = df.groupBy(*keys).agg(F.countDistinct("valor").alias("n_values")).where(
            F.col("n_values") != 1).count()
        missing_values = df.where(F.col("valor").isNull()).count()
        quality.add(name, "dictionary_conflicting_values", conflicts, 0, conflicts == 0)
        quality.add(name, "dictionary_null_value", missing_values, 0, missing_values == 0)
        quality.enforce(name)
    row_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)]),
                              options={"ignoreNullFields": "false"}), 256)
    window = Window.partitionBy(*keys).orderBy(F.col("data_ingestao").desc_nulls_last(), row_hash.desc())
    result = df.withColumn("_dedup_rank", F.row_number().over(window)).where(
        F.col("_dedup_rank") == 1).drop("_dedup_rank").persist(StorageLevel.MEMORY_AND_DISK)
    count = integrity(result, name, quality, label="deduplicated")
    # A janela produz uma linha por chave; valida ainda a cardinalidade medida.
    unique = result.select(*keys).distinct().count()
    quality.add(name, "deduplicated:key_unique", count - unique, 0, count == unique)
    quality.add(name, "duplicates_removed", source_count - count, None, True,
                "Ultima data_ingestao, desempate por SHA256 de toda a linha normalizada", blocking=False)
    quality.enforce(name)
    return result, count


def decode(df, dictionary, name, column, quality, threshold):
    lookup = dictionary.where((F.col("id_tabela") == name) & (F.col("nome_coluna") == column)).select(
        F.col("chave").alias("_dict_key"), F.col("valor").alias("_dict_value"))
    # Nao se usa dicionario de outra entidade para adivinhar equivalencia.
    lookup_count = lookup.count()
    duplicate_keys = lookup_count - lookup.select("_dict_key").distinct().count()
    quality.add(name, f"decode:{column}:lookup_nonempty", lookup_count, 1, lookup_count > 0)
    quality.add(name, f"decode:{column}:right_key_unique", duplicate_keys, 0, duplicate_keys == 0)
    quality.enforce(name)
    before = df.count()
    joined = df.join(F.broadcast(lookup), F.col(column) == F.col("_dict_key"), "left").withColumn(
        column + "_descricao", F.col("_dict_value")).persist(StorageLevel.MEMORY_AND_DISK)
    stats = joined.agg(F.count(F.lit(1)).alias("after"),
                       bad_count(F.col(column).isNotNull()).alias("nonnull"),
                       bad_count(F.col(column).isNotNull() & F.col("_dict_value").isNotNull()).alias("matched"),
                       bad_count(F.col(column).isNotNull() & F.col("_dict_value").isNull()).alias("unmapped")).first()
    nonnull, matched, after, unmapped = map(int, (stats.nonnull, stats.matched, stats.after, stats.unmapped))
    match_rate = matched / nonnull if nonnull else 1.0
    quality.add(name, f"decode:{column}:left_cardinality", after, before, after == before)
    quality.add(name, f"decode:{column}:match_rate", match_rate, threshold, match_rate >= threshold,
                f"{matched}/{nonnull} codigos nao nulos; nulos de origem preservados")
    quality.add(name, f"decode:{column}:unmapped_nonnull", unmapped, 0, unmapped == 0)
    quality.enforce(name)
    result = joined.drop("_dict_key", "_dict_value")
    # O caller libera os intermediarios no finally do job; preserva cache para evitar reexecucao.
    return result, joined


def add_evaluation_flags(df, dictionary, quality):
    # Estes significados sao os usados no notebook recebido. O dicionario real
    # precisa conter exatamente as categorias reconhecidas; mudanca de dominio bloqueia.
    labels = {
        "presenca": {"Presente": True, "Ausente": False},
        "preenchimento_caderno": {"Prova preenchida": True, "Prova não preenchida": False},
        "alfabetizado": {"Sim": True, "Não": False},
    }
    for column, mapping in labels.items():
        observed = {row.valor for row in dictionary.where(
            (F.col("id_tabela") == "alunos") & (F.col("nome_coluna") == column)
        ).select("valor").distinct().collect()}
        quality.add("alunos", f"boolean_domain:{column}", sorted(observed), sorted(mapping), observed == set(mapping),
                    "Nao converte categoria desconhecida ou nula em falso")
    quality.enforce("alunos")
    def boolean_from_description(column):
        expr = F.lit(None).cast("boolean")
        for label, value in labels[column].items():
            expr = F.when(F.col(column + "_descricao") == label, F.lit(value)).otherwise(expr)
        return expr
    result = df.withColumn("presente", boolean_from_description("presenca")).withColumn(
        "caderno_preenchido", boolean_from_description("preenchimento_caderno"))
    result = result.withColumn("avaliado", F.col("presente") & F.col("caderno_preenchido"))
    result = result.withColumn("alfabetizado_avaliado", F.when(F.col("avaliado"), boolean_from_description(
        "alfabetizado")).otherwise(F.lit(None).cast("boolean")))
    weight = F.col("peso_aluno")
    invalid_weight = weight.isNull() | F.isnan(weight) | (weight <= 0) | (weight == float("inf")) | (weight == float("-inf"))
    stats = result.agg(
        bad_count(F.col("avaliado")).alias("evaluated"),
        bad_count(F.col("avaliado").isNull()).alias("unknown_evaluation"),
        bad_count(F.col("avaliado") & F.col("alfabetizado_avaliado").isNull()).alias("unknown_evaluated_alf"),
        bad_count(F.col("avaliado") & invalid_weight).alias("invalid_evaluated_weight"),
    ).first().asDict()
    for rule in ("unknown_evaluation", "unknown_evaluated_alf", "invalid_evaluated_weight"):
        quality.add("alunos", rule, int(stats[rule] or 0), 0, (stats[rule] or 0) == 0)
    quality.add("alunos", "evaluated_rows", int(stats["evaluated"] or 0), 1, (stats["evaluated"] or 0) > 0)
    quality.enforce("alunos")
    return result


def objects(s3, bucket, prefix):
    count = size = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                count += 1
                size += obj["Size"]
    return {"parquet_file_count": count, "parquet_bytes": size}


def register_tables(glue_api, config, metrics, frames, manifest_uri):
    """Metadados estaveis; sete updates nao constituem transacao conjunta."""
    database = config.database_silver
    try:
        glue_api.create_database(DatabaseInput={"Name": database,
            "Description": "FIAP alfabetizacao: Silver validada; preferir manifesto para snapshot consistente"})
    except glue_api.exceptions.AlreadyExistsException:
        pass
    for metric in metrics:
        name = metric["table"]
        try:
            existing = glue_api.get_table(DatabaseName=database, Name=name)["Table"]
        except glue_api.exceptions.EntityNotFoundException:
            existing = None
        if existing and existing.get("Parameters", {}).get("fiap_pipeline") != "inep-silver":
            raise RuntimeError(f"Recusa sobrescrever tabela Catalog sem ownership: {database}.{name}")
        columns = [{"Name": field.name, "Type": field.dataType.simpleString()}
                   for field in frames[name].schema.fields]
        table_input = {"Name": name, "TableType": "EXTERNAL_TABLE",
            "Description": "Silver FIAP: snapshot S3 validado; manifesto controla publicacao das sete tabelas",
            "Parameters": {"EXTERNAL": "TRUE", "classification": "parquet", "fiap_pipeline": "inep-silver",
                           "fiap_run_id": config.run_id, "fiap_manifest_uri": manifest_uri},
            "StorageDescriptor": {"Columns": columns, "Location": metric["target_uri"],
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Compressed": True,
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                              "Parameters": {"serialization.format": "1"}}}}
        if existing:
            glue_api.update_table(DatabaseName=database, TableInput=table_input)
        else:
            glue_api.create_table(DatabaseName=database, TableInput=table_input)


def _run(argv=None):
    global F, Window, StorageLevel
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark import SparkContext, StorageLevel as SparkStorageLevel
    from pyspark.sql import Window as SparkWindow, functions as SparkFunctions
    F, Window, StorageLevel = SparkFunctions, SparkWindow, SparkStorageLevel
    config = parse_config(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s3 = boto3.client("s3")
    started = now_iso()
    timer = time.monotonic()
    process_date = started[:10]
    audit_path = f"{config.audit_prefix}/process_date={process_date}/run_id={config.run_id}/"
    quality = Quality()
    manifest_bucket, manifest_key = parse_s3_uri(config.bronze_manifest_uri)
    raw_manifest = s3.get_object(Bucket=manifest_bucket, Key=manifest_key)["Body"].read()
    bronze = json.loads(raw_manifest)
    entries = validate_bronze_manifest(bronze)
    if any(parse_s3_uri(entry["target_uri"])[0] == config.bucket and
           parse_s3_uri(entry["target_uri"])[1].startswith(config.prefix + "/") for entry in entries.values()):
        raise ValueError("Destino Silver sobrepoe origem Bronze")
    common = {"layer": "silver", "script_version": VERSION, "job_name": config.job_name,
              "run_id": config.run_id, "process_date": process_date, "started_at_utc": started,
              "bronze_manifest_uri": config.bronze_manifest_uri, "bronze_run_id": bronze["run_id"],
              "bronze_manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(), "config": asdict(config)}
    put_json(s3, config.bucket, audit_path + "start.json", dict(common, status="STARTED"))
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    job = Job(glue)
    job.init(config.job_name, {"JOB_NAME": config.job_name})
    cached = []
    transformed = {}
    metrics = []
    counts = {}
    source_counts = {}
    try:
        # Todas as transformacoes e regras de negocio antecedem qualquer escrita Silver.
        for name in TABLES:
            table_started = time.monotonic()
            raw = spark.read.parquet(entries[name]["target_uri"])
            df, count = normalize(raw, name, quality)
            quality.add(name, "bronze_manifest_count", count, entries[name]["target_count"],
                        count == entries[name]["target_count"])
            quality.enforce(name)
            integrity(df, name, quality, label="normalized_source")
            df, dedup_count = deduplicate(df, name, count, quality)
            cached.append(df)
            if name in {"municipio", "uf", "alunos"}:
                columns = ["rede", "serie"]
                if name == "alunos":
                    columns += ["presenca", "preenchimento_caderno", "alfabetizado"]
                for column in columns:
                    df, cache = decode(df, transformed["dicionario"], name, column, quality, config.limiar_join)
                    cached.append(cache)
                if name == "alunos":
                    df = add_evaluation_flags(df, transformed["dicionario"], quality)
            elif name.startswith("meta_"):
                # Metas trazem rede como texto, sem dicionario para essas tabelas.
                df = df.withColumn("rede_descricao", F.col("rede"))
            df = df.withColumn("id_processamento_silver", F.lit(config.run_id)).withColumn(
                "data_processamento_silver", F.lit(started).cast("timestamp")).withColumn(
                "bronze_run_id", F.lit(bronze["run_id"]))
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(df)
            output_count = integrity(df, name, quality, label="transformed")
            quality.add(name, "transformation_preserves_deduplicated_count", output_count, dedup_count,
                        output_count == dedup_count)
            quality.enforce(name)
            transformed[name] = df
            counts[name] = output_count
            source_counts[name] = count
            metrics.append({"table": name, "declared_key": KEYS[name], "source_uri": entries[name]["target_uri"],
                            "source_count": count, "deduplicated_count": output_count,
                            "duplicates_removed": count - output_count,
                            "transform_duration_seconds": round(time.monotonic() - table_started, 3)})
        quality.enforce()
        for metric in metrics:
            name = metric["table"]
            table_started = time.monotonic()
            key = f"{config.prefix}/{name}/process_date={process_date}/run_id={config.run_id}/"
            uri = f"s3://{config.bucket}/{key}"
            partitions = config.alunos_partitions if name == "alunos" else 1
            transformed[name].coalesce(partitions).write.mode("errorifexists").option(
                "compression", "snappy").parquet(uri)
            reread = spark.read.parquet(uri)
            target_count = integrity(reread, name, quality, label="readback")
            quality.add(name, "readback_count_matches", target_count, counts[name], target_count == counts[name])
            # Spark Parquet readback torna campos nullable; compara tipos/nomes sem nullability.
            expected_types = [(f.name, f.dataType.simpleString()) for f in transformed[name].schema.fields]
            actual_types = [(f.name, f.dataType.simpleString()) for f in reread.schema.fields]
            quality.add(name, "readback_schema_matches", actual_types, expected_types, actual_types == expected_types)
            quality.enforce(name)
            inventory = objects(s3, config.bucket, key)
            quality.add(name, "parquet_files_nonempty", inventory["parquet_file_count"], 1,
                        inventory["parquet_file_count"] > 0)
            quality.enforce(name)
            metric.update(status="SUCCEEDED", target_uri=uri, target_count=target_count,
                          counts_match=(target_count == counts[name]), schema=json.loads(reread.schema.json()),
                          write_readback_duration_seconds=round(time.monotonic() - table_started, 3), **inventory)
            metric["quality"] = [r for r in quality.results if r["table"] == name]
            put_json(s3, config.bucket, audit_path + f"tables/{name}.json", dict(metric, run_id=config.run_id))
        quality.enforce()
        put_json(s3, config.bucket, audit_path + "quality.json", dict(common, status="PASSED", results=quality.results))
        manifest_uri = f"s3://{config.bucket}/{audit_path}manifest.json"
        if config.register_catalog:
            register_tables(boto3.client("glue"), config, metrics, transformed, manifest_uri)
        job.commit()
        manifest = dict(common, status="SUCCEEDED", is_full_batch=True, all_counts_match=True,
                        published_contract="Consume somente os sete target_uri deste manifesto SUCCEEDED",
                        catalog_registered=config.register_catalog, database_silver=config.database_silver,
                        catalog_publication_note="Updates por tabela, sem transacao conjunta; Gold consome manifesto",
                        validated_table_count=len(metrics), tables=metrics,
                        quality=quality.results, total_source_rows=sum(source_counts.values()),
                        total_target_rows=sum(counts.values()), finished_at_utc=now_iso(),
                        duration_seconds=round(time.monotonic() - timer, 3))
        put_json(s3, config.bucket, audit_path + "manifest.json", manifest)
        LOG.info(json.dumps({"status": "SUCCEEDED", "manifest_uri": manifest_uri,
                             "counts": counts}, ensure_ascii=False))
        return manifest
    except Exception as error:
        failure = dict(common, status="FAILED", error_type=type(error).__name__, error=str(error),
                       tables=metrics, quality=quality.results, finished_at_utc=now_iso(),
                       duration_seconds=round(time.monotonic() - timer, 3))
        try:
            put_json(s3, config.bucket, audit_path + "failure.json", failure)
        except Exception:
            LOG.exception("Falha ao persistir failure.json; a excecao original sera propagada")
        raise
    finally:
        for df in cached:
            try:
                df.unpersist()
            except Exception:
                LOG.warning("Nao foi possivel liberar um cache Spark", exc_info=True)


def main(argv=None):
    """Emite uma metrica EMF final, inclusive para falha no preflight."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        job_name = arguments[arguments.index("--JOB_NAME") + 1]
    except (ValueError, IndexError):
        job_name = "unknown"
    failed = 1
    try:
        result = _run(arguments)
        failed = 0
        return result
    finally:
        print(json.dumps({"_aws": {"Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{"Namespace": "FIAP/Medallion", "Dimensions": [["Layer", "JobName"]],
                "Metrics": [{"Name": "Failed", "Unit": "Count"}]}]},
            "Layer": "silver", "JobName": job_name, "Failed": failed}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
