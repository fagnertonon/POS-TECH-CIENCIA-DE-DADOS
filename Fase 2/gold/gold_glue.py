"""FIAP Gold: AWS Glue 5.1 / Spark; no schedules or source mutations.

Only an immutable SUCCEEDED manifest publishes a complete five-table run.
Versioned Parquet paths remain unavailable to consumers until that commit.
The original notebook is preserved separately; this file contains its port.
"""
import hashlib
import json
import math
import re
import sys
import uuid
from datetime import datetime, timezone
from functools import reduce
from time import monotonic, time
from urllib.parse import urlparse

import boto3
from pyspark import StorageLevel
from pyspark.sql import functions as F, types as T

SCRIPT_VERSION = "1.0.2"
ORIGINAL_NOTEBOOK_SHA256 = "3cfa6fe66a1121e3cc6c98d2f286d6d7c149e6c47fe314c9e1ba73dae8ebcc5e"
DEFAULTS = {
    "JOB_NAME": "fiap-gold-inep-batch",
    "DATABASE_SILVER": "tech_challenge_fase2_silver",
    "DATABASE_GOLD": "tech_challenge_fase2_gold",
    "S3_BUCKET": "fiap-tc-fase2-2026",
    "SILVER_MANIFEST_URI": "",
    "GOLD_PREFIX": "gold/inep",
    "AUDIT_PREFIX": "auditoria/gold",
    "LIMIAR_JOIN": "0.95",
    "TOLERANCIA_PP": "0.1",
    "SERIE_BRASIL": "2",
    "REGISTER_CATALOG": "false",
    "DEMO_FAILURE": "false",
    "VERIFY_COVERAGE_FIXTURE": "false",
}
NOMES_SILVER = ["alunos", "municipio", "uf", "dicionario",
                "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf",
                "meta_alfabetizacao_municipio"]
REDE_META_PARA_CODIGO = {"Municipal": "3", "Pública": "5"}
COMPOSICAO_REDE = {"5": ["2", "3"], "0": ["1", "2", "3", "4"]}
ANOS_META = list(range(2024, 2031))
COLUNAS_META = [f"meta_alfabetizacao_{ano}" for ano in ANOS_META]
NIVEIS = [f"proporcao_aluno_nivel_{n}" for n in range(9)]
NUMERIC = {"tinyint", "smallint", "int", "integer", "bigint", "long", "float", "double"}
UF_IBGE = dict(zip(
    "11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 41 42 43 50 51 52 53".split(),
    "RO AC AM RR PA AP TO MA PI CE RN PB PE AL SE BA MG ES RJ SP PR SC RS MS MT GO DF".split()))
CAMPOS_COBERTURA = (
    "quantidade_ufs_microdados", "ufs_microdados", "ufs_sem_microdados", "ufs_resultados_uf",
    "ufs_resultados_sem_microdados", "ufs_microdados_sem_resultado_uf", "status_comparabilidade", "nota_fonte")
STATUS_COMPARABILIDADE = (
    "REFERENCIA_UF_INDISPONIVEL", "COBERTURA_DIFERENTE_ENTRE_BASES",
    "COBERTURA_UF_COINCIDENTE_METODOLOGIA_NAO_COMPROVADA")


def required_columns():
    comum = {"ano": "numeric", "rede": "string", "serie": "string"}
    resultado = dict(comum, rede_descricao="string", taxa_alfabetizacao="numeric")
    metas = {"ano": "numeric", "rede_descricao": "string", "taxa_alfabetizacao": "numeric"}
    metas.update({c: "numeric" for c in COLUNAS_META})
    municipio = dict(resultado, id_municipio="string", serie_descricao="string", media_portugues="numeric")
    municipio.update({c: "numeric" for c in NIVEIS})
    return {
        "alunos": dict(comum, id_municipio="string", peso_aluno="numeric",
                       avaliado="boolean", alfabetizado_avaliado="boolean"),
        "municipio": municipio,
        "uf": dict(resultado, sigla_uf="string"),
        "dicionario": {c: "string" for c in ("id_tabela", "nome_coluna", "chave", "valor")},
        "meta_alfabetizacao_brasil": metas,
        "meta_alfabetizacao_uf": dict(metas, sigla_uf="string"),
        "meta_alfabetizacao_municipio": dict(metas, id_municipio="string"),
    }


def utcnow():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def params(overrides=None):
    result = dict(DEFAULTS)
    for i, item in enumerate(sys.argv[1:], 1):
        if item.startswith("--") and "=" in item:
            name, value = item[2:].split("=", 1)
            result[name] = value
        elif item.startswith("--") and i + 1 < len(sys.argv):
            result[item[2:]] = sys.argv[i + 1]
    result.update(overrides or {})
    result["JOB_RUN_ID"] = result.get("JOB_RUN_ID") or "interactive_" + str(uuid.uuid4())
    for name in ("DATABASE_SILVER", "DATABASE_GOLD"):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,251}", result[name]):
            raise ValueError(f"Invalid {name}: use a Glue database identifier")
    if result["DATABASE_SILVER"] == result["DATABASE_GOLD"]:
        raise ValueError("Silver and Gold databases must differ")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", result["JOB_RUN_ID"]):
        raise ValueError("Invalid JOB_RUN_ID")
    if result["SERIE_BRASIL"] != "2":
        raise ValueError("Official CNCA Brazil comparison requires series 2 (second year of elementary school)")
    for name, root in (("GOLD_PREFIX", "gold/"), ("AUDIT_PREFIX", "auditoria/gold")):
        result[name] = result[name].strip("/")
        if not result[name].startswith(root) or ".." in result[name].split("/"):
            raise ValueError(f"{name} must stay under {root}")
    for name in ("REGISTER_CATALOG", "DEMO_FAILURE", "VERIFY_COVERAGE_FIXTURE"):
        if str(result[name]).lower() not in ("true", "false"):
            raise ValueError(f"{name} must be true or false")
        result[name] = str(result[name]).lower() == "true"
    result["LIMIAR_JOIN"] = float(result["LIMIAR_JOIN"])
    result["TOLERANCIA_PP"] = float(result["TOLERANCIA_PP"])
    if not (math.isfinite(result["LIMIAR_JOIN"]) and 0 <= result["LIMIAR_JOIN"] <= 1):
        raise ValueError("LIMIAR_JOIN must be finite and in [0,1]")
    if not (math.isfinite(result["TOLERANCIA_PP"]) and result["TOLERANCIA_PP"] >= 0):
        raise ValueError("TOLERANCIA_PP must be finite and nonnegative")
    return result


def type_matches(actual, expected):
    actual = actual.lower()
    return (actual in NUMERIC or actual.startswith("decimal(")) if expected == "numeric" else actual == expected


def inspect_silver(glue, cfg):
    """Read one committed Silver manifest; never use all-history table roots.

    Glue is retained as an argument for interface stability, but this function
    needs no Glue catalog entries. Silver is selected explicitly by manifest.
    """
    source_uri = cfg.get("SILVER_MANIFEST_URI", "")
    uri = urlparse(source_uri)
    if uri.scheme != "s3" or not uri.netloc or not uri.path.endswith("/manifest.json"):
        raise RuntimeError("BLOCKED_MISSING_SILVER: supply SILVER_MANIFEST_URI of a successful complete Silver run")
    content = boto3.client("s3").get_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))["Body"].read()
    manifest = json.loads(content)
    if (manifest.get("status") != "SUCCEEDED" or manifest.get("layer") != "silver"
            or manifest.get("is_full_batch") is not True):
        raise RuntimeError("Silver manifest must be SUCCEEDED, layer=silver, is_full_batch=true")
    items = manifest.get("tables", [])
    if len(items) != len(NOMES_SILVER) or {item.get("table") for item in items} != set(NOMES_SILVER):
        raise RuntimeError("Silver manifest must contain exactly the seven distinct required tables")
    by_name = {item["table"]: item for item in items}
    errors, result = [], {}
    for name, required in required_columns().items():
        item = by_name[name]
        if item.get("status") != "SUCCEEDED" or item.get("counts_match") is not True or item.get("target_count", 0) <= 0:
            errors.append(f"{name}: Silver table must be successful, nonempty and count-validated")
        columns = item.get("schema", {}).get("fields", [])
        actual = {c["name"]: c["type"] for c in columns if isinstance(c.get("type"), str)}
        for column, expected in required.items():
            if column not in actual:
                errors.append(f"{name}: missing {column}")
            elif not type_matches(actual[column], expected):
                errors.append(f"{name}.{column}: expected {expected}, found {actual[column]}")
        location = item.get("target_uri", "")
        uri = urlparse(location)
        run = re.search(r"(?:^|/)run_id=([A-Za-z0-9_-]+)(?:/|$)", uri.path)
        if uri.scheme != "s3" or not uri.netloc or not run:
            errors.append(f"{name}: Silver location must be an S3 path of one run_id")
        if run and run.group(1) != manifest.get("run_id"):
            errors.append(f"{name}: location run_id differs from committed Silver manifest")
        if not uri.path.lstrip("/").startswith("silver/"):
            errors.append(f"{name}: source location must stay in the Silver layer")
        if item.get("format", "parquet") != "parquet":
            errors.append(f"{name}: this port expects Parquet Silver")
        result[name] = {
            "database": cfg["DATABASE_SILVER"], "table": name,
            "version_id": manifest.get("run_id"), "location": location,
            "silver_run_id": run.group(1) if run else None,
            "columns": actual, "expected_count": item.get("target_count"),
            "silver_manifest_uri": source_uri,
            "silver_manifest_sha256": hashlib.sha256(content).hexdigest(),
        }
    runs = {item["silver_run_id"] for item in result.values()}
    if len(runs) > 1:
        errors.append("Silver tables point to different run_id values")
    if errors:
        raise RuntimeError("Silver preflight failed; no Gold data was read or written: " + "; ".join(errors))
    return result


def put_json(key, value):
    body = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode("utf-8")
    S3.put_object(Bucket=CFG["S3_BUCKET"], Key=key, Body=body,
                  ContentType="application/json", IfNoneMatch="*")
    return {"s3_uri": f"s3://{CFG['S3_BUCKET']}/{key}", "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}


def registrar_qualidade(entidade, resultados):
    if entidade in QUALITY:
        raise RuntimeError("Quality entity already recorded: " + entidade)
    QUALITY[entidade] = resultados
    put_json(f"{AUDIT_RUN}/quality/{entidade}.json", {
        "run_id": ID_EXECUCAO, "recorded_at_utc": utcnow(),
        "entity": entidade, "results": resultados,
    })
    for r in resultados:
        print(json.dumps({"entity": entidade, **r}, ensure_ascii=False))


def audit_join(left, right, keys, how, name, blocking=True, eligible=None, universe_description=None):
    if eligible is not None:
        full_ratio, full_matched, full_total = taxa_correspondencia(left, right, keys)
        full = regra("cobertura_total:" + name, "informativa", full_ratio, None, True,
                     f"{full_matched} of {full_total}; all left rows including intentionally ineligible cohorts")
        if full_total == 0:
            full.update(valor_medido=None, passou=None, aplicavel=False, detalhe="N/A: no left rows")
        JOIN_METRICS.append(full)
    ratio, matched, total = taxa_correspondencia(eligible if eligible is not None else left, right, keys)
    metric = regra(
        "taxa_correspondencia:" + name, "bloqueante" if blocking else "informativa",
        ratio, LIMIAR_JOIN if blocking else None, ratio >= LIMIAR_JOIN if blocking else True,
        f"{matched} of {total}; join={how}; keys={keys}; measured before join; "
        + (universe_description or "Universe: all left rows"),
    )
    if total == 0:
        metric.update(tipo="informativa", valor_medido=None, limite=None, passou=None, aplicavel=False,
                      detalhe="N/A: eligible universe empty; no match-rate pass is asserted; "
                              + (universe_description or "Universe: all left rows"))
    else:
        metric["aplicavel"] = True
    JOIN_METRICS.append(metric)
    exigir("join:" + name, [metric])
    return left.join(right, keys, how)


def emit_outcome(failed, duration_seconds):
    """CloudWatch EMF via stdout only; logging must not mask a job exception."""
    try:
        print(json.dumps({
            "_aws": {"Timestamp": int(time() * 1000), "CloudWatchMetrics": [{
                "Namespace": "FIAP/Medallion", "Dimensions": [["Layer", "JobName"]],
                "Metrics": [{"Name": "Failed", "Unit": "Count"}],
            }]},
            "Layer": "gold", "JobName": CFG["JOB_NAME"], "Failed": 1 if failed else 0,
            "Status": "FAILED" if failed else "SUCCEEDED", "RunId": ID_EXECUCAO,
            "DurationSeconds": round(duration_seconds, 3),
        }, ensure_ascii=False))
        return True
    except Exception:
        return False


def invalid_number(col):
    value = F.col(col).cast("double")
    return F.isnan(value) | (F.abs(value) == F.lit(float("inf")))


def regras_entrada(silver):
    res = []
    unique_keys = {
        "municipio": ["ano", "id_municipio", "serie", "rede"],
        "uf": ["ano", "sigla_uf", "serie", "rede"],
        "meta_alfabetizacao_brasil": ["ano", "rede_descricao"],
        "meta_alfabetizacao_uf": ["ano", "sigla_uf", "rede_descricao"],
        "meta_alfabetizacao_municipio": ["ano", "id_municipio", "rede_descricao"],
    }
    for name, keys in unique_keys.items():
        df = silver[name]
        total = df.count()
        nulls = df.where(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in keys])).count()
        duplicated = total - df.select(*keys).distinct().count()
        res.extend([
            regra("chaves_nulas:" + name, "bloqueante", nulls, 0, nulls == 0, str(keys)),
            regra("chaves_duplicadas:" + name, "bloqueante", duplicated, 0, duplicated == 0, str(keys)),
        ])
    for name, columns in required_columns().items():
        numeric_columns = [column for column, kind in columns.items() if kind == "numeric"]
        if numeric_columns:
            invalid = silver[name].where(reduce(lambda a, b: a | b, [invalid_number(c) for c in numeric_columns])).count()
            res.append(regra("numericos_finitos:" + name, "bloqueante", invalid, 0, invalid == 0,
                             "All present numeric input values must be finite; nulls are preserved"))
    alunos = silver["alunos"]
    evaluated = alunos.where(F.col("avaliado"))
    bad = evaluated.where(
        F.col("alfabetizado_avaliado").isNull() | F.col("peso_aluno").isNull()
        | invalid_number("peso_aluno") | (F.col("peso_aluno") <= 0)
        | reduce(lambda a, b: a | b, [F.col(c).isNull() for c in ("ano", "id_municipio", "serie", "rede")])
    ).count()
    res.append(regra("avaliados_denominador_valido", "bloqueante", bad, 0, bad == 0,
                     "Every evaluated pupil needs known alphabetization, finite positive weight and keys; no silent drop"))
    unknown_network = evaluated.where(~F.col("rede").isin("1", "2", "3", "4")).count()
    res.append(regra("microdados_rede_elementar", "bloqueante", unknown_network, 0, unknown_network == 0,
                     "Aggregate networks 5/0 must be built once from elementary networks"))
    invalid_uf = evaluated.where(
        F.col("rede").isin("2", "3")
        & ~F.substring(F.col("id_municipio"), 1, 2).isin(list(UF_IBGE))
    ).count()
    res.append(regra("microdados_publicos_uf_identificavel", "bloqueante", invalid_uf, 0, invalid_uf == 0,
                     "UF is inferred from the normalized municipality IBGE prefix; unknown UFs are not silently dropped"))
    # Official Brazil lacks a series dimension: comparison is meaningful only
    # if evaluated public pupils have one exact series for each year.
    ambiguous_series = (evaluated.where(F.col("rede").isin("2", "3"))
                        .groupBy("ano").agg(F.countDistinct("serie").alias("n"))
                        .where(F.col("n") != 1).count())
    res.append(regra("brasil_serie_unica_por_ano", "bloqueante", ambiguous_series, 0, ambiguous_series == 0,
                     "Official Brazil has no series key; multiple series require an explicit business decision"))
    incompatible_series = evaluated.where(F.col("rede").isin("2", "3") & (F.col("serie") != CFG["SERIE_BRASIL"])).count()
    res.append(regra("brasil_serie_compativel_cnca", "bloqueante", incompatible_series, 0, incompatible_series == 0,
                     "CNCA is end of second year of elementary school; source dictionary maps this cohort to series code 2"))
    for name, network in (("municipio", "3"), ("uf", "5")):
        excluded = silver[name].where((F.col("rede") == network) & (F.col("serie") != CFG["SERIE_BRASIL"])).count()
        res.append(regra("resultado_meta_outras_series_excluidas:" + name, "informativa", excluded, None, True,
                         "Only second-year records enter result-vs-meta; other cohorts remain in their source/indicator tables"))
    return res

def codigo_rede_meta(coluna="rede_descricao"):
    """Traduz o texto de rede das tabelas de meta para o código de rede. Texto fora do mapa fica nulo (e a regra bloqueante acusa)."""
    expr = F.lit(None).cast("string")
    for texto, codigo in REDE_META_PARA_CODIGO.items():
        expr = F.when(F.col(coluna) == texto, F.lit(codigo)).otherwise(expr)
    return expr

def meta_do_ano(coluna_ano="ano", deslocamento=0):
    """Escolhe a coluna meta_alfabetizacao_<ano + deslocamento>. Ano sem coluna de meta (ex.: 2023) fica nulo."""
    return F.coalesce(*[
        F.when(F.col(coluna_ano) + deslocamento == a, F.col(f"meta_alfabetizacao_{a}"))
        for a in ANOS_META
    ])

def aplicar_meta(df, coluna_taxa="taxa_alfabetizacao"):
    """Compara a taxa com a meta do mesmo ano, com ausência de meta e resultado explícitas."""
    return (
        df.withColumn("meta_alfabetizacao_ano", meta_do_ano())
        .withColumn(
            "status_meta",
            F.when(F.col("meta_alfabetizacao_ano").isNull(), "sem_meta")
            .when(F.col(coluna_taxa).isNull(), "sem_resultado")
            .when(F.col(coluna_taxa) >= F.col("meta_alfabetizacao_ano"), "atingiu")
            .otherwise("nao_atingiu"),
        )
        .withColumn("meta_disponivel", F.col("meta_alfabetizacao_ano").isNotNull())
        .withColumn("resultado_disponivel", F.col(coluna_taxa).isNotNull())
        .withColumn("distancia_ate_meta_pp", F.round(F.col(coluna_taxa) - F.col("meta_alfabetizacao_ano"), 4))
        .withColumn("distancia_ate_meta_2030_pp", F.round(F.col(coluna_taxa) - F.col("meta_alfabetizacao_2030"), 4))
    )

def taxa_calculada_microdados(alunos, por=("id_municipio",)):
    """Taxa de alfabetização (%) recalculada dos microdados: ponderada pelo peso amostral, só alunos avaliados."""
    base = (
        alunos.where(F.col("avaliado") & F.col("peso_aluno").isNotNull())
        .select("ano", *por, "serie", "rede", "peso_aluno",
                F.col("alfabetizado_avaliado").cast("int").alias("alf"))
    )
    recortes = [base]
    for codigo, composicao in COMPOSICAO_REDE.items():
        recortes.append(base.where(F.col("rede").isin(composicao)).withColumn("rede", F.lit(codigo)))
    todos = reduce(lambda a, b: a.unionByName(b), recortes)
    return todos.groupBy("ano", *por, "serie", "rede").agg(
        F.count(F.lit(1)).alias("alunos_avaliados"),
        F.sum("peso_aluno").alias("soma_peso_avaliados"),
        F.round(100 * F.sum(F.col("peso_aluno") * F.col("alf")) / F.sum("peso_aluno"), 4)
        .alias("taxa_alfabetizacao_calculada"),
    )

def construir_indicador_municipio(municipio, alunos):
    """Município × ano × série × rede: taxa oficial (Silver `municipio`) + taxa recalculada dos microdados (`alunos`)."""
    calculada = taxa_calculada_microdados(alunos)
    oficial = municipio.select(
        "ano", "id_municipio", "serie", "serie_descricao", "rede", "rede_descricao",
        F.col("taxa_alfabetizacao").alias("taxa_alfabetizacao_oficial"),
        F.col("media_portugues").alias("media_portugues_oficial"),
        *NIVEIS,
    )
    df = audit_join(oficial, calculada, ["ano", "id_municipio", "serie", "rede"], "left", "indicador_municipio")
    df = (
        df.withColumn("diferenca_pp", F.round(F.col("taxa_alfabetizacao_calculada") - F.col("taxa_alfabetizacao_oficial"), 4))
        .withColumn("dentro_tolerancia",
                    F.when(F.col("diferenca_pp").isNull(), F.lit(None).cast("boolean"))
                    .otherwise(F.abs(F.col("diferenca_pp")) <= TOLERANCIA_PP))
    )
    return df.select(
        "ano", "id_municipio", "serie", "serie_descricao", "rede", "rede_descricao",
        "taxa_alfabetizacao_oficial", "taxa_alfabetizacao_calculada", "diferenca_pp", "dentro_tolerancia",
        "alunos_avaliados", "soma_peso_avaliados", "media_portugues_oficial", *NIVEIS,
    )

def construir_resultado_vs_meta(municipio, uf, meta_brasil, meta_uf, meta_municipio):
    """Resultado × meta nos três níveis, sempre no mesmo recorte de rede (municipal = rede 3; UF e Brasil = rede 5)."""
    colunas = ["nivel", "ano", "id_municipio", "sigla_uf", "serie", "rede", "rede_descricao",
               "taxa_alfabetizacao", *COLUNAS_META]
    nulo = F.lit(None).cast("string")

    mm = meta_municipio.withColumn("rede", codigo_rede_meta()).select("ano", "id_municipio", "rede", *COLUNAS_META)
    nivel_municipio = (
        audit_join(municipio.where((F.col("rede") == "3") & (F.col("serie") == CFG["SERIE_BRASIL"])), mm, ["ano", "id_municipio", "rede"], "left", "resultado_municipio_meta")
        .select(F.lit("municipio").alias("nivel"), "ano", "id_municipio", nulo.alias("sigla_uf"),
                "serie", "rede", "rede_descricao", "taxa_alfabetizacao", *COLUNAS_META)
    )

    mf = meta_uf.withColumn("rede", codigo_rede_meta()).select("ano", "sigla_uf", "rede", *COLUNAS_META)
    nivel_uf = (
        audit_join(uf.where((F.col("rede") == "5") & (F.col("serie") == CFG["SERIE_BRASIL"])), mf, ["ano", "sigla_uf", "rede"], "left", "resultado_uf_meta")
        .select(F.lit("uf").alias("nivel"), "ano", nulo.alias("id_municipio"), "sigla_uf",
                "serie", "rede", "rede_descricao", "taxa_alfabetizacao", *COLUNAS_META)
    )

    nivel_brasil = meta_brasil.select(
        F.lit("brasil").alias("nivel"), "ano", nulo.alias("id_municipio"), nulo.alias("sigla_uf"),
        nulo.alias("serie"), codigo_rede_meta().alias("rede"), "rede_descricao",
        "taxa_alfabetizacao", *COLUNAS_META,
    )

    uniao = reduce(lambda a, b: a.unionByName(b),
                   [d.select(*colunas) for d in (nivel_municipio, nivel_uf, nivel_brasil)])
    return aplicar_meta(uniao)

def construir_evolucao_temporal(resultado_vs_meta):
    """Série por UF e Brasil, com variação em p.p. em relação ao ano anterior. Sem projeção (poucos pontos)."""
    from pyspark.sql import Window
    base = resultado_vs_meta.where(F.col("nivel").isin("uf", "brasil"))
    janela = Window.partitionBy("nivel", "sigla_uf", "serie", "rede").orderBy("ano")
    return (
        base.withColumn("taxa_ano_anterior", F.lag("taxa_alfabetizacao").over(janela))
        .withColumn("ano_anterior", F.lag("ano").over(janela))
        .withColumn("variacao_pp_ano_anterior", F.when(F.col("ano") - F.col("ano_anterior") == 1, F.round(F.col("taxa_alfabetizacao") - F.col("taxa_ano_anterior"), 4)))
        .withColumn("pontos_na_serie", F.count("ano").over(Window.partitionBy("nivel", "sigla_uf", "serie", "rede")))
        .select("nivel", "sigla_uf", "ano", "serie", "rede", "rede_descricao", "taxa_alfabetizacao",
                "meta_alfabetizacao_ano", "status_meta", "distancia_ate_meta_pp",
                "variacao_pp_ano_anterior", "pontos_na_serie")
    )

def resumir_cobertura_brasil(ano, serie, ufs_microdados, ufs_resultados_uf):
    """Geographic evidence, independent of numeric agreement or national methodology."""
    universe = set(UF_IBGE.values())
    micro, results = set(ufs_microdados), set(ufs_resultados_uf)
    if (micro | results) - universe:
        raise ValueError("Unknown UF in national coverage metadata")
    if not results:
        status = "REFERENCIA_UF_INDISPONIVEL"
    elif micro != results:
        status = "COBERTURA_DIFERENTE_ENTRE_BASES"
    else:
        status = "COBERTURA_UF_COINCIDENTE_METODOLOGIA_NAO_COMPROVADA"
    note = (
        "Cobertura de alunos avaliados das redes 2+3 versus resultados publicados em uf (rede 5). "
        "ufs_sem_microdados usa as 27 UFs apenas como referencia geografica; nao mede participacao de alunos. "
        "A tabela uf nao define, sozinha, o universo do indicador Brasil. Coincidencia de UFs ou taxas nao "
        "comprova equivalencia metodologica nem cobertura integral de alunos/escolas."
    )
    if ano == 2023 and "SP" not in micro:
        note += (
            " No snapshot atual, SP esta ausente dos alunos. O Leia-Me AEEB 2023, p.17, "
            "documenta a publicacao de TS_ALUNO sem Sao Paulo; isso nao atribui toda a diferenca nacional a SP. "
            "Fonte: https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/"
            "avaliacao-da-alfabetizacao/resultados/2023"
        )
    if ano == 2024 and "DF" in micro and "DF" not in results:
        note += (
            " No snapshot atual, DF tem alunos mas nao resultado em uf. O Leia-Me AEEB 2024, pp.21-22, "
            "documenta essa ausencia em TS_ESTADO e a presenca do resultado estadual em TS_MUNICIPIO. "
            "Fonte: https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/"
            "avaliacao-da-alfabetizacao/resultados/2024"
        )
    return {
        "ano": ano, "serie": serie, "rede": "5", "quantidade_ufs_microdados": len(micro),
        "ufs_microdados": sorted(micro), "ufs_sem_microdados": sorted(universe - micro),
        "ufs_resultados_uf": sorted(results), "ufs_resultados_sem_microdados": sorted(results - micro),
        "ufs_microdados_sem_resultado_uf": sorted(micro - results),
        "status_comparabilidade": status, "nota_fonte": note,
    }


def construir_cobertura_brasil(alunos, uf):
    # Match the public denominator exactly; invalid weights/keys have already
    # been rejected by regras_entrada rather than excluded to improve results.
    observed = (alunos.where(F.col("avaliado") & F.col("peso_aluno").isNotNull()
                            & F.col("rede").isin(COMPOSICAO_REDE["5"]))
                .select("ano", "serie", F.substring(F.col("id_municipio"), 1, 2).alias("codigo_uf"))
                .distinct().collect())
    cohorts = {}
    for row in observed:
        state = UF_IBGE.get(row["codigo_uf"])
        if state is None:
            raise RuntimeError("Unknown municipality UF prefix in public denominator; no coverage row dropped")
        cohorts.setdefault((row["ano"], row["serie"]), set()).add(state)
    references = {}
    for row in (uf.where((F.col("rede") == "5") & F.col("taxa_alfabetizacao").isNotNull())
                .select("ano", "serie", "sigla_uf").distinct().collect()):
        references.setdefault((row["ano"], row["serie"]), set()).add(row["sigla_uf"])
    rows = [resumir_cobertura_brasil(year, series, states, references.get((year, series), set()))
            for (year, series), states in sorted(cohorts.items())]
    fields = [T.StructField("ano", alunos.schema["ano"].dataType, False), T.StructField("serie", T.StringType(), False),
              T.StructField("rede", T.StringType(), False),
              T.StructField("quantidade_ufs_microdados", T.IntegerType(), False)]
    fields.extend(T.StructField(name, T.ArrayType(T.StringType(), containsNull=False), False)
                  for name in CAMPOS_COBERTURA[1:6])
    fields.extend(T.StructField(name, T.StringType(), False) for name in CAMPOS_COBERTURA[6:])
    return spark.createDataFrame(rows, T.StructType(fields))


def construir_validacao_cruzada_brasil(alunos, meta_brasil, uf):
    """Brasil (rede pública): taxa recalculada dos microdados × taxa publicada em `meta_alfabetizacao_brasil`."""
    calculada = taxa_calculada_microdados(alunos, por=()).where(F.col("rede") == "5")
    oficial = meta_brasil.withColumn("rede", codigo_rede_meta()).select(
        "ano", "rede", F.col("taxa_alfabetizacao").alias("taxa_oficial_publicada"))
    numeric = (
        audit_join(calculada, oficial, ["ano", "rede"], "left", "validacao_brasil_oficial")
        .withColumn("diferenca_pp", F.round(F.col("taxa_alfabetizacao_calculada") - F.col("taxa_oficial_publicada"), 4))
        .withColumn("dentro_tolerancia",
                    F.when(F.col("diferenca_pp").isNull(), F.lit(None).cast("boolean"))
                    .otherwise(F.abs(F.col("diferenca_pp")) <= TOLERANCIA_PP))
        .select("ano", "serie", "rede", "alunos_avaliados", "taxa_alfabetizacao_calculada",
                "taxa_oficial_publicada", "diferenca_pp", "dentro_tolerancia")
    )
    coverage = construir_cobertura_brasil(alunos, uf)
    return audit_join(numeric, coverage, ["ano", "serie", "rede"], "left", "validacao_brasil_cobertura").select(
        *numeric.columns, *CAMPOS_COBERTURA)


def verificar_fixture_cobertura_spark():
    """Optional Spark integration fixture; never writes synthetic analytical data."""
    global JOIN_METRICS
    real_join_metrics = JOIN_METRICS
    JOIN_METRICS = []
    try:
        alunos = spark.createDataFrame([
            (2023, "3304557", "2", "3", 2.0, True, True),
            (2023, "3106200", "2", "2", 1.0, True, False),
            (2023, "3550308", "2", "3", 10.0, False, None),
            (2023, "4205407", "2", "4", 100.0, True, True),
            (2024, "5300108", "2", "2", 1.0, True, True),
            (2024, "3550308", "2", "3", 3.0, True, False),
        ], "ano int, id_municipio string, serie string, rede string, peso_aluno double, avaliado boolean, alfabetizado_avaliado boolean")
        meta = spark.createDataFrame([(2023, "Pública", 66.6667), (2024, "Pública", 25.0)],
                                     "ano int, rede_descricao string, taxa_alfabetizacao double")
        uf = spark.createDataFrame([
            (2023, "2", "5", "RJ", 80.0), (2023, "2", "5", "MG", 20.0),
            (2023, "2", "5", "SP", 60.0), (2024, "2", "5", "SP", 40.0),
        ], "ano int, serie string, rede string, sigla_uf string, taxa_alfabetizacao double")
        enriched = construir_validacao_cruzada_brasil(alunos, meta, uf)
        rows = [r.asDict(recursive=True) for r in enriched.orderBy("ano").collect()]
        numeric_columns = ("ano", "serie", "rede", "alunos_avaliados", "taxa_alfabetizacao_calculada",
                           "taxa_oficial_publicada", "diferenca_pp", "dentro_tolerancia")
        expected_numeric = [(2023, "2", "5", 2, 66.6667, 66.6667, 0.0, True),
                            (2024, "2", "5", 2, 25.0, 25.0, 0.0, True)]
        actual_numeric = [tuple(r[c] for c in numeric_columns) for r in rows]
        by_year = {r["ano"]: r for r in rows}
        numeric_ok = actual_numeric == expected_numeric
        coverage_ok = (
            len(rows) == 2 and set(by_year) == {2023, 2024}
            and by_year[2023]["ufs_microdados"] == ["MG", "RJ"]
            and by_year[2023]["ufs_resultados_sem_microdados"] == ["SP"]
            and by_year[2024]["ufs_microdados"] == ["DF", "SP"]
            and by_year[2024]["ufs_microdados_sem_resultado_uf"] == ["DF"]
            and all(r["quantidade_ufs_microdados"] == 2 for r in rows)
            and all(r["status_comparabilidade"] == "COBERTURA_DIFERENTE_ENTRE_BASES" for r in rows)
        )
        rejected = False
        try:
            invalid_fixture = alunos.where(F.col("avaliado") & F.col("rede").isin("2", "3")).limit(1)
            construir_cobertura_brasil(invalid_fixture.withColumn("id_municipio", F.lit("0000000")), uf)
        except RuntimeError as error:
            if "Unknown municipality UF prefix" not in str(error):
                raise
            rejected = True
        details = json.dumps({"synthetic": True, "fixture_join_metrics": JOIN_METRICS,
                              "numeric_rows": [list(r) for r in actual_numeric],
                              "observed_ufs": {str(y): r["ufs_microdados"] for y, r in by_year.items()}}, ensure_ascii=False)
        extra_checks = regras_extras("validacao_cruzada_brasil", enriched)
        return [
            regra("fixture_spark_taxas_e_cardinalidade", "bloqueante", int(numeric_ok), 1, numeric_ok, details),
            regra("fixture_spark_filtro_privados_ausentes_cobertura", "bloqueante", int(coverage_ok), 1, coverage_ok,
                  "synthetic=true; private SC and absent SP must not enter the public denominator/coverage"),
            regra("fixture_spark_prefixo_uf_desconhecido_rejeitado", "bloqueante", int(rejected), 1, rejected,
                  "synthetic=true; an unknown municipality UF prefix must not disappear silently"),
        ] + [{**r, "regra": "fixture_" + r["regra"], "detalhe": "synthetic=true; " + r["detalhe"]}
             for r in extra_checks]
    finally:
        JOIN_METRICS = real_join_metrics


def construir_features(indicador, meta_municipio):
    """Features em t (ano_features) e alvo em t+1 (ano_alvo). Só entram dados conhecidos até t.

    Excluídas de propósito: taxa, média de português e proporções de nível do ano t+1 (são o próprio alvo ou derivam dele),
    `status_meta`, `diferenca_pp` e `dentro_tolerancia` (derivadas de resultado e validação).
    `id_municipio` é identificador, não feature numérica.
    As proporções por nível (`proporcao_aluno_nivel_*`) ficaram de fora: na fonte só existem para 2024, então seriam
    sempre nulas nas features de 2023. Com mais anos, dá para incluí-las (do ano t).
    """
    atual = indicador.select(
        F.col("ano").alias("ano_features"), "id_municipio", "serie", "rede", "rede_descricao",
        F.col("taxa_alfabetizacao_oficial").alias("taxa_t"),
        F.col("taxa_alfabetizacao_calculada").alias("taxa_calculada_t"),
        F.col("media_portugues_oficial").alias("media_portugues_t"),
        F.col("alunos_avaliados").alias("alunos_avaliados_t"),
    )
    alvo = indicador.where(F.col("taxa_alfabetizacao_oficial").isNotNull()).select(
        (F.col("ano") - 1).alias("ano_features"), "id_municipio", "serie", "rede",
        F.col("ano").alias("ano_alvo"),
        F.col("taxa_alfabetizacao_oficial").alias("alvo_taxa_alfabetizacao"),
    )
    meta_alvo = (
        meta_municipio.withColumn("rede", codigo_rede_meta())
        .select(F.col("ano").alias("ano_features"), "id_municipio", "rede",
                meta_do_ano("ano", deslocamento=1).alias("meta_ano_alvo"))
    )
    chaves = ["ano_features", "id_municipio", "serie", "rede"]
    max_year = indicador.agg(F.max("ano").alias("max_year")).first()["max_year"]
    eligible = atual.where(F.col("ano_features") < F.lit(max_year))
    paired = audit_join(atual, alvo, chaves, "inner", "features_t_para_t_mais_1", eligible=eligible)
    # Eligible coverage is defined by network and available years, never by
    # matching municipality IDs (missing geographic matches must still fail).
    available_years = [r["ano_features"] for r in meta_alvo.select("ano_features").distinct().collect()]
    meta_eligible = paired.where((F.col("rede") == "3") & F.col("ano_features").isin(available_years))
    return audit_join(paired, meta_alvo, ["ano_features", "id_municipio", "rede"],
                      "left", "features_meta_contextual", eligible=meta_eligible,
                      universe_description="Network 3 and feature year present in municipal meta; no geography-based eligibility filter")

def regra(nome, tipo, medido, limite, passou, detalhe):
    return dict(regra=nome, tipo=tipo, valor_medido=float(medido),
                limite=None if limite is None else float(limite), passou=bool(passou), detalhe=detalhe)

def exigir(entidade, resultados):
    """Levanta erro se alguma regra BLOQUEANTE reprovou. Não há try/except: a falha interrompe a execução."""
    falhas = [r for r in resultados if r["tipo"] == "bloqueante" and not r["passou"]]
    if falhas:
        resumo = "; ".join(f"{r['regra']}={r['valor_medido']:.4g}" for r in falhas)
        raise RuntimeError(f"[{entidade}] regras bloqueantes reprovadas: {resumo}")

def taxa_correspondencia(esquerda, direita, chaves):
    """Parte das linhas da esquerda que encontram par na direita (join que não casa vira nulo, não erro)."""
    pares = direita.select(*chaves).distinct().withColumn("_achou", F.lit(1))
    j = esquerda.select(*chaves).join(pares, list(chaves), "left")
    total = j.count()
    casados = j.where(F.col("_achou").isNotNull()).count()
    return (casados / total if total else 0.0), casados, total

def regras_dos_joins(silver):
    """Rodam ANTES de construir/publicar a Gold. Taxas abaixo do limiar são bloqueantes."""
    res = []
    calculada = taxa_calculada_microdados(silver["alunos"])
    mm = silver["meta_alfabetizacao_municipio"].withColumn("rede", codigo_rede_meta())
    mf = silver["meta_alfabetizacao_uf"].withColumn("rede", codigo_rede_meta())

    sem_mapa = (silver["meta_alfabetizacao_municipio"].where(codigo_rede_meta().isNull()).count()
                + silver["meta_alfabetizacao_uf"].where(codigo_rede_meta().isNull()).count()
                + silver["meta_alfabetizacao_brasil"].where(codigo_rede_meta().isNull()).count())
    res.append(regra("rede_de_meta_sem_mapeamento", "bloqueante", sem_mapa, 0, sem_mapa == 0,
                     "linhas de meta com rede fora do mapa REDE_META_PARA_CODIGO"))

    joins = [
        ("municipio -> microdados recalculados", silver["municipio"], calculada, ["ano", "id_municipio", "serie", "rede"]),
        ("municipio(rede 3) -> meta_alfabetizacao_municipio", silver["municipio"].where((F.col("rede") == "3") & (F.col("serie") == CFG["SERIE_BRASIL"])), mm,
         ["ano", "id_municipio", "rede"]),
        ("uf(rede 5) -> meta_alfabetizacao_uf", silver["uf"].where((F.col("rede") == "5") & (F.col("serie") == CFG["SERIE_BRASIL"])), mf, ["ano", "sigla_uf", "rede"]),
    ]
    for nome, esq, dir_, chaves in joins:
        taxa, casados, total = taxa_correspondencia(esq, dir_, chaves)
        res.append(regra(f"taxa_correspondencia:{nome}", "bloqueante", taxa, LIMIAR_JOIN, taxa >= LIMIAR_JOIN,
                         f"{casados} de {total} linhas casam"))

    # Informativa: linhas de meta sem resultado correspondente.
    for nome, meta, resultado, chaves in [
        ("meta_alfabetizacao_municipio sem municipio(rede 3)", mm,
         silver["municipio"].where((F.col("rede") == "3") & (F.col("serie") == CFG["SERIE_BRASIL"])), ["ano", "id_municipio", "rede"]),
        ("meta_alfabetizacao_uf sem uf(rede 5)", mf, silver["uf"].where((F.col("rede") == "5") & (F.col("serie") == CFG["SERIE_BRASIL"])), ["ano", "sigla_uf", "rede"]),
    ]:
        taxa, casados, total = taxa_correspondencia(meta, resultado, chaves)
        res.append(regra(f"meta_sem_resultado:{nome}", "informativa", total - casados, None, True,
                         f"{total - casados} de {total} linhas de meta sem resultado (ex.: anos sem microdado)"))

    # Informativa: a taxa das tabelas de meta confere com a das tabelas de resultado? (valida o mapeamento de rede)
    for nome, meta, resultado, chaves in [
        ("meta_municipio x municipio", mm, silver["municipio"], ["ano", "id_municipio", "rede"]),
        ("meta_uf x uf", mf, silver["uf"], ["ano", "sigla_uf", "rede"]),
    ]:
        resultado = resultado.where(F.col("serie") == CFG["SERIE_BRASIL"])
        # Consistency enriches published results with their available reference.
        # Reference rows lacking a published result remain separately audited
        # by meta_sem_resultado; they cannot create a missing official result.
        eligible_pairs = meta.select("ano", "rede").distinct().collect()
        eligible_condition = reduce(lambda a, b: a | b,
            [(F.col("ano") == row["ano"]) & (F.col("rede") == row["rede"]) for row in eligible_pairs], F.lit(False))
        eligible_result = resultado.where(eligible_condition).select(*chaves, F.col("taxa_alfabetizacao").alias("t_res"))
        j = (audit_join(resultado.select(*chaves, F.col("taxa_alfabetizacao").alias("t_res")),
                        meta.select(*chaves, F.col("taxa_alfabetizacao").alias("t_meta")),
                        chaves, "inner", "consistencia_resultado_x_meta:" + nome, eligible=eligible_result,
                        universe_description="Published official second-year results whose year/network pair exists in meta references; no geography-based eligibility filter")
             .where(F.col("t_meta").isNotNull() & F.col("t_res").isNotNull()))
        total = j.count()
        ok = j.where(F.abs(F.col("t_meta") - F.col("t_res")) <= TOLERANCIA_PP).count()
        res.append(regra(f"consistencia_taxa:{nome}", "informativa", ok / total if total else 0.0, None, True,
                         f"{ok} de {total} pares dentro de {TOLERANCIA_PP} p.p."))
    return res

def regras_extras(nome, df):
    res = []
    if nome == "resultado_vs_meta":
        estados = {r["status_meta"]: r["count"] for r in df.groupBy("status_meta").count().collect()}
        invalidos = sum(v for k, v in estados.items() if k not in ("atingiu", "nao_atingiu", "sem_meta", "sem_resultado"))
        res.append(regra("status_meta_valores_permitidos", "bloqueante", invalidos, 0, invalidos == 0,
                         "valores fora de atingiu / nao_atingiu / sem_meta / sem_resultado"))
        for k in ("atingiu", "nao_atingiu", "sem_meta", "sem_resultado"):
            res.append(regra(f"linhas_status_meta:{k}", "informativa", estados.get(k, 0), None, True,
                             "sem_meta > 0 prova que o terceiro estado funciona" if k == "sem_meta" else "contagem"))
    if nome in ("indicador_municipio", "validacao_cruzada_brasil"):
        ok = df.where(F.col("dentro_tolerancia")).count()
        com_calc = df.where(F.col("dentro_tolerancia").isNotNull()).count()
        res.append(regra("recalculada_dentro_da_tolerancia", "informativa", ok / com_calc if com_calc else 0.0, None, True,
                         f"{ok} de {com_calc} linhas com |recalculada - oficial| <= {TOLERANCIA_PP} p.p."))
        sem_calc = df.where(F.col("dentro_tolerancia").isNull()).count()
        res.append(regra("linhas_sem_recalculo", "informativa", sem_calc, None, True,
                         "sem microdado correspondente (taxa recalculada nula)"))
    if nome == "validacao_cruzada_brasil":
        missing = df.where(reduce(lambda a, b: a | b,
                                 [F.col(c).isNull() for c in CAMPOS_COBERTURA])).count()
        invalid_status = df.where(~F.col("status_comparabilidade").isin(list(STATUS_COMPARABILIDADE))).count()
        invalid_sets = df.where(
            (F.size("ufs_microdados") != F.col("quantidade_ufs_microdados"))
            | (F.size(F.array_intersect("ufs_microdados", "ufs_sem_microdados")) != 0)
            | (F.size(F.array_union("ufs_microdados", "ufs_sem_microdados")) != len(UF_IBGE))
        ).count()
        res.extend([
            regra("cobertura_metadados_presentes", "bloqueante", missing, 0, missing == 0,
                  "Every national row needs geographic evidence; a numeric match alone is insufficient"),
            regra("cobertura_status_valido", "bloqueante", invalid_status, 0, invalid_status == 0,
                  "Comparability status is independent of dentro_tolerancia"),
            regra("cobertura_conjuntos_consistentes", "bloqueante", invalid_sets, 0, invalid_sets == 0,
                  "Present and missing UF sets partition the 27-UF geographic reference"),
        ])
        for row in df.select("ano", "serie", "rede", *CAMPOS_COBERTURA).orderBy("ano", "serie", "rede").collect():
            res.append(regra(f"cobertura_uf:{row['ano']}:{row['serie']}:{row['rede']}", "informativa",
                             row["quantidade_ufs_microdados"], None, True,
                             json.dumps(row.asDict(recursive=True), ensure_ascii=False)))
    if nome == "features_alfabetizacao":
        alvo_nulo = df.where(F.col("alvo_taxa_alfabetizacao").isNull()).count()
        res.append(regra("alvo_sem_nulo", "bloqueante", alvo_nulo, 0, alvo_nulo == 0, "linhas sem alvo"))
        com_meta = df.where(F.col("meta_ano_alvo").isNotNull()).count()
        res.append(regra("linhas_com_meta_do_ano_alvo", "informativa", com_meta, None, True, "meta nula é preservada, não vira zero"))
    return res

def regras_basicas(df, chaves, colunas_taxa=(), nullable_keys=()):
    total = df.count()
    unique = df.select(*chaves).distinct().count()
    not_nullable = [c for c in chaves if c not in nullable_keys]
    null_count = df.where(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in not_nullable])).count()
    results = [
        regra("linhas_maior_que_zero", "bloqueante", total, 1, total > 0, "Rows in staged table"),
        regra("chave_sem_duplicata", "bloqueante", total - unique, 0, total == unique, str(chaves)),
        regra("chaves_obrigatorias_sem_nulo", "bloqueante", null_count, 0, null_count == 0, str(not_nullable)),
    ]
    for column in colunas_taxa:
        bad = df.where(F.col(column).isNotNull() &
                       (invalid_number(column) | (F.col(column) < 0) | (F.col(column) > 100))).count()
        results.append(regra("valor_finito_entre_0_e_100:" + column, "bloqueante", bad, 0, bad == 0,
                             "Null remains null; NaN/infinity/out-of-range block publication"))
    return results


def validate_level_keys(df):
    bad = ~F.col("nivel").isin("municipio", "uf", "brasil")
    if "id_municipio" in df.columns:
        bad = bad | ((F.col("nivel") == "municipio") & (F.col("id_municipio").isNull() | F.col("serie").isNull()))
    else:
        bad = bad | (F.col("nivel") == "municipio")
    bad = bad | ((F.col("nivel") == "uf") & (F.col("sigla_uf").isNull() | F.col("serie").isNull()))
    invalid = df.where(bad).count()
    return regra("chaves_por_nivel", "bloqueante", invalid, 0, invalid == 0,
                 "Municipality needs municipality/series; UF needs UF/series; Brazil has neither geography key")


def stage_gold(name, df, keys, rate_columns=(), nullable_keys=()):
    """Write immutable data, reread it, validate it; no latest alias changes."""
    prefix = f"{CFG['GOLD_PREFIX']}/{name}/ingestion_date={INGESTION_DATE}/run_id={ID_EXECUCAO}/"
    uri = f"s3://{CFG['S3_BUCKET']}/{prefix}"
    started, timer = utcnow(), monotonic()
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        source_count = df.count()
        if source_count <= 0:
            results = [regra("linhas_maior_que_zero", "bloqueante", source_count, 1, False, "No output written")]
            registrar_qualidade(name, results)
            exigir(name, results)
        (df.write.mode("errorifexists").option("compression", "snappy").parquet(uri))
        reread = spark.read.parquet(uri).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            results = regras_basicas(reread, keys, rate_columns, nullable_keys) + regras_extras(name, reread)
            output_count = int(results[0]["valor_medido"])
            results.append(regra("contagem_gravada_confere", "bloqueante", output_count, source_count,
                                 output_count == source_count, f"built={source_count}, S3={output_count}"))
            if name in ("resultado_vs_meta", "evolucao_temporal"):
                results.append(validate_level_keys(reread))
            registrar_qualidade(name, results)
            metric = {"table": name, "s3_uri": uri, "source_count": source_count,
                      "s3_count": output_count, "schema": json.loads(reread.schema.json()),
                      "keys": keys, "status": "STAGED", "started_at_utc": started,
                      "finished_at_utc": utcnow(), "duration_seconds": round(monotonic() - timer, 3)}
            TABLE_METRICS.append(metric)
            exigir(name, results)
            return reread
        except Exception:
            reread.unpersist()
            raise
    finally:
        df.unpersist()


def register_versioned_tables(glue):
    """Optional metadata only. Catalog writes are not atomic across tables.

    Never update a stable latest table. A consumer must first check this run's
    SUCCEEDED manifest, which is written only after every registration succeeds.
    """
    if not CFG["REGISTER_CATALOG"]:
        return
    try:
        glue.get_database(Name=CFG["DATABASE_GOLD"])
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": CFG["DATABASE_GOLD"],
                                           "Description": "FIAP Gold; consume only SUCCEEDED manifest runs"})
    suffix = hashlib.sha256(ID_EXECUCAO.encode()).hexdigest()[:16]
    for metric in TABLE_METRICS:
        name = metric["table"] + "_r_" + suffix
        frame = spark.read.parquet(metric["s3_uri"])
        columns = [{"Name": field.name, "Type": field.dataType.simpleString()} for field in frame.schema.fields]
        definition = {
            "Name": name, "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE", "fiap_run_id": ID_EXECUCAO,
                           "fiap_commit_manifest": f"s3://{CFG['S3_BUCKET']}/{AUDIT_RUN}/manifest.json"},
            "StorageDescriptor": {"Columns": columns, "Location": metric["s3_uri"],
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                              "Parameters": {"serialization.format": "1"}}},
        }
        glue.create_table(DatabaseName=CFG["DATABASE_GOLD"], TableInput=definition)
        metric["catalog_table"] = CFG["DATABASE_GOLD"] + "." + name


def main(overrides=None):
    global CFG, S3, spark, ID_EXECUCAO, DATA_EXECUCAO, INGESTION_DATE, AUDIT_RUN
    global LIMIAR_JOIN, TOLERANCIA_PP, QUALITY, JOIN_METRICS, TABLE_METRICS
    CFG = params(overrides)
    ID_EXECUCAO = CFG["JOB_RUN_ID"]
    DATA_EXECUCAO = datetime.now(timezone.utc)
    INGESTION_DATE = DATA_EXECUCAO.date().isoformat()
    LIMIAR_JOIN, TOLERANCIA_PP = CFG["LIMIAR_JOIN"], CFG["TOLERANCIA_PP"]
    AUDIT_RUN = f"{CFG['AUDIT_PREFIX']}/ingestion_date={INGESTION_DATE}/run_id={ID_EXECUCAO}"
    QUALITY, JOIN_METRICS, TABLE_METRICS = {}, [], []
    S3, glue = boto3.client("s3"), boto3.client("glue")
    started, timer = utcnow(), monotonic()
    put_json(AUDIT_RUN + "/start.json", {"run_id": ID_EXECUCAO, "status": "RUNNING",
             "started_at_utc": started, "script_version": SCRIPT_VERSION,
             "original_notebook_sha256": ORIGINAL_NOTEBOOK_SHA256,
             "silver_manifest_uri": CFG["SILVER_MANIFEST_URI"],
             "demo_failure": CFG["DEMO_FAILURE"]})
    silver, staged, lineage = {}, [], {}
    try:
        # Audit writes precede metadata preflight; no Gold data or Spark scans
        # occur until the committed Silver manifest has passed its contract.
        lineage = inspect_silver(glue, CFG)
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from pyspark.context import SparkContext
        ctx = GlueContext(SparkContext.getOrCreate())
        spark = ctx.spark_session
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.shuffle.partitions", "8")
        job = Job(ctx)
        job.init(CFG["JOB_NAME"], {k: str(v) for k, v in CFG.items()})
        if CFG["VERIFY_COVERAGE_FIXTURE"]:
            fixture_results = verificar_fixture_cobertura_spark()
            registrar_qualidade("coverage_spark_fixture", fixture_results)
            exigir("coverage_spark_fixture", fixture_results)
        for name in NOMES_SILVER:
            frame = spark.read.parquet(lineage[name]["location"])
            actual = {f.name: f.dataType.simpleString() for f in frame.schema.fields}
            for column, expected in required_columns()[name].items():
                if column not in actual or not type_matches(actual[column], expected):
                    raise RuntimeError(f"Physical Parquet schema mismatch: {name}.{column}")
            # Project declared Silver columns only; directory partition inference
            # must never inject Gold or source snapshot metadata into the output.
            frame = frame.select(*required_columns()[name]).persist(StorageLevel.MEMORY_AND_DISK)
            count = frame.count()
            if count <= 0 or count != lineage[name]["expected_count"]:
                raise RuntimeError("Silver count differs from committed manifest: " + name)
            lineage[name]["row_count"] = count
            silver[name] = frame
        result = regras_entrada(silver)
        registrar_qualidade("silver_inputs", result)
        exigir("silver_inputs", result)
        joins = regras_dos_joins(silver)
        registrar_qualidade("source_joins", joins)
        exigir("source_joins", joins)

        indicador = stage_gold("indicador_municipio", construir_indicador_municipio(silver["municipio"], silver["alunos"]),
                               ["ano", "id_municipio", "serie", "rede"],
                               ["taxa_alfabetizacao_oficial", "taxa_alfabetizacao_calculada"])
        staged.append(indicador)
        resultado_input = construir_resultado_vs_meta(silver["municipio"], silver["uf"],
                                silver["meta_alfabetizacao_brasil"], silver["meta_alfabetizacao_uf"],
                                silver["meta_alfabetizacao_municipio"])
        if CFG["DEMO_FAILURE"]:
            # Isolated run, preserved as FAILED evidence; never commits Gold.
            resultado_input = resultado_input.withColumn("taxa_alfabetizacao", F.lit(150.0))
        resultado = stage_gold("resultado_vs_meta", resultado_input,
                               ["nivel", "ano", "id_municipio", "sigla_uf", "serie", "rede"],
                               ["taxa_alfabetizacao", "meta_alfabetizacao_ano"], ["id_municipio", "sigla_uf", "serie"])
        staged.append(resultado)
        evolucao = stage_gold("evolucao_temporal", construir_evolucao_temporal(resultado),
                              ["nivel", "sigla_uf", "ano", "serie", "rede"], ["taxa_alfabetizacao"], ["sigla_uf", "serie"])
        staged.append(evolucao)
        validacao = stage_gold("validacao_cruzada_brasil", construir_validacao_cruzada_brasil(silver["alunos"],
                                silver["meta_alfabetizacao_brasil"], silver["uf"]), ["ano", "serie", "rede"],
                               ["taxa_alfabetizacao_calculada", "taxa_oficial_publicada"])
        staged.append(validacao)
        features = stage_gold("features_alfabetizacao", construir_features(indicador, silver["meta_alfabetizacao_municipio"]),
                               ["ano_features", "id_municipio", "serie", "rede"], ["taxa_t", "alvo_taxa_alfabetizacao"])
        staged.append(features)
        registrar_qualidade("all_construction_joins", JOIN_METRICS)
        # Detect catalog changes during execution; immutable source locations
        # remain an upstream requirement (this is not a multi-table transaction).
        refreshed = inspect_silver(glue, CFG)
        for name in NOMES_SILVER:
            if any(refreshed[name][k] != lineage[name][k] for k in ("version_id", "location", "columns", "silver_manifest_sha256")):
                raise RuntimeError("Silver manifest changed during the run: " + name)
        register_versioned_tables(glue)
        job.commit()
        for metric in TABLE_METRICS:
            metric["status"] = "VALIDATED"
        manifest = {
            "status": "SUCCEEDED", "run_id": ID_EXECUCAO, "job_name": CFG["JOB_NAME"],
            "script_version": SCRIPT_VERSION, "original_notebook_sha256": ORIGINAL_NOTEBOOK_SHA256,
            "started_at_utc": started, "finished_at_utc": utcnow(), "source_tables": lineage,
            "duration_seconds": round(monotonic() - timer, 3),
            "table_metrics": TABLE_METRICS, "all_five_tables_validated": len(TABLE_METRICS) == 5,
            "quality": QUALITY, "catalog_registration_enabled": CFG["REGISTER_CATALOG"],
            "publication_contract": "Consume only paths listed in this SUCCEEDED manifest; catalog writes are not atomic",
            "training_feature_columns": ["serie", "rede", "taxa_t", "taxa_calculada_t", "media_portugues_t", "alunos_avaliados_t"],
            "training_identifiers": ["id_municipio", "ano_features", "ano_alvo"],
            "target_column": "alvo_taxa_alfabetizacao",
            "meta_ano_alvo_usage": "Context only; excluded from model features without proof of point-in-time availability",
            "national_comparison_cohort": {"serie": CFG["SERIE_BRASIL"], "meaning": "Second year of elementary school", "network": "5"},
            "national_coverage_audit": {
                "geographic_reference": "27 UFs; not pupil participation or proof of the official Brazil universe",
                "comparison": "Evaluated public pupils versus published UF results of the same year/series",
                "numeric_tolerance_unchanged_pp": TOLERANCIA_PP,
                "rows": [row.asDict(recursive=True) for row in validacao.select(
                    "ano", "serie", "rede", *CAMPOS_COBERTURA).orderBy("ano", "serie", "rede").collect()],
            },
            "coverage_spark_fixture": {"enabled": CFG["VERIFY_COVERAGE_FIXTURE"], "synthetic": True,
                                       "writes_analytical_data": False},
            "simulation": False,
        }
        result = put_json(AUDIT_RUN + "/manifest.json", manifest)
        emit_outcome(False, monotonic() - timer)
        print(json.dumps({"status": "SUCCEEDED", "run_id": ID_EXECUCAO, "manifest": result}, ensure_ascii=False))
        return manifest
    except Exception as error:
        emit_outcome(True, monotonic() - timer)
        # Keep original failure even if auditing itself is unavailable.
        try:
            put_json(AUDIT_RUN + "/failed.json", {
                "status": "FAILED", "run_id": ID_EXECUCAO, "finished_at_utc": utcnow(),
                "duration_seconds": round(monotonic() - timer, 3), "demo_failure": CFG["DEMO_FAILURE"],
                "error_type": type(error).__name__, "error": str(error)[:3000],
                "staged_tables": TABLE_METRICS, "quality": QUALITY, "joins": JOIN_METRICS,
                "published": False, "source_tables": lineage,
            })
        except Exception as audit_error:
            print("Failure audit could not be persisted: " + type(audit_error).__name__)
        raise
    finally:
        for frame in staged + list(silver.values()):
            frame.unpersist()


if __name__ == "__main__":
    main()
