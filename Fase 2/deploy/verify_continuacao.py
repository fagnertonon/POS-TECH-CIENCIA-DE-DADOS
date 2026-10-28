"""Observador somente leitura para os jobs Silver/Gold do Tech Challenge.

Escreve apenas o relatorio local indicado. Nao altera S3, Glue, filas ou catalogos.
--verify-parquet le um objeto por vez em memoria e conta linhas pelos footers,
sem executar Spark nem imprimir amostras de dados. O estado FAILED e um resultado
do relatorio, nao uma excecao omitida pelo observador.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

ACCOUNT = "637246457028"
REGION = "us-east-2"
TABLES = {
    "silver": {"alunos", "municipio", "uf", "dicionario", "meta_alfabetizacao_brasil",
               "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio"},
    "gold": {"indicador_municipio", "resultado_vs_meta", "evolucao_temporal",
             "validacao_cruzada_brasil", "features_alfabetizacao"},
}
TERMINAL_FAILURES = {"FAILED", "TIMEOUT", "ERROR", "STOPPED", "EXPIRED"}


def utcnow():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def s3_location(uri):
    value = urlsplit(uri)
    if value.scheme != "s3" or not value.netloc or not value.path.strip("/") or value.query or value.fragment:
        raise ValueError("URI S3 invalida")
    return value.netloc, value.path.lstrip("/")


def load_json(s3, bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    try:
        content = response["Body"].read()
    finally:
        response["Body"].close()
    return json.loads(content), hashlib.sha256(content).hexdigest()


def inventory(s3, bucket, prefix):
    result = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        result.extend(page.get("Contents", []))
    return result


def find_audits(s3, bucket, layer, run_id):
    suffix = f"/run_id={run_id}/"
    found = {}
    for item in inventory(s3, bucket, f"auditoria/{layer}/"):
        key = item["Key"]
        for filename in ("start.json", "manifest.json", "failure.json", "failed.json"):
            if key.endswith(suffix + filename):
                if filename in found:
                    raise ValueError(f"Mais de um {filename} para o mesmo run_id")
                found[filename] = key
    prefixes = {key.rsplit("/", 1)[0] for key in found.values()}
    if len(prefixes) > 1:
        raise ValueError("Documentos da execucao em prefixos de auditoria diferentes")
    return found


def normalize_quality(source):
    result = []
    groups = source.items() if isinstance(source, dict) else [(None, source)]
    for entity, rules in groups:
        if not isinstance(rules, list):
            continue
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            converted = {
                "entity": rule.get("table", entity), "rule": rule.get("rule", rule.get("regra")),
                "type": rule.get("type", rule.get("tipo")),
                "measured": rule.get("measured", rule.get("valor_medido")),
                "limit": rule.get("limit", rule.get("limite")),
                "passed": rule.get("passed", rule.get("passou")),
                "detail": rule.get("detail", rule.get("detalhe", "")),
            }
            if "aplicavel" in rule:
                converted["applicable"] = rule["aplicavel"]
            match = re.search(r"(\d+)\s*(?:/|of\b|de\b)\s*(\d+)", converted["detail"] or "")
            if match:
                converted.update(matched_rows=int(match[1]), eligible_rows=int(match[2]))
            result.append(converted)
    return result


def blocking_failures(quality):
    return [rule for rule in quality if rule["type"] in {"blocking", "bloqueante"}
            and rule["passed"] is False and rule.get("applicable", True) is not False]


def arrow_schema_errors(pa, schema, expected):
    errors = []
    fields = expected.get("fields", [])
    if [field["name"] for field in fields] != schema.names:
        return ["Nomes/ordem do schema fisico diferem do manifesto"]
    for field in fields:
        actual = schema.field(field["name"]).type
        declared = field["type"]
        if isinstance(declared, dict):
            valid = (declared.get("type") == "array" and declared.get("elementType") == "string"
                     and (pa.types.is_list(actual) or pa.types.is_large_list(actual))
                     and (pa.types.is_string(actual.value_type) or pa.types.is_large_string(actual.value_type)))
        elif declared in {"long", "bigint"}:
            valid = pa.types.is_int64(actual)
        elif declared in {"integer", "int"}:
            valid = pa.types.is_int32(actual)
        elif declared in {"short", "smallint"}:
            valid = pa.types.is_int16(actual)
        elif declared in {"byte", "tinyint"}:
            valid = pa.types.is_int8(actual)
        elif declared == "double":
            valid = pa.types.is_float64(actual)
        elif declared == "float":
            valid = pa.types.is_float32(actual)
        elif declared == "string":
            valid = pa.types.is_string(actual) or pa.types.is_large_string(actual)
        elif declared == "boolean":
            valid = pa.types.is_boolean(actual)
        elif declared in {"timestamp", "timestamp_ntz"}:
            valid = pa.types.is_timestamp(actual)
        elif declared == "date":
            valid = pa.types.is_date(actual)
        elif isinstance(declared, str) and declared.startswith("decimal("):
            valid = pa.types.is_decimal(actual)
        else:
            valid = False
        if not valid:
            errors.append(f"{field['name']}: esperado {declared}, fisico {actual}")
    return errors


def verify_table(s3, metric, layer, run_id, verify_parquet, max_bytes):
    name = metric.get("table")
    uri = metric.get("target_uri", metric.get("s3_uri", ""))
    result = {"table": name, "target_uri": uri, "source_count": metric.get("source_count"),
              "deduplicated_count": metric.get("deduplicated_count"),
              "target_count": metric.get("target_count", metric.get("s3_count")), "errors": []}
    try:
        bucket, prefix = s3_location(uri)
        if not prefix.startswith(layer + "/") or f"/run_id={run_id}/" not in "/" + prefix:
            raise ValueError("Destino nao pertence a camada/run_id declarado")
        if not prefix.endswith("/") or f"/{name}/" not in "/" + prefix:
            raise ValueError("Destino nao e o diretorio da entidade declarada")
        entries = inventory(s3, bucket, prefix)
        parquet = [item for item in entries if item["Key"].endswith(".parquet")]
        result.update(parquet_file_count=len(parquet), parquet_bytes=sum(item["Size"] for item in parquet),
                      object_count=len(entries), object_bytes=sum(item["Size"] for item in entries),
                      audit_table_status=metric.get("status"), schema=metric.get("schema"),
                      duration_seconds=metric.get("duration_seconds"),
                      transform_duration_seconds=metric.get("transform_duration_seconds"),
                      write_readback_duration_seconds=metric.get("write_readback_duration_seconds"))
        if not parquet:
            result["errors"].append("Nenhum arquivo Parquet encontrado")
        if metric.get("status") not in {"SUCCEEDED", "VALIDATED"}:
            result["errors"].append("Entidade nao tem status de validacao final")
        expected_count = result["target_count"]
        before_write = metric.get("deduplicated_count") if layer == "silver" else metric.get("source_count")
        if type(expected_count) is not int or expected_count <= 0 or expected_count != before_write:
            result["errors"].append("Contagem auditada de saida invalida ou difere do DF anterior a escrita")
        if layer == "silver" and metric.get("counts_match") is not True:
            result["errors"].append("Silver nao declara counts_match=true")
        for field in ("parquet_file_count", "parquet_bytes"):
            if field in metric and metric[field] != result[field]:
                result["errors"].append(f"Inventario {field} difere da auditoria")
        if not isinstance(metric.get("schema"), dict) or not metric["schema"].get("fields"):
            result["errors"].append("Schema auditado ausente")
        if verify_parquet:
            import pyarrow as pa
            import pyarrow.parquet as pq
            footer_count = 0
            for item in parquet:
                if item["Size"] > max_bytes:
                    raise ValueError(f"Objeto supera limite de memoria permitido: {item['Size']} bytes")
                response = s3.get_object(Bucket=bucket, Key=item["Key"])
                try:
                    content = response["Body"].read()
                finally:
                    response["Body"].close()
                parquet_file = pq.ParquetFile(pa.BufferReader(content))
                footer_count += parquet_file.metadata.num_rows
                result["errors"].extend(arrow_schema_errors(pa, parquet_file.schema_arrow, metric.get("schema", {})))
                del parquet_file, content
            result["independent_footer_rows"] = footer_count
            result["footer_rows_match"] = footer_count == expected_count
            if footer_count != expected_count:
                result["errors"].append("Contagem independente dos footers Parquet diverge do manifesto")
        else:
            result["independent_footer_rows"] = None
            result["footer_rows_match"] = None
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    result["validation_status"] = "SUCCEEDED" if not result["errors"] else "FAILED"
    return result


def inspect(args):
    import boto3
    session = boto3.Session(region_name=args.region)
    sts, glue, s3 = (session.client(name) for name in ("sts", "glue", "s3"))
    account = sts.get_caller_identity()["Account"]
    if account != ACCOUNT:
        raise ValueError(f"Conta ativa {account} difere da conta autorizada {ACCOUNT}")
    job_name = args.job_name or f"fiap-{args.layer}-inep-batch"
    run = glue.get_job_run(JobName=job_name, RunId=args.run_id, PredecessorsIncluded=False)["JobRun"]
    selected = ("Id", "JobName", "JobRunState", "StartedOn", "CompletedOn", "LastModifiedOn", "ExecutionTime",
                "DPUSeconds", "WorkerType", "NumberOfWorkers", "MaxCapacity", "Timeout", "GlueVersion",
                "ErrorMessage", "Attempt", "LogGroupName")
    glue_summary = {key: run[key] for key in selected if key in run}
    if isinstance(run.get("ExecutionTime"), (int, float)) and isinstance(run.get("NumberOfWorkers"), int):
        glue_summary["allocated_worker_seconds_estimate"] = run["ExecutionTime"] * run["NumberOfWorkers"]
        glue_summary["allocated_worker_seconds_note"] = "ExecutionTime x NumberOfWorkers; nao e fatura nem preco"
    safe_args = ("--BRONZE_MANIFEST_URI", "--SILVER_MANIFEST_URI", "--DEMO_FAILURE", "--REGISTER_CATALOG")
    glue_summary["source_and_mode_parameters"] = {key: run.get("Arguments", {})[key] for key in safe_args
                                                 if key in run.get("Arguments", {})}
    report = {"observed_at_utc": utcnow(), "account": account, "region": args.region, "layer": args.layer,
              "run_id": args.run_id, "job_name": job_name, "glue_job_run": glue_summary,
              "parquet_footer_verification_requested": args.verify_parquet,
              "status": run["JobRunState"], "published": False, "errors": [], "pending": [], "tables": [],
              "quality_scope": "Regras Spark persistidas no manifesto; observador nao recalcula valores das linhas"}
    found = find_audits(s3, args.bucket, args.layer, args.run_id)
    report["audit_uris"] = {name: f"s3://{args.bucket}/{key}" for name, key in found.items()}
    report["publication_manifest_present"] = "manifest.json" in found
    failure_name = "failure.json" if "failure.json" in found else "failed.json" if "failed.json" in found else None
    if "manifest.json" in found:
        manifest, digest = load_json(s3, args.bucket, found["manifest.json"])
        report.update(manifest_uri=report["audit_uris"]["manifest.json"], manifest_sha256=digest,
                      manifest_status=manifest.get("status"), duration_seconds=manifest.get("duration_seconds"),
                      started_at_utc=manifest.get("started_at_utc"), finished_at_utc=manifest.get("finished_at_utc"),
                      source_manifest_uri=manifest.get("bronze_manifest_uri"),
                      catalog_registered=manifest.get("catalog_registered", manifest.get("catalog_registration_enabled")))
        if args.layer == "gold":
            source_tables = manifest.get("source_tables", {})
            source_uris = {entry.get("silver_manifest_uri") for entry in source_tables.values()}
            source_runs = {entry.get("silver_run_id") for entry in source_tables.values()}
            report["source_manifest_uris"] = sorted(uri for uri in source_uris if uri)
            report["source_silver_run_ids"] = sorted(value for value in source_runs if value)
            report["source_row_counts"] = {name: entry.get("row_count") for name, entry in source_tables.items()}
            if len(source_uris) != 1 or None in source_uris or len(source_runs) != 1 or None in source_runs:
                report["errors"].append("Linhagem Gold nao identifica um unico manifesto/run Silver")
            report["training_feature_columns"] = manifest.get("training_feature_columns")
            report["target_column"] = manifest.get("target_column")
        if manifest.get("status") != "SUCCEEDED" or manifest.get("run_id") != args.run_id:
            report["errors"].append("Manifesto nao corresponde a execucao SUCCEEDED solicitada")
        if args.layer == "silver" and (manifest.get("layer") != "silver" or manifest.get("is_full_batch") is not True
                                        or manifest.get("all_counts_match") is not True):
            report["errors"].append("Contrato Silver completo nao satisfeito")
        if args.layer == "gold" and manifest.get("all_five_tables_validated") is not True:
            report["errors"].append("Gold nao declara todas as cinco tabelas validadas")
        metrics = manifest.get("tables" if args.layer == "silver" else "table_metrics", [])
        if len(metrics) != len(TABLES[args.layer]) or {row.get("table") for row in metrics} != TABLES[args.layer]:
            report["errors"].append("Lista de entidades incompleta, repetida ou inesperada")
        quality = normalize_quality(manifest.get("quality", []))
        report["quality"] = quality
        if not quality:
            report["errors"].append("Nenhuma regra de qualidade auditada")
        if blocking_failures(quality):
            report["errors"].append("Manifesto SUCCEEDED contem regra bloqueante reprovada")
        report["joins"] = [rule for rule in quality if "match_rate" in (rule["rule"] or "")
                           or "correspondencia" in (rule["rule"] or "") or "cobertura_total:" in (rule["rule"] or "")]
        for metric in metrics:
            result = verify_table(s3, metric, args.layer, args.run_id, args.verify_parquet,
                                  args.max_object_mb * 1024 * 1024)
            report["tables"].append(result)
            report["errors"].extend(f"{result['table']}: {error}" for error in result["errors"])
        report["total_output_rows"] = sum(row.get("target_count", 0) or 0 for row in report["tables"])
        report["total_parquet_files"] = sum(row.get("parquet_file_count", 0) for row in report["tables"])
        report["total_parquet_bytes"] = sum(row.get("parquet_bytes", 0) for row in report["tables"])
        if failure_name:
            report["errors"].append("Mesmo run_id tem manifesto SUCCEEDED e auditoria de falha")
        if run["JobRunState"] != "SUCCEEDED":
            report["errors"].append("Manifesto existe mas Glue nao terminou SUCCEEDED")
        report["published"] = not report["errors"]
        report["verification_status"] = "SUCCEEDED" if not report["errors"] else "FAILED"
        report["status"] = "SUCCEEDED" if not report["errors"] else "VALIDATION_FAILED"
    elif failure_name:
        failure, digest = load_json(s3, args.bucket, found[failure_name])
        quality = normalize_quality(failure.get("quality", []))
        expected_demo = failure.get("demo_failure") is True
        report.update(status="FAILED", failure_sha256=digest, failure_uri=report["audit_uris"][failure_name],
                      error_type=failure.get("error_type"), error=failure.get("error"),
                      duration_seconds=failure.get("duration_seconds"), quality=quality,
                      failed_blocking_rules=blocking_failures(quality), demo_failure=expected_demo,
                      staged_tables=failure.get("staged_tables", failure.get("tables", [])),
                      published=False, joins=normalize_quality(failure.get("joins", [])))
        if failure.get("status") != "FAILED" or failure.get("run_id") != args.run_id:
            report["errors"].append("Auditoria de falha nao corresponde a execucao")
        if expected_demo:
            injected_failure = [rule for rule in blocking_failures(quality)
                                if rule["entity"] == "resultado_vs_meta"
                                and rule["rule"] == "valor_finito_entre_0_e_100:taxa_alfabetizacao"
                                and isinstance(rule["measured"], (int, float)) and rule["measured"] > 0]
            report["injected_invalid_rate_blocked"] = bool(injected_failure)
            confirmed = run["JobRunState"] in TERMINAL_FAILURES and bool(injected_failure) and not report["errors"]
            report["verification_status"] = "EXPECTED_FAILURE_CONFIRMED" if confirmed else "EXPECTED_FAILURE_NOT_CONFIRMED"
        else:
            report["verification_status"] = "FAILED_RUN_OBSERVED"
    else:
        report["verification_status"] = "INCOMPLETE"
        report["pending"].append("Sem manifesto aprovado nem auditoria de falha para esta execucao")
        if run["JobRunState"] == "SUCCEEDED":
            report["errors"].append("Glue SUCCEEDED sem manifesto de publicacao")
            report["status"] = "VALIDATION_FAILED"
        elif run["JobRunState"] in TERMINAL_FAILURES:
            report["status"] = "FAILED"
            report["verification_status"] = "FAILED_RUN_WITHOUT_FAILURE_AUDIT"
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", required=True, choices=("silver", "gold"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--verify-parquet", action="store_true")
    parser.add_argument("--max-object-mb", type=int, default=512)
    parser.add_argument("--bucket", default="fiap-tc-fase2-2026")
    parser.add_argument("--region", default=REGION)
    parser.add_argument("--job-name", default="")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", args.run_id) or not 1 <= args.max_object_mb <= 1024:
        parser.error("run-id ou max-object-mb invalido")
    try:
        report = inspect(args)
    except Exception as error:
        report = {"observed_at_utc": utcnow(), "layer": args.layer, "run_id": args.run_id,
                  "status": "OBSERVER_ERROR", "verification_status": "FAILED", "published": False,
                  "errors": [f"{type(error).__name__}: {error}"]}
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=lambda obj: obj.isoformat()) + "\n", encoding="utf-8")
    print(json.dumps({"report": str(output), "status": report["status"],
                      "verification_status": report.get("verification_status"), "published": report["published"],
                      "errors": report["errors"], "counts": {row["table"]: row.get("target_count")
                                                               for row in report.get("tables", [])}}, ensure_ascii=False))
    return report


if __name__ == "__main__":
    main()
