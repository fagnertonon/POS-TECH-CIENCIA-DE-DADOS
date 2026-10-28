"""Verifica uma execução Bronze existente sem alterar recursos da AWS.

Exemplo: python verify_bronze.py --run-id jr_... --date 2026-10-20
Use --allow-subset somente para um piloto e --csv contagens.csv se desejado.
"""

import argparse
import csv
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re


TABLES = {
    "alunos", "municipio", "uf", "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "dicionario",
}
METRIC_FIELDS = (
    "table", "source_table", "target_uri", "run_id", "status",
    "started_at_utc", "source_read_finished_at_utc", "write_finished_at_utc",
    "finished_at_utc", "duration_seconds", "source_count", "target_count",
    "counts_match", "source_column_count", "target_column_count",
    "parquet_file_count", "parquet_bytes", "object_count", "object_bytes",
)


def iso(value):
    return value.isoformat() if isinstance(value, (datetime, date)) else value


def inventory(s3, bucket, prefix):
    result = {"parquet_file_count": 0, "parquet_bytes": 0, "object_count": 0, "object_bytes": 0}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            result["object_count"] += 1
            result["object_bytes"] += obj["Size"]
            if obj["Key"].endswith(".parquet"):
                result["parquet_file_count"] += 1
                result["parquet_bytes"] += obj["Size"]
    return result


def verify(args, glue, s3):
    errors = []
    report = {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "region": args.region, "run_id": args.run_id, "ingestion_date": args.date,
        "allow_subset": args.allow_subset, "tables": [], "errors": errors,
    }
    run = glue.get_job_run(JobName=args.job_name, RunId=args.run_id, PredecessorsIncluded=False)["JobRun"]
    report["glue_job_run"] = {
        name: iso(run[name]) for name in (
            "Id", "JobName", "JobRunState", "StartedOn", "CompletedOn", "LastModifiedOn",
            "ExecutionTime", "WorkerType", "NumberOfWorkers", "DPUSeconds", "MaxCapacity",
            "GlueVersion", "ExecutionClass", "Attempt", "Timeout",
        ) if name in run
    }
    report["script_uri"] = glue.get_job(JobName=args.job_name)["Job"].get("Command", {}).get("ScriptLocation")
    if run.get("JobRunState") != "SUCCEEDED":
        errors.append("Glue JobRunState não é SUCCEEDED")
    manifest_key = f"{args.audit_prefix}/ingestion_date={args.date}/run_id={args.run_id}/manifest.json"
    report["manifest_uri"] = f"s3://{args.bucket}/{manifest_key}"
    try:
        body = s3.get_object(Bucket=args.bucket, Key=manifest_key)["Body"].read()
        manifest = json.loads(body)
    except Exception as exc:
        errors.append(f"Manifesto indisponível ou inválido: {type(exc).__name__}")
        report["validation_status"] = "FAILED"
        return report
    report["manifest"] = {k: v for k, v in manifest.items() if k != "table_metrics"}
    for key, expected in (("status", "SUCCEEDED"), ("run_id", args.run_id), ("ingestion_date", args.date), ("job_name", args.job_name)):
        if manifest.get(key) != expected:
            errors.append(f"Manifesto: {key} diverge do esperado")
    items = manifest.get("table_metrics", [])
    if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
        errors.append("table_metrics inválido")
        items = []
    names = [item.get("table") for item in items]
    if any(not isinstance(name, str) for name in names):
        errors.append("Nome de tabela inválido")
        names = [name for name in names if isinstance(name, str)]
    known_unique = bool(names) and len(names) == len(set(names)) and set(names).issubset(TABLES)
    if not known_unique:
        errors.append("Tabelas ausentes, desconhecidas ou repetidas")
    full_batch = set(names) == TABLES and len(names) == 7
    if not args.allow_subset and (manifest.get("is_full_batch") is not True or not full_batch):
        errors.append("Rodada deve conter exatamente as sete tabelas e is_full_batch=true")
    if manifest.get("is_full_batch") is not full_batch:
        errors.append("is_full_batch inconsistente com as tabelas presentes")
    requested = manifest.get("requested_tables")
    if not isinstance(requested, list) or sorted(requested) != sorted(names):
        errors.append("requested_tables diverge das métricas publicadas")
    if manifest.get("validated_table_count") != len(items) or manifest.get("all_counts_match") is not True:
        errors.append("Resumo de validação do manifesto inconsistente")
    for item in items:
        summary = {key: item[key] for key in METRIC_FIELDS if key in item}
        table = item.get("table")
        report["tables"].append(summary)
        if table not in TABLES:
            continue
        expected_prefix = f"{args.prefix}/{table}/ingestion_date={args.date}/run_id={args.run_id}/"
        expected_uri = f"s3://{args.bucket}/{expected_prefix}"
        if item.get("target_uri") != expected_uri or item.get("run_id") != args.run_id:
            errors.append(f"{table}: caminho/run_id inconsistente")
            continue
        source_count, target_count = item.get("source_count"), item.get("target_count")
        if not (type(source_count) is int and type(target_count) is int and source_count > 0 and source_count == target_count):
            errors.append(f"{table}: contagens inválidas, zero ou divergentes")
        if item.get("status") != "SUCCEEDED" or item.get("counts_match") is not True:
            errors.append(f"{table}: status de validação não é sucesso")
        observed = inventory(s3, args.bucket, expected_prefix)
        summary["verified_s3_inventory"] = observed
        for field in ("parquet_file_count", "parquet_bytes", "object_count", "object_bytes"):
            if observed[field] != item.get(field):
                errors.append(f"{table}: inventário S3 diverge em {field}")
        if observed["parquet_file_count"] <= 0 or observed["parquet_bytes"] <= 0:
            errors.append(f"{table}: inventário sem arquivos Parquet válidos")
    if all(type(item.get("source_count")) is int and type(item.get("target_count")) is int for item in items):
        for field, count_field in (("total_source_rows", "source_count"), ("total_target_rows", "target_count")):
            if manifest.get(field) != sum(item[count_field] for item in items):
                errors.append(f"{field} inconsistente")
    report["validation_status"] = "FAILED" if errors else "SUCCEEDED"
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--date", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--job-name", default="fiap-bronze-inep-batch")
    parser.add_argument("--bucket", default="fiap-tc-fase2-2026")
    parser.add_argument("--prefix", default="bronze/inep")
    parser.add_argument("--audit-prefix", default="auditoria/bronze")
    parser.add_argument("--output", default="bronze_report.json")
    parser.add_argument("--csv", help="Caminho opcional do CSV com contagens")
    parser.add_argument("--allow-subset", action="store_true")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", args.run_id):
        parser.error("run-id inválido")
    try:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.date):
            raise ValueError()
        date.fromisoformat(args.date)
    except ValueError:
        parser.error("date deve ser uma data válida YYYY-MM-DD")
    args.prefix = args.prefix.strip("/")
    args.audit_prefix = args.audit_prefix.strip("/")
    return args


def main(argv=None):
    import boto3

    args = parse_args(argv)
    try:
        report = verify(args, boto3.client("glue", region_name=args.region), boto3.client("s3", region_name=args.region))
    except Exception as exc:
        report = {"run_id": args.run_id, "validation_status": "FAILED", "errors": [f"Falha na verificação: {type(exc).__name__}"]}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.csv:
        with Path(args.csv).open("w", encoding="utf-8", newline="") as stream:
            fields = ["table", "source_count", "target_count", "parquet_file_count", "parquet_bytes", "duration_seconds", "target_uri", "status"]
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(report.get("tables", []))
    print(json.dumps({"validation_status": report["validation_status"], "output": args.output, "errors": report["errors"]}, ensure_ascii=False))
    return 0 if report["validation_status"] == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
