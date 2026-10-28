"""Inspect the published national comparison. Reads AWS S3; writes only local JSON."""
import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
from urllib.parse import urlsplit

DEFAULT_MANIFEST = (
    "s3://fiap-tc-fase2-2026/auditoria/gold/ingestion_date=2026-10-22/"
    "run_id=jr_74dca58f19d0bd28dd27bba71b3c1a0bc420e55ec2dab74fb047e23b1901d4cd/manifest.json"
)
COLUMNS = ["ano", "serie", "rede", "alunos_avaliados", "taxa_alfabetizacao_calculada",
           "taxa_oficial_publicada", "diferenca_pp", "dentro_tolerancia"]


def parse_s3(uri):
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError("Expected an exact s3://bucket/key URI")
    if parsed.query or parsed.fragment or any(p in (".", "..") for p in parsed.path.split("/")):
        raise ValueError("Invalid S3 URI")
    return parsed.netloc, parsed.path.lstrip("/")


def comparison_entry(manifest):
    if manifest.get("status") != "SUCCEEDED" or manifest.get("all_five_tables_validated") is not True:
        raise ValueError("Use a committed, complete Gold SUCCEEDED manifest")
    entries = [x for x in manifest.get("table_metrics", []) if x.get("table") == "validacao_cruzada_brasil"]
    if len(entries) != 1:
        raise ValueError("Expected exactly one national comparison entry")
    entry = entries[0]
    uri = entry.get("s3_uri", "")
    _, key = parse_s3(uri)
    run_id = manifest.get("run_id")
    if not run_id or f"/run_id={run_id}/" not in "/" + key or "/validacao_cruzada_brasil/" not in "/" + key:
        raise ValueError("National comparison path is not part of the selected run")
    if entry.get("status") != "VALIDATED":
        raise ValueError("National comparison entry is not VALIDATED")
    return entry


def inspect_rows(rows, tolerance):
    output = []
    for row in rows:
        item = dict(row)
        calculated, official = row.get("taxa_alfabetizacao_calculada"), row.get("taxa_oficial_publicada")
        if calculated is None or official is None:
            item.update(diferenca_reavaliada_pp=None, situacao="SEM_COMPARACAO")
        else:
            if not all(isinstance(x, (int, float)) and math.isfinite(x) for x in (calculated, official)):
                raise ValueError("Nonfinite or unexpected national rate")
            difference = round(float(calculated) - float(official), 4)
            within = abs(difference) <= tolerance
            item.update(diferenca_reavaliada_pp=difference,
                        situacao="DENTRO_DA_TOLERANCIA" if within else "FORA_DA_TOLERANCIA",
                        flag_gravada_confere=row.get("dentro_tolerancia") == within)
        output.append(item)
    return sorted(output, key=lambda r: (str(r.get("ano")), str(r.get("serie")), str(r.get("rede"))))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-uri", default=DEFAULT_MANIFEST)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--output-dir", default="diagnostico_gold_inicial")
    args = parser.parse_args(argv)
    destination = Path(args.output_dir)
    if destination.exists():
        raise FileExistsError("Choose a new output directory; prior evidence is preserved")
    # Dependencies are imported only for the actual S3 inspection.
    import boto3
    import pyarrow.parquet as pq
    s3 = boto3.client("s3", region_name=args.region)
    bucket, key = parse_s3(args.manifest_uri)
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    manifest = json.loads(raw)
    entry = comparison_entry(manifest)
    bucket, prefix = parse_s3(entry["s3_uri"])
    objects = sorted((o for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                      for o in p.get("Contents", []) if o["Key"].endswith(".parquet")), key=lambda o: o["Key"])
    if not objects or len(objects) > 64 or sum(o["Size"] for o in objects) > 32 * 1024 * 1024:
        raise ValueError("Unexpected national comparison inventory; inspect before downloading")
    rows, inventory = [], []
    for obj in objects:
        body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
        parquet = pq.ParquetFile(io.BytesIO(body))
        missing = set(COLUMNS) - set(parquet.schema_arrow.names)
        if missing:
            raise ValueError("Missing columns: " + ", ".join(sorted(missing)))
        rows.extend(parquet.read(columns=COLUMNS).to_pylist())
        inventory.append({"uri": "s3://" + bucket + "/" + obj["Key"], "bytes": len(body),
                          "rows": parquet.metadata.num_rows, "sha256": hashlib.sha256(body).hexdigest()})
    expected = entry.get("s3_count")
    if expected is None or len(rows) != expected:
        raise ValueError("Physical row count differs from the committed manifest")
    tolerance = 0.1  # Existing Gold comparison policy, in percentage points.
    inspected = inspect_rows(rows, tolerance)
    report = {"observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "scope": "Read-only S3 national comparison inspection; not a new Glue run or a root-cause conclusion",
              "source_manifest_uri": args.manifest_uri,
              "source_manifest_sha256": hashlib.sha256(raw).hexdigest(),
              "gold_run_id": manifest["run_id"], "tolerance_pp": tolerance,
              "rows": inspected, "object_inventory": inventory,
              "outside_tolerance": sum(r["situacao"] == "FORA_DA_TOLERANCIA" for r in inspected),
              "note": "A difference identifies what to investigate; it does not establish which source or formula is wrong."}
    destination.mkdir(parents=True, exist_ok=False)
    target = destination / "diagnostico_gold.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print("ANO | SERIE | REDE | OFICIAL | RECALCULADA | DIFERENCA_PP | SITUACAO")
    for r in inspected:
        print(" | ".join(str(r.get(k)) for k in ["ano", "serie", "rede", "taxa_oficial_publicada",
              "taxa_alfabetizacao_calculada", "diferenca_reavaliada_pp", "situacao"]))
    print("Relatorio local:", target.resolve())
    print("Nenhum objeto AWS foi alterado e nenhum job Glue foi iniciado.")
    return report


if __name__ == "__main__":
    main()
