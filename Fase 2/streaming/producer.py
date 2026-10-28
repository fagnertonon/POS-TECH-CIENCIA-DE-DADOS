"""Replay controlado de alunos reais da Bronze para SQS; nunca consulta credenciais.

Python 3.11+. Instale requirements-producer.txt em um ambiente virtual.
Os eventos são uma simulação de chegada incremental, não atualizações ao vivo do INEP.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import urlparse
import uuid


DEFAULT_MANIFEST = (
    "s3://fiap-tc-fase2-2026/auditoria/bronze/ingestion_date=2026-10-20/"
    "run_id=jr_cdfd2933220ec75b45c04518c6e5afe93eb98915f3b88328f8bde8d7183d3700/manifest.json"
)
ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,160}")
TECHNICAL_COLUMNS = {"id_ingestao", "data_ingestao", "tabela_origem"}


def utc_now():
    return datetime.now(timezone.utc)


def iso_utc(value):
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def strict_json(value):
    """Serialização determinística; não converte NaN ou tipos da fonte silenciosamente."""
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("Evento contém valor incompatível com JSON estrito; payload não foi transformado") from exc


def split_s3_uri(uri):
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/") or parsed.query or parsed.fragment:
        raise ValueError("URI deve ter formato s3://bucket/chave")
    return parsed.netloc, parsed.path.lstrip("/")


def source_spec(manifest):
    if not (
        manifest.get("status") == "SUCCEEDED"
        and manifest.get("is_full_batch") is True
        and manifest.get("all_counts_match") is True
    ):
        raise ValueError("Manifesto não representa um batch completo validado")
    source_run_id = manifest.get("run_id")
    if not isinstance(source_run_id, str) or not ID_PATTERN.fullmatch(source_run_id):
        raise ValueError("run_id da fonte inválido")
    matches = [item for item in manifest.get("table_metrics", []) if item.get("table") == "alunos"]
    if len(matches) != 1:
        raise ValueError("Manifesto deve conter exatamente uma métrica alunos")
    item = matches[0]
    if item.get("status") != "SUCCEEDED" or item.get("counts_match") is not True:
        raise ValueError("Tabela alunos não foi validada")
    if type(item.get("source_count")) is not int or item["source_count"] <= 0 or item["source_count"] != item.get("target_count"):
        raise ValueError("Contagens alunos inválidas")
    names = [field["name"] for field in item.get("source_schema", {}).get("fields", [])]
    if not names or len(names) != len(set(names)) or TECHNICAL_COLUMNS.intersection(names):
        raise ValueError("Schema original de alunos ausente, repetido ou contaminado por metadados")
    bucket, prefix = split_s3_uri(item["target_uri"])
    if not prefix.endswith(f"/run_id={source_run_id}/"):
        raise ValueError("Caminho alunos diverge do run_id validado")
    return source_run_id, bucket, prefix, names


def load_sample(s3, manifest_uri, count):
    """Baixa somente os shards necessários; lê só as colunas originais do manifesto."""
    import pyarrow.parquet as pq

    manifest_bucket, manifest_key = split_s3_uri(manifest_uri)
    manifest = json.loads(s3.get_object(Bucket=manifest_bucket, Key=manifest_key)["Body"].read())
    source_run_id, bucket, prefix, names = source_spec(manifest)
    keys = sorted(
        obj["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    )
    if not keys:
        raise ValueError("Snapshot validado sem arquivos Parquet")
    rows = []
    with tempfile.TemporaryDirectory(prefix="fiap-streaming-") as temporary:
        for shard_index, key in enumerate(keys):
            local = Path(temporary) / f"part-{shard_index}.parquet"
            s3.download_file(bucket, key, str(local))
            parquet = pq.ParquetFile(local)
            if not set(names).issubset(parquet.schema_arrow.names):
                raise ValueError("Shard não contém as colunas originais do manifesto")
            for batch in parquet.iter_batches(batch_size=min(count, 1000), columns=names):
                for row_index, row in enumerate(batch.to_pylist()):
                    # A seleção de colunas foi feita pelo leitor; os valores permanecem intactos.
                    strict_json(row)
                    rows.append({"payload": row, "source_object_uri": f"s3://{bucket}/{key}", "sample_index": len(rows)})
                    if len(rows) == count:
                        return rows, {"manifest_uri": manifest_uri, "snapshot_run_id": source_run_id,
                                      "source_table": "alunos", "source_columns": names,
                                      "source_target_uri": f"s3://{bucket}/{prefix}"}
    raise ValueError(f"Fonte contém menos de {count} registros disponíveis; nada foi enviado")


def make_event(sample, run_id, snapshot_run_id, emitted_at=None):
    identity = f"{run_id}|{snapshot_run_id}|alunos|{sample['source_object_uri']}|{sample['sample_index']}"
    event = {
        "schema_version": 1,
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, identity)),
        "run_id": run_id,
        "entity": "alunos",
        "event_time": iso_utc(emitted_at or utc_now()),
        "source": "inep_bronze_replay",
        "source_snapshot_run_id": snapshot_run_id,
        "simulation": True,
        "payload": sample["payload"],
    }
    strict_json(event)
    return event


def read_replay(path):
    """Preserva os bytes JSON originais de cada linha, sem recriar IDs ou event_time."""
    bodies = []
    run_ids = set()
    event_ids = set()
    for number, raw in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            raise ValueError(f"Linha vazia no replay: {number}")
        event = json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError("JSON não finito")))
        for name in ("run_id", "event_id"):
            if not isinstance(event.get(name), str) or not ID_PATTERN.fullmatch(event[name]):
                raise ValueError(f"Identificador inválido no replay, linha {number}")
        if event.get("schema_version") != 1 or event.get("entity") != "alunos" or event.get("simulation") is not True:
            raise ValueError(f"Contrato de replay inválido, linha {number}")
        if event["event_id"] in event_ids:
            raise ValueError("events.jsonl deve conter somente os eventos únicos; duplicatas ficam no relatório")
        event_ids.add(event["event_id"])
        run_ids.add(event["run_id"])
        bodies.append(raw)
    if not bodies or len(run_ids) != 1:
        raise ValueError("Replay deve conter eventos de exatamente uma rodada")
    return bodies, next(iter(run_ids))


def send_body(sqs, queue_url, body, kind, order):
    before = utc_now()
    response = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
    message_id = response.get("MessageId")
    if not message_id or response.get("ResponseMetadata", {}).get("HTTPStatusCode", 200) != 200:
        raise RuntimeError("SQS não confirmou o envio")
    event = json.loads(body)
    return {"order": order, "kind": kind, "event_id": event["event_id"], "run_id": event["run_id"],
            "sqs_message_id": message_id, "sent_at_utc": iso_utc(before),
            "confirmed_at_utc": iso_utc(utc_now()), "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest()}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-uri", default=DEFAULT_MANIFEST)
    parser.add_argument("--queue-url", required=True)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--events", type=int, default=30, help="Quantidade de eventos novos; replay-file usa todas as linhas")
    parser.add_argument("--interval-seconds", type=float, default=None, help="Padrão 1 para novos eventos, 0 para replay")
    parser.add_argument("--duplicates", type=int, default=None, help="Padrão 5 para novos eventos, 0 para replay")
    parser.add_argument("--run-id")
    parser.add_argument("--output-dir", default="./streaming-evidence")
    parser.add_argument("--replay-file", help="Reenvia os envelopes exatos de events.jsonl; não altera event_time")
    args = parser.parse_args(argv)
    if args.replay_file and args.run_id:
        parser.error("--replay-file preserva seu run_id; não use --run-id")
    args.interval_seconds = args.interval_seconds if args.interval_seconds is not None else (0.0 if args.replay_file else 1.0)
    args.duplicates = args.duplicates if args.duplicates is not None else (0 if args.replay_file else 5)
    if not 1 <= args.events <= 10000 or not 0 <= args.duplicates <= 10000 or not 0 <= args.interval_seconds <= 60:
        parser.error("events: 1..10000; duplicates: 0..10000; interval-seconds: 0..60")
    if args.run_id and not ID_PATTERN.fullmatch(args.run_id):
        parser.error("run-id inválido")
    queue = urlparse(args.queue_url)
    if queue.scheme != "https" or queue.hostname != f"sqs.{args.region}.amazonaws.com" or len(queue.path.strip("/").split("/")) != 2:
        parser.error("queue-url deve identificar uma fila SQS regional HTTPS")
    return args


def main(argv=None):
    import boto3

    args = parse_args(argv)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    events_path, report_path = out / "events.jsonl", out / "producer_report.json"
    if events_path.exists() or report_path.exists():
        raise FileExistsError("Diretório já contém evidências; use outro output-dir para preservar o histórico")
    started = utc_now()
    report = {"status": "RUNNING", "simulation": True, "started_at_utc": iso_utc(started),
              "region": args.region, "queue_url": args.queue_url, "send_results": []}
    bodies = []
    try:
        sqs = boto3.client("sqs", region_name=args.region)
        if args.replay_file:
            bodies, run_id = read_replay(args.replay_file)
            report["replay_file"] = str(Path(args.replay_file))
            samples = None
        else:
            samples, provenance = load_sample(boto3.client("s3", region_name=args.region), args.manifest_uri, args.events)
            run_id = args.run_id or f"sim_{started.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:12]}"
            report["source"] = provenance
        report["run_id"] = run_id
        report["unique_event_count"] = len(bodies) if samples is None else len(samples)
        report["duplicate_send_count"] = args.duplicates
        report["interval_seconds"] = args.interval_seconds
        with events_path.open("x", encoding="utf-8", newline="\n") as events_stream:
            for index in range(report["unique_event_count"]):
                if index and args.interval_seconds:
                    time.sleep(args.interval_seconds)
                if samples is not None:
                    bodies.append(strict_json(make_event(samples[index], run_id, report["source"]["snapshot_run_id"])))
                body = bodies[index]
                # Persistir antes de enviar permite recuperar a identidade em uma falha de rede ambígua.
                events_stream.write(body + "\n")
                events_stream.flush()
                report["send_results"].append(send_body(sqs, args.queue_url, body, "REPLAY" if samples is None else "NEW", index + 1))
            for index in range(args.duplicates):
                if args.interval_seconds:
                    time.sleep(args.interval_seconds)
                body = bodies[index % len(bodies)]
                report["send_results"].append(send_body(sqs, args.queue_url, body, "DUPLICATE", report["unique_event_count"] + index + 1))
        report["status"] = "SUCCEEDED"
    except Exception as exc:
        report["status"] = "FAILED"
        report["error_type"] = type(exc).__name__
        raise
    finally:
        report["finished_at_utc"] = iso_utc(utc_now())
        report["confirmed_send_count"] = len(report["send_results"])
        report["duration_seconds"] = round((utc_now() - started).total_seconds(), 3)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "run_id": report["run_id"],
                      "unique_events": report["unique_event_count"], "confirmed_sends": report["confirmed_send_count"],
                      "events_file": str(events_path), "report_file": str(report_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
