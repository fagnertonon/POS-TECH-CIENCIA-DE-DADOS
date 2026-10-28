"""Compact a validated replay into immutable, explicitly simulated Parquet datasets.

This is a bounded, on-demand operation, not another continuously running service.
Only the exact identities in events.jsonl are read. The final manifest is published
after all checks and both conditional Parquet writes succeed. Readers must choose
one manifest/snapshot: historical snapshots must never be unioned as new arrivals.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import re
import time
import uuid


PIPELINE_VERSION = "2.0.0"
ORIGIN = "simulacao_replay_inep"
BUCKET = "fiap-tc-fase2-2026"
SAFE_ID = re.compile(r"[A-Za-z0-9_-]{1,160}\Z")
BRONZE_PREFIX = "bronze/inep_streaming"
SILVER_PREFIX = "silver/inep_streaming"
GOLD_PREFIX = "gold/inep_streaming"
PROCESSED_PREFIX = "auditoria/streaming/processed"
MANIFEST_PREFIX = "auditoria/streaming/analitico/runs"
MAX_EVENTS = 10000  # Deliberately bounded: this command is a small replay finalizer.
FINALIZER_VERSION = "2.0.1"
EXECUTION_PREFIX = "auditoria/streaming/analitico/execucoes"


class FinalizationError(ValueError):
    """An incomplete, inconsistent or conflicting run must not be published."""


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _reject_constant(value):
    raise FinalizationError("Non-finite JSON number: " + value)


def _json(body):
    try:
        return json.loads(body, parse_constant=_reject_constant)
    except (json.JSONDecodeError, UnicodeError, TypeError) as exc:
        raise FinalizationError("Invalid JSON object") from exc


def _timestamp(value):
    if not isinstance(value, str) or not value.endswith(("Z", "+00:00")):
        raise FinalizationError("Expected an ISO UTC timestamp")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FinalizationError("Invalid ISO UTC timestamp") from exc
    if stamp.utcoffset() != timezone.utc.utcoffset(stamp):
        raise FinalizationError("Expected UTC")
    return stamp


def validate_events(events):
    if not isinstance(events, list) or not events or len(events) > MAX_EVENTS:
        raise FinalizationError(f"Expected between 1 and {MAX_EVENTS} unique events")
    run_ids, snapshot_ids, identities = set(), set(), set()
    for event in events:
        if not isinstance(event, dict):
            raise FinalizationError("Event must be an object")
        if (type(event.get("schema_version")) is not int or event["schema_version"] != 1
                or event.get("entity") != "alunos" or event.get("source") != "inep_bronze_replay"
                or event.get("simulation") is not True or not isinstance(event.get("payload"), dict)):
            raise FinalizationError("Expected the simulated alunos replay contract")
        for field in ("run_id", "event_id", "source_snapshot_run_id"):
            if not isinstance(event.get(field), str) or not SAFE_ID.fullmatch(event[field]):
                raise FinalizationError("Invalid " + field)
        _timestamp(event.get("event_time"))
        canonical_json(event)
        identity = (event["run_id"], event["event_id"])
        if identity in identities:
            raise FinalizationError("events.jsonl must contain unique identities only")
        identities.add(identity)
        run_ids.add(event["run_id"])
        snapshot_ids.add(event["source_snapshot_run_id"])
    if len(run_ids) != 1 or len(snapshot_ids) != 1:
        raise FinalizationError("One replay run and one source snapshot are required")
    return sorted(events, key=lambda event: event["event_id"])


def read_events(path):
    lines = Path(path).read_text(encoding="utf-8-sig").splitlines()
    if any(not line.strip() for line in lines):
        raise FinalizationError("Empty lines are not allowed in events.jsonl")
    return validate_events([_json(line) for line in lines])


def event_keys(event):
    date = _timestamp(event["event_time"]).date().isoformat()
    run_id, event_id = event["run_id"], event["event_id"]
    suffix = f"event_date={date}/run_id={run_id}/event_id={event_id}"
    return {
        "bronze_key": f"{BRONZE_PREFIX}/entity=alunos/{suffix}.json",
        "silver_key": f"{SILVER_PREFIX}/entity=alunos/{suffix}.json.gz",
        "gold_key": f"{GOLD_PREFIX}/contribuicoes/{suffix}.json.gz",
        "receipt_key": f"{PROCESSED_PREFIX}/run_id={run_id}/event_id={event_id}.json",
    }


def _get_bytes(s3, bucket, key):
    try:
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as exc:
        raise FinalizationError("Required streaming object is unavailable: " + key) from exc


def _get_json(s3, bucket, key):
    body = _get_bytes(s3, bucket, key)
    try:
        value = _json(gzip.decompress(body) if key.endswith(".gz") else body)
    except (OSError, EOFError) as exc:
        raise FinalizationError("Invalid compressed streaming object: " + key) from exc
    if not isinstance(value, dict):
        raise FinalizationError("Stored streaming object must be an object")
    return value, len(body)


def _assert_fields(value, expected, label):
    for name, correct in expected.items():
        actual = value.get(name)
        if type(actual) is not type(correct) or actual != correct:
            raise FinalizationError(f"{label}: field {name} differs")


def _validate_projection(stored, event, keys, layer):
    _assert_fields(stored, {
        "schema_version": 2, "pipeline_version": PIPELINE_VERSION, "origin": ORIGIN,
        "simulation": True, "layer": layer, "event_sha256": digest(event),
    }, layer)
    expected_provenance = {name: event[name] for name in (
        "run_id", "event_id", "source_snapshot_run_id", "event_time")}
    expected_provenance["bronze_key"] = keys["bronze_key"]
    if not isinstance(stored.get("provenance"), dict):
        raise FinalizationError(layer + ": provenance is required")
    _assert_fields(stored["provenance"], expected_provenance, layer + " provenance")
    data = stored.get("data")
    if not isinstance(data, dict) or stored.get("data_sha256") != digest(data):
        raise FinalizationError(layer + ": data hash differs")
    return data


def _number(value, name):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise FinalizationError("Expected finite numeric " + name)
    return value


def _validate_gold(silver, gold, event):
    # Recompute the reviewed transformation from the raw event as well as
    # reconciling the additive components below. Self-consistent hashes alone
    # must not authorize a changed UF, flag or unrecognized source category.
    import lambda_function as consumer
    try:
        expected_silver, expected_gold = consumer.transform_payload(event["payload"])
    except ValueError as exc:
        raise FinalizationError("Raw event fails the reviewed analytical contract") from exc
    if digest(silver) != digest(expected_silver) or digest(gold) != digest(expected_gold):
        raise FinalizationError("Stored projections differ from the reviewed raw-event transformation")
    for name in ("ano", "sigla_uf", "serie", "rede", "avaliado", "alfabetizado_avaliado", "peso_aluno", "id_aluno"):
        if name not in silver:
            raise FinalizationError("Silver is missing " + name)
    if type(silver["ano"]) is not int or type(silver["avaliado"]) is not bool:
        raise FinalizationError("Invalid Silver year/evaluation types")
    for name in ("sigla_uf", "serie", "rede", "id_aluno"):
        if not isinstance(silver[name], str) or not silver[name]:
            raise FinalizationError("Invalid Silver " + name)
    if silver["ano"] != event["payload"].get("ano"):
        raise FinalizationError("Silver year differs from raw event")
    for name in ("serie", "rede", "id_aluno"):
        if silver[name] != str(event["payload"].get(name, "")).strip():
            raise FinalizationError("Silver source key differs: " + name)
    evaluated = silver["avaliado"]
    alphabetized = silver["alfabetizado_avaliado"]
    if evaluated and type(alphabetized) is not bool:
        raise FinalizationError("Evaluated pupil must have known alphabetization")
    if not evaluated and alphabetized is not None:
        raise FinalizationError("Unevaluated pupil must not receive an alphabetization result")
    weight = _number(silver["peso_aluno"], "peso_aluno") if evaluated else 0.0
    if evaluated and weight <= 0:
        raise FinalizationError("Evaluated pupil needs positive weight")
    expected = {name: silver[name] for name in ("ano", "sigla_uf", "serie", "rede")}
    expected.update(eventos=1, alunos_avaliados=int(evaluated),
                    alunos_alfabetizados=int(evaluated and alphabetized),
                    soma_peso_avaliados=float(weight),
                    soma_peso_alfabetizados=float(weight if evaluated and alphabetized else 0.0))
    if set(gold) != set(expected):
        raise FinalizationError("Gold contribution schema differs")
    for name, correct in expected.items():
        if isinstance(correct, float):
            if not math.isclose(_number(gold.get(name), name), correct, rel_tol=1e-12, abs_tol=1e-12):
                raise FinalizationError("Gold contribution differs: " + name)
        elif type(gold.get(name)) is not type(correct) or gold[name] != correct:
            raise FinalizationError("Gold contribution differs: " + name)


def _validate_receipt(receipt, event, keys, silver, gold):
    expected = {name: event[name] for name in ("run_id", "event_id", "source_snapshot_run_id", "event_time")}
    expected.update(schema_version=2, pipeline_version=PIPELINE_VERSION, status="GOLD_READY",
                    origin=ORIGIN, simulation=True, event_sha256=digest(event),
                    silver_data_sha256=digest(silver), gold_data_sha256=digest(gold))
    expected.update({name: keys[name] for name in ("bronze_key", "silver_key", "gold_key")})
    _assert_fields(receipt, expected, "receipt")
    latency = _number(receipt.get("end_to_end_latency_seconds"), "end_to_end_latency_seconds")
    measured = max(0.0, (_timestamp(receipt.get("completed_at_utc")) - _timestamp(event["event_time"])).total_seconds())
    if latency < 0 or not math.isclose(latency, measured, rel_tol=0, abs_tol=0.001):
        raise FinalizationError("Receipt completion clock and latency differ")


def read_run(s3, events, bucket=BUCKET):
    """Validate committed identities and reconcile all three layers before any write."""
    events = validate_events(events)
    silver_rows, gold_contributions, identities, receipts = [], [], [], []
    byte_counts = defaultdict(int)
    business_keys = set()
    for event in events:
        keys = event_keys(event)
        objects = {}
        for kind, key in keys.items():
            objects[kind], size = _get_json(s3, bucket, key)
            byte_counts[kind] += size
        bronze = objects["bronze_key"]
        if digest(bronze.get("event")) != digest(event) or not isinstance(bronze.get("ingestion"), dict):
            raise FinalizationError("Bronze envelope differs from producer event")
        _assert_fields(bronze["ingestion"], {"event_sha256": digest(event),
                       "payload_sha256": digest(event["payload"]), "target_key": keys["bronze_key"]}, "Bronze")
        silver = _validate_projection(objects["silver_key"], event, keys, "silver")
        gold = _validate_projection(objects["gold_key"], event, keys, "gold_contribution")
        _validate_gold(silver, gold, event)
        _validate_receipt(objects["receipt_key"], event, keys, silver, gold)
        # The simulator represents append-only pupil arrivals, not CDC updates.
        business_key = (silver["ano"], silver["id_aluno"])
        if business_key in business_keys:
            raise FinalizationError("Duplicate pupil/year under distinct event identities")
        business_keys.add(business_key)
        metadata = {"run_id": event["run_id"], "event_id": event["event_id"],
                    "source_snapshot_run_id": event["source_snapshot_run_id"],
                    "event_time": event["event_time"], "event_sha256": digest(event),
                    "origin": ORIGIN, "simulation": True, "pipeline_version": PIPELINE_VERSION}
        if set(silver).intersection(metadata):
            raise FinalizationError("Silver data conflicts with provenance columns")
        compacted = dict(silver, **metadata)
        # The source contract permits JSON integer weights/scores; Parquet
        # deliberately gives these columns a stable nullable float64 type.
        # Hash the materialized logical schema, not its pre-cast JSON variant.
        for name in ("peso_aluno", "proficiencia"):
            if name in compacted and compacted[name] is not None:
                compacted[name] = float(_number(compacted[name], name))
        silver_rows.append(compacted)
        gold_contributions.append(gold)
        identities.append({"event_id": event["event_id"], "event_sha256": digest(event),
                           "silver_data_sha256": digest(silver), "gold_data_sha256": digest(gold),
                           **keys})
        receipts.append(objects["receipt_key"])
    gold_rows = aggregate_gold(gold_contributions, events[0])
    snapshot_spec = {"schema_version": 2, "pipeline_version": PIPELINE_VERSION, "origin": ORIGIN,
                     "run_id": events[0]["run_id"], "source_snapshot_run_id": events[0]["source_snapshot_run_id"],
                     "identities": identities}
    return {"events": events, "silver_rows": silver_rows, "gold_rows": gold_rows,
            "identities": identities, "receipts": receipts, "input_bytes": dict(byte_counts),
            "snapshot_id": digest(snapshot_spec), "dataset_sha256": {
                "silver": digest(silver_rows), "gold": digest(gold_rows)}}


def aggregate_gold(contributions, event):
    groups = defaultdict(list)
    for item in contributions:
        groups[tuple(item[name] for name in ("ano", "sigla_uf", "serie", "rede"))].append(item)
    rows = []
    for group, values in sorted(groups.items()):
        row = dict(zip(("ano", "sigla_uf", "serie", "rede"), group))
        for name in ("eventos", "alunos_avaliados", "alunos_alfabetizados"):
            row[name] = sum(item[name] for item in values)
        for name in ("soma_peso_avaliados", "soma_peso_alfabetizados"):
            row[name] = math.fsum(item[name] for item in values)
        denominator = row["soma_peso_avaliados"]
        row["taxa_alfabetizacao_simulada"] = (round(100.0 * row["soma_peso_alfabetizados"] / denominator, 4)
                                                 if denominator else None)
        row.update(run_id=event["run_id"], source_snapshot_run_id=event["source_snapshot_run_id"],
                   origin=ORIGIN, simulation=True, pipeline_version=PIPELINE_VERSION)
        rows.append(row)
    return rows


def parquet_bytes(rows, layer):
    """Import PyArrow only for materialization; core validation remains stdlib-only."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    names = sorted(set().union(*(row.keys() for row in rows)))
    string_names = {"id_aluno", "id_escola", "id_municipio", "serie", "rede", "presenca",
                    "preenchimento_caderno", "alfabetizado", "caderno", "sigla_uf", "run_id",
                    "event_id", "event_time", "event_sha256", "source_snapshot_run_id", "origin", "pipeline_version"}
    bool_names = {"presente", "caderno_preenchido", "avaliado", "alfabetizado_avaliado", "simulation"}
    int_names = {"ano", "eventos", "alunos_avaliados", "alunos_alfabetizados"}
    float_names = {"peso_aluno", "proficiencia", "soma_peso_avaliados", "soma_peso_alfabetizados", "taxa_alfabetizacao_simulada"}
    types = {**dict.fromkeys(string_names, pa.string()), **dict.fromkeys(bool_names, pa.bool_()),
             **dict.fromkeys(int_names, pa.int64()), **dict.fromkeys(float_names, pa.float64())}
    unknown = set(names) - set(types)
    if unknown:
        raise FinalizationError("Unrecognized Parquet columns: " + ",".join(sorted(unknown)))
    schema = pa.schema([pa.field(name, types[name]) for name in names], metadata={
        b"origin": ORIGIN.encode(), b"simulation": b"true", b"pipeline_version": PIPELINE_VERSION.encode(),
        b"layer": layer.encode(), b"compression": b"snappy"})
    table = pa.Table.from_pylist(rows, schema=schema)
    output = io.BytesIO()
    pq.write_table(table, output, compression="snappy", version="2.6", use_dictionary=True,
                   write_statistics=True, row_group_size=10000)
    return output.getvalue()


def _put_immutable(s3, bucket, key, body, content_type, data_hash):
    metadata = {"dataset-sha256": data_hash, "content-sha256": hashlib.sha256(body).hexdigest(),
                "pipeline-version": PIPELINE_VERSION, "origin": ORIGIN}
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type,
                      Metadata=metadata, IfNoneMatch="*")
        return "CREATED"
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"PreconditionFailed", "ConditionalRequestConflict", "412", "409"}:
            raise
        # A collision is idempotent only if the existing bytes are identical.
        # Otherwise fail closed; an immutable snapshot is never overwritten.
        if _get_bytes(s3, bucket, key) != body:
            raise FinalizationError("Existing immutable snapshot differs: " + key) from exc
        return "EXISTING"


def _measured(phases, name, operation):
    """Use independent UTC timestamps and a monotonic duration, never event clocks."""
    if phases is None:
        return operation()
    entry = {"stage": name, "started_at_utc": datetime.now(timezone.utc).isoformat()}
    started = time.perf_counter()
    try:
        result = operation()
    except Exception as exc:
        entry.update(status="FAILED", error_type=type(exc).__name__)
        raise
    else:
        entry["status"] = "SUCCEEDED"
        return result
    finally:
        entry.update(finished_at_utc=datetime.now(timezone.utc).isoformat(),
                     duration_seconds=time.perf_counter() - started)
        phases.append(entry)


def finalize_simulation(s3, events, *, bucket=BUCKET, output_dir=None, encoder=parquet_bytes, _phases=None):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise FinalizationError("Invalid S3 bucket")
    if output_dir is not None and Path(output_dir).exists():
        raise FinalizationError("Output evidence directory already exists")
    prepared = _measured(_phases, "validate_and_read", lambda: read_run(s3, events, bucket))
    first = prepared["events"][0]
    run_id, snapshot_id = first["run_id"], prepared["snapshot_id"]
    suffix = f"run_id={run_id}/snapshot_id={snapshot_id}"
    targets = {"silver": f"{SILVER_PREFIX}/compactado/{suffix}/part-00000.parquet",
               "gold": f"{GOLD_PREFIX}/indicador_uf/{suffix}/part-00000.parquet"}
    manifest_key = f"{MANIFEST_PREFIX}/{suffix}/manifest.json"
    # Encode both before the first write: a local dependency/schema failure
    # cannot leave a half-materialized output that resembles a successful run.
    bodies = _measured(_phases, "encode_parquet", lambda: {
        layer: encoder(prepared[layer + "_rows"], layer) for layer in ("silver", "gold")})
    datasets = {}
    for layer in ("silver", "gold"):
        body = bodies[layer]
        _measured(_phases, "publish_" + layer, lambda: _put_immutable(
            s3, bucket, targets[layer], body, "application/vnd.apache.parquet", prepared["dataset_sha256"][layer]))
        datasets[layer] = {"key": targets[layer], "uri": f"s3://{bucket}/{targets[layer]}",
                           "rows": len(prepared[layer + "_rows"]), "bytes": len(body),
                           "dataset_sha256": prepared["dataset_sha256"][layer],
                           "content_sha256": hashlib.sha256(body).hexdigest(),
                           "format": "parquet", "compression": "snappy"}
    receipt_times = [_timestamp(item["completed_at_utc"]) for item in prepared["receipts"]]
    latencies = [item["end_to_end_latency_seconds"] for item in prepared["receipts"]]
    manifest = {
        "schema_version": 2, "pipeline_version": PIPELINE_VERSION, "status": "SUCCEEDED",
        "origin": ORIGIN, "simulation": True, "run_id": run_id,
        "source_snapshot_run_id": first["source_snapshot_run_id"], "snapshot_id": snapshot_id,
        "expected_unique_events": len(prepared["events"]), "bronze_events": len(prepared["events"]),
        "silver_events": len(prepared["silver_rows"]), "gold_contributions": len(prepared["events"]),
        "gold_groups": len(prepared["gold_rows"]), "all_counts_match": True,
        "last_gold_ready_at_utc": max(receipt_times).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "gold_ready_latency_seconds": {"minimum": min(latencies), "maximum": max(latencies),
                                       "mean": math.fsum(latencies) / len(latencies)},
        "quality": {"blocking_checks_passed": True, "business_key_duplicates": 0,
                    "producer_bronze_hash_match_ratio": 1.0, "silver_gold_identity_match_ratio": 1.0,
                    "receipt_match_ratio": 1.0,
                    "note": "Only exact GOLD_READY events from events.jsonl; one source snapshot and one transform version."},
        "input_bytes": prepared["input_bytes"], "identities": prepared["identities"], "datasets": datasets,
        "reader_policy": "Read one manifest/snapshot; never sum historical snapshots. Replay sample, not an official indicator.",
    }
    _measured(_phases, "publish_manifest", lambda: _put_immutable(
        s3, bucket, manifest_key, canonical_json(manifest) + b"\n", "application/json", digest(manifest)))
    report = {"status": "SUCCEEDED", "run_id": run_id, "snapshot_id": snapshot_id,
              "manifest_key": manifest_key, "manifest_uri": f"s3://{bucket}/{manifest_key}",
              "expected_unique_events": len(prepared["events"]), "datasets": datasets,
              "gold_rows": prepared["gold_rows"], "origin": ORIGIN, "simulation": True}
    if output_dir is not None:
        folder = Path(output_dir)
        folder.mkdir(parents=True, exist_ok=False)
        for layer, body in bodies.items():
            (folder / (layer + ".parquet")).write_bytes(body)
        (folder / "manifest.json").write_bytes(canonical_json(manifest) + b"\n")
        (folder / "finalization_report.json").write_bytes(canonical_json(report) + b"\n")
    return report


def finalize_observed(s3, events, *, bucket=BUCKET, output_dir=None, encoder=parquet_bytes):
    """CLI path: persist a distinct operational attempt without changing the snapshot.

    The lower-level finalize_simulation remains deterministic for callers that
    reconcile analytical content. Real CLI executions always use this wrapper.
    Operational failures are re-raised after attempting to persist their metrics.
    """
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise FinalizationError("Invalid S3 bucket")
    if output_dir is not None and Path(output_dir).exists():
        raise FinalizationError("Output evidence directory already exists")
    attempt_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.perf_counter()
    phases = []
    audit_key = f"{EXECUTION_PREFIX}/attempt_id={attempt_id}/metrics.json"
    run_id = (events[0].get("run_id") if isinstance(events, list) and events
              and isinstance(events[0], dict) else None)
    if not isinstance(run_id, str) or not SAFE_ID.fullmatch(run_id):
        run_id = None
    report, failure = None, None
    try:
        report = finalize_simulation(s3, events, bucket=bucket, output_dir=output_dir,
                                     encoder=encoder, _phases=phases)
    except Exception as exc:
        failure = exc
    metrics = {"schema": "fiap.finalizer.observability.v1", "finalizer_version": FINALIZER_VERSION,
               "pipeline_version": PIPELINE_VERSION, "origin": ORIGIN, "simulation": True,
               "attempt_id": attempt_id, "run_id": run_id,
               "status": "FAILED" if failure else "SUCCEEDED",
               "started_at_utc": started_at,
               "finished_at_utc": datetime.now(timezone.utc).isoformat(),
               "duration_seconds": time.perf_counter() - started,
               "duration_scope": "Validation through immutable publication and local analytical evidence; excludes metrics persistence",
               "stages": phases, "audit_key": audit_key,
               "expected_events": len(events) if isinstance(events, list) else None,
               "input_event_objects": None, "output_silver_rows": None,
               "output_gold_groups": None, "output_parquet_bytes": None}
    if report:
        metrics.update(snapshot_id=report["snapshot_id"], manifest_key=report["manifest_key"],
            input_event_objects=report["expected_unique_events"],
            output_silver_rows=report["datasets"]["silver"]["rows"],
            output_gold_groups=report["datasets"]["gold"]["rows"],
            output_parquet_bytes={layer:data["bytes"] for layer,data in report["datasets"].items()})
    if failure:
        metrics["error_type"] = type(failure).__name__
    audit_failure = None
    try:
        _put_immutable(s3, bucket, audit_key, canonical_json(metrics) + b"\n", "application/json", digest(metrics))
    except Exception as exc:
        audit_failure = exc
    if output_dir is not None:
        folder = Path(output_dir)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "execution_metrics.json").write_bytes(canonical_json(metrics) + b"\n")
        if audit_failure:
            (folder / "audit_persistence_failure.json").write_bytes(canonical_json({
                "status": "FAILED", "error_type": type(audit_failure).__name__, "audit_key": audit_key}) + b"\n")
    if failure:
        failure.finalizer_execution_metrics = metrics
        if audit_failure:
            failure.add_note("Execution metrics could not be persisted to S3; inspect local evidence.")
        raise failure
    if audit_failure:
        raise FinalizationError("Analytical snapshot succeeded but execution metrics could not be persisted") from audit_failure
    report = dict(report, execution_metrics=metrics, execution_audit_uri=f"s3://{bucket}/{audit_key}")
    if output_dir is not None:
        (Path(output_dir) / "finalization_report.json").write_bytes(canonical_json(report) + b"\n")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", required=True, help="Producer events.jsonl containing unique IDs")
    parser.add_argument("--output-dir", required=True, help="New evidence directory; existing evidence is preserved")
    parser.add_argument("--bucket", default=BUCKET)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--notification-topic-arn", help="Optional approved SNS topic for failure notifications")
    args = parser.parse_args(argv)
    if args.notification_topic_arn and args.notification_topic_arn != f"arn:aws:sns:{args.region}:637246457028:fiap-inep-alertas-operacionais":
        parser.error("Failure notifications must use the project's approved topic in this account/region")
    if Path(args.output_dir).exists():
        parser.error("--output-dir already exists; select a new directory")
    events = read_events(args.events)
    import boto3
    s3 = boto3.client("s3", region_name=args.region)
    try:
        report = finalize_observed(s3, events, bucket=args.bucket, output_dir=args.output_dir)
    except Exception as exc:
        # Do not print payloads/identifiers from untrusted library error strings.
        # The exception still makes the CLI fail instead of masking failure.
        if args.notification_topic_arn:
            metrics = getattr(exc, "finalizer_execution_metrics", {})
            notice = {"kind": "FIAP_FINALIZER_FAILURE", "status": "FAILED",
                      "simulation": True, "attempt_id": metrics.get("attempt_id"),
                      "run_id": metrics.get("run_id"), "error_type": type(exc).__name__,
                      "finished_at_utc": metrics.get("finished_at_utc"),
                      "audit_key": metrics.get("audit_key"),
                      "action": "Inspect persisted execution metrics; repair before consuming a new snapshot"}
            delivery = {"topic_arn": args.notification_topic_arn, "status": "FAILED"}
            try:
                response = boto3.client("sns", region_name=args.region).publish(
                    TopicArn=args.notification_topic_arn, Subject="FIAP - falha no finalizador de simulacao",
                    Message=canonical_json(notice).decode("utf-8"))
                delivery.update(status="ACCEPTED_BY_SNS", message_id=response["MessageId"],
                                note="SNS acceptance is not proof of email receipt")
            except Exception as notification_error:
                delivery["error_type"] = type(notification_error).__name__
                exc.add_note("Failure notification could not be accepted by SNS; inspect local evidence")
            folder = Path(args.output_dir)
            folder.mkdir(parents=True, exist_ok=True)
            (folder / "failure_notification.json").write_bytes(canonical_json(delivery) + b"\n")
        raise
    print(json.dumps(report, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
