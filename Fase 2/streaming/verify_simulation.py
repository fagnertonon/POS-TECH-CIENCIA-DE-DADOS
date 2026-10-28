"""Read-only evidence check for the simulated Bronze -> Silver -> Gold path.

No queue messages are sent, received, deleted or redriven. The optional older
verify_streaming.py remains responsible for SQS/DLQ and CloudWatch evidence.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import statistics
from urllib.parse import urlparse

import lambda_function as consumer
import producer


def timestamp(value):
    if not isinstance(value, str) or not consumer.UTC_TIMESTAMP.fullmatch(value):
        raise ValueError("Expected a UTC ISO timestamp")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def distribution(values):
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    return {"count": len(values), "min": ordered[0], "max": ordered[-1],
            "mean": statistics.fmean(values), "median": statistics.median(values),
            "p95_nearest_rank": ordered[max(0, (95 * len(ordered) + 99) // 100 - 1)]}


def default_config(bucket):
    return consumer.Config(bucket, "bronze/inep_streaming", "auditoria/streaming/idempotency",
                           frozenset({"alunos"}))


def object_json(s3, bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    raw = response["Body"].read()
    decoded = gzip.decompress(raw) if key.endswith(".gz") else raw
    value = json.loads(decoded, parse_constant=consumer._reject_constant)
    return value, {"key": key, "bytes": len(raw), "object_sha256": hashlib.sha256(raw).hexdigest()}


def inventory(s3, bucket, prefix, run_id):
    marker = f"/run_id={run_id}/"
    return sorted(obj["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix.rstrip("/") + "/")
        for obj in page.get("Contents", []) if marker in obj["Key"])


def missing_object(exc):
    return consumer._code(exc) in {"NoSuchKey", "NotFound", "404"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_derived(value, envelope, expected_data, layer, bronze_key):
    require(type(value.get("schema_version")) is int and value["schema_version"] == 2, f"{layer}: schema_version")
    require(value.get("pipeline_version") == consumer.PIPELINE_VERSION, f"{layer}: pipeline_version")
    require(value.get("layer") == layer, f"{layer}: layer")
    require(value.get("origin") == consumer.ORIGIN and value.get("simulation") is True,
            f"{layer}: simulation provenance")
    require(value.get("event_sha256") == consumer.sha256(envelope), f"{layer}: event hash")
    provenance = value.get("provenance", {})
    expected = {name: envelope[name] for name in
                ("run_id", "event_id", "source_snapshot_run_id", "event_time")}
    expected["bronze_key"] = bronze_key
    require(consumer.sha256(provenance) == consumer.sha256(expected), f"{layer}: exact provenance")
    require(consumer.sha256(value.get("data")) == consumer.sha256(expected_data),
            f"{layer}: normalized data does not match original event")
    require(value.get("data_sha256") == consumer.sha256(expected_data), f"{layer}: data hash")


def validate_producer_report(value, bodies, envelopes):
    require(value.get("status") == "SUCCEEDED", "producer report status")
    require(value.get("simulation") is True, "producer report simulation")
    require(value.get("run_id") == envelopes[0]["run_id"], "producer report run_id")
    require(value.get("unique_event_count") == len(envelopes), "producer report unique count")
    results = value.get("send_results", [])
    hashes = {e["event_id"]: hashlib.sha256(body.encode("utf-8")).hexdigest()
              for e, body in zip(envelopes, bodies)}
    require(value.get("confirmed_send_count") == len(results), "producer confirmed count")
    new_ids = [item["event_id"] for item in results if item.get("kind") == "NEW"]
    require(len(new_ids) == len(envelopes) and set(new_ids) == set(hashes), "producer new identities")
    for item in results:
        require(item.get("kind") in {"NEW", "DUPLICATE"}, "producer send kind")
        require(item.get("run_id") == value["run_id"], "producer send run_id")
        require(item.get("event_id") in hashes, "producer unknown event")
        require(item.get("body_sha256") == hashes[item["event_id"]], "producer changed duplicate/body")
    duplicates = len(results) - len(new_ids)
    require(duplicates == value.get("duplicate_send_count"), "producer duplicate count")
    return {"confirmed_new": len(new_ids), "confirmed_duplicate_sends": duplicates,
            "limitation": "Send confirmations alone do not prove delivery; S3 identities are verified separately."}


def verify_run(s3, bodies, *, bucket="fiap-tc-fase2-2026", config=None, producer_report=None):
    config = config or default_config(bucket)
    envelopes = [consumer.parse_event(body, config)[0] for body in bodies]
    require(bool(envelopes), "No events supplied")
    run_id = envelopes[0]["run_id"]
    ids = [item["event_id"] for item in envelopes]
    require(len(set(ids)) == len(ids), "Expected events must contain unique event IDs")
    require(all(item["run_id"] == run_id for item in envelopes), "Expected exactly one run")
    report = {"verified_at_utc": consumer.iso_utc(consumer.utc_now()),
              "scope": "Read-only exact identities and immutable objects through simulated Gold contributions",
              "run_id": run_id, "simulation": True, "origin": consumer.ORIGIN,
              "pipeline_version": consumer.PIPELINE_VERSION, "bucket": bucket,
              "expected_event_count": len(envelopes), "events": [], "errors": [], "pending": [],
              "inventory": {}, "latency_definition":
              "event_time from the producer to completed_at_utc measured after Gold PUT; receipt is written afterwards"}
    if producer_report is not None:
        try:
            report["producer_validation"] = validate_producer_report(producer_report, bodies, envelopes)
        except (ValueError, KeyError, TypeError) as exc:
            report["errors"].append(str(exc))
    expected_keys = {name: set() for name in ("bronze", "claim", "silver", "gold", "receipt")}
    latencies = []
    for envelope in envelopes:
        paths = consumer.stage_paths(envelope, config)
        paths["claim_key"] = f"{config.idempotency_prefix}/run_id={run_id}/event_id={envelope['event_id']}.json"
        for layer in expected_keys:
            expected_keys[layer].add(paths[layer + "_key"])
        item = {"event_id": envelope["event_id"], "event_sha256": consumer.sha256(envelope), "objects": {}}
        try:
            # Invalid source data is FAILED, not an apparently still-processing
            # run merely because the downstream quality gate kept objects absent.
            silver_data, gold_data = consumer.transform_payload(envelope["payload"])
            values = {}
            for layer in expected_keys:
                values[layer], item["objects"][layer] = object_json(s3, bucket, paths[layer + "_key"])
            bronze = values["bronze"]
            require(consumer.sha256(bronze.get("event")) == consumer.sha256(envelope), "Bronze changed original envelope")
            ingestion = bronze.get("ingestion", {})
            require(ingestion.get("event_sha256") == consumer.sha256(envelope), "Bronze event hash")
            require(ingestion.get("payload_sha256") == consumer.sha256(envelope["payload"]), "Bronze payload hash")
            require(ingestion.get("target_key") == paths["bronze_key"], "Bronze target key")
            received = timestamp(ingestion["received_at_utc"])
            timestamp(ingestion["claim_started_at_utc"])
            require(consumer.sha256(values["claim"]) == consumer.sha256(bronze), "Claim and Bronze first reception differ")
            validate_derived(values["silver"], envelope, silver_data, "silver", paths["bronze_key"])
            validate_derived(values["gold"], envelope, gold_data, "gold_contribution", paths["bronze_key"])
            receipt = values["receipt"]
            require(type(receipt.get("schema_version")) is int and receipt["schema_version"] == 2, "Receipt schema_version")
            require(receipt.get("status") == "GOLD_READY", "Receipt Gold status")
            require(receipt.get("pipeline_version") == consumer.PIPELINE_VERSION, "Receipt pipeline_version")
            require(receipt.get("origin") == consumer.ORIGIN and receipt.get("simulation") is True,
                    "Receipt simulation provenance")
            for name in ("run_id", "event_id", "source_snapshot_run_id", "event_time"):
                require(receipt.get(name) == envelope[name], "Receipt " + name)
            require(receipt.get("event_sha256") == consumer.sha256(envelope), "Receipt event hash")
            for layer in ("bronze", "silver", "gold"):
                require(receipt.get(layer + "_key") == paths[layer + "_key"], "Receipt " + layer + " path")
            for layer, data in (("silver", silver_data), ("gold", gold_data)):
                require(receipt.get(layer + "_data_sha256") == consumer.sha256(data), "Receipt " + layer + " data hash")
            completed = timestamp(receipt["completed_at_utc"])
            measured = receipt.get("end_to_end_latency_seconds")
            require(type(measured) in (int, float) and math.isfinite(measured) and measured >= 0,
                    "Receipt finite non-negative latency")
            elapsed = max(0.0, (completed - timestamp(envelope["event_time"])).total_seconds())
            require(abs(measured - elapsed) <= 0.00001, "Receipt latency differs from independent timestamps")
            require(completed >= received, "Gold completion precedes first Lambda reception")
            latencies.append(measured)
            item.update(status="SUCCEEDED", end_to_end_latency_seconds=measured,
                        completed_at_utc=receipt["completed_at_utc"])
        except Exception as exc:
            item["status"] = "INCOMPLETE" if missing_object(exc) else "FAILED"
            item["error_type"] = type(exc).__name__
            item["reason"] = str(exc) if isinstance(exc, (ValueError, KeyError, TypeError)) else consumer._code(exc) or type(exc).__name__
            report["pending" if item["status"] == "INCOMPLETE" else "errors"].append(
                {"event_id": envelope["event_id"], "reason": item["reason"]})
        report["events"].append(item)
    prefixes = {"bronze": config.prefix, "claim": config.idempotency_prefix,
                "silver": config.silver_prefix, "gold": config.gold_prefix, "receipt": config.processed_prefix}
    for layer, prefix in prefixes.items():
        try:
            # Finalizer Parquet snapshots are separate from the per-event objects.
            actual = {key for key in inventory(s3, bucket, prefix, run_id)
                      if "/event_id=" in key}
            missing, extra = sorted(expected_keys[layer] - actual), sorted(actual - expected_keys[layer])
            report["inventory"][layer] = {"expected": len(expected_keys[layer]), "actual": len(actual),
                                          "missing": missing, "unexpected": extra}
            if missing:
                report["pending"].append({"layer": layer, "missing_keys": missing})
            if extra:
                report["errors"].append({"layer": layer, "unexpected_keys": extra})
        except Exception as exc:
            report["errors"].append({"layer": layer, "inventory_error_type": type(exc).__name__})
    report["end_to_end_latency_seconds"] = distribution(latencies)
    report["verified_event_count"] = sum(item["status"] == "SUCCEEDED" for item in report["events"])
    report["validation_status"] = "FAILED" if report["errors"] else "INCOMPLETE" if report["pending"] else "SUCCEEDED"
    return report


def verify_finalized_snapshot(s3, finalization_report, bucket, expected_events):
    """Validate published finalizer artifacts; Parquet is read locally, never queried."""
    import pyarrow.parquet as pq
    import finalize_simulation as finalizer

    value = finalization_report
    require(value.get("status") == "SUCCEEDED", "Finalization report status")
    prepared = finalizer.read_run(s3, expected_events, bucket)
    first = prepared["events"][0]
    require(value.get("run_id") == first["run_id"], "Finalization report belongs to another run")
    require(value.get("snapshot_id") == prepared["snapshot_id"], "Finalization report snapshot identities")
    uri = value.get("manifest_uri") or value.get("s3_manifest_uri") or value.get("s3manifesturi")
    require(isinstance(uri, str), "Finalization report must reference its S3 manifest")
    location = urlparse(uri)
    require(location.scheme == "s3" and location.netloc == bucket, "Finalization manifest bucket")
    manifest, evidence = object_json(s3, bucket, location.path.lstrip("/"))
    expected_key = (f"auditoria/streaming/analitico/runs/run_id={first['run_id']}/"
                    f"snapshot_id={prepared['snapshot_id']}/manifest.json")
    require(evidence["key"] == expected_key, "Finalization manifest exact key")
    require(manifest.get("status") == "SUCCEEDED", "Finalization manifest is not committed")
    require(manifest.get("simulation") is True, "Finalization manifest is not simulation")
    require(manifest.get("origin") == consumer.ORIGIN, "Finalization manifest origin")
    require(manifest.get("pipeline_version") == consumer.PIPELINE_VERSION, "Finalization manifest version")
    for name, expected in {"run_id": first["run_id"], "source_snapshot_run_id": first["source_snapshot_run_id"],
                           "snapshot_id": prepared["snapshot_id"], "expected_unique_events": len(expected_events),
                           "bronze_events": len(expected_events), "silver_events": len(prepared["silver_rows"]),
                           "gold_contributions": len(expected_events), "gold_groups": len(prepared["gold_rows"]),
                           "all_counts_match": True}.items():
        require(consumer.sha256(manifest.get(name)) == consumer.sha256(expected), "Finalization manifest " + name)
    require(consumer.sha256(manifest.get("identities")) == consumer.sha256(prepared["identities"]),
            "Finalization manifest exact event identities")
    datasets = manifest.get("datasets")
    require(isinstance(datasets, (list, dict)) and len(datasets) == 2, "Expected Silver and Gold Parquet datasets")
    results = []
    entries = datasets.items() if isinstance(datasets, dict) else [(item.get("name"), item) for item in datasets]
    for name, artifact in entries:
        require(name in {"silver", "gold"}, "Unexpected finalized dataset")
        key = artifact.get("key") or artifact.get("s3_key")
        if not key:
            key = urlparse(artifact.get("uri", "")).path.lstrip("/")
        require(isinstance(key, str) and key.endswith(".parquet"), "Finalized dataset Parquet key")
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        content_hash = hashlib.sha256(raw).hexdigest()
        require(content_hash == artifact.get("content_sha256"), "Finalized dataset content hash")
        parquet = pq.ParquetFile(io.BytesIO(raw))
        table = parquet.read()
        expected_rows = artifact.get("row_count", artifact.get("rows"))
        require(table.num_rows == expected_rows, "Finalized dataset row count")
        require("simulation" in table.column_names and all(flag is True for flag in table["simulation"].to_pylist()),
                "Finalized dataset simulation provenance")
        require("origin" in table.column_names and set(table["origin"].to_pylist()) == {consumer.ORIGIN},
                "Finalized dataset origin")
        rows_hash = consumer.sha256(table.to_pylist())
        require(rows_hash == artifact.get("dataset_sha256") == prepared["dataset_sha256"][name],
                "Finalized dataset logical rows differ from exact event projection")
        expected_path = (f"{name}/inep_streaming/" + ("compactado" if name == "silver" else "indicador_uf")
                         + f"/run_id={first['run_id']}/snapshot_id={prepared['snapshot_id']}/part-00000.parquet")
        require(key == expected_path, "Finalized dataset exact key")
        require(artifact.get("compression") == "snappy", "Finalized dataset compression declaration")
        require(all(parquet.metadata.row_group(group).column(column).compression == "SNAPPY"
                    for group in range(parquet.metadata.num_row_groups)
                    for column in range(parquet.metadata.num_columns)), "Finalized dataset compression")
        results.append({"name": name, "key": key, "rows": table.num_rows,
                        "bytes": len(raw), "content_sha256": content_hash, "dataset_sha256": rows_hash})
    return {"status": "SUCCEEDED", "manifest": evidence, "datasets": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", required=True, type=Path)
    parser.add_argument("--producer-report", type=Path)
    parser.add_argument("--finalization-report", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--bucket", default="fiap-tc-fase2-2026")
    parser.add_argument("--region", default="us-east-2")
    args = parser.parse_args(argv)
    import boto3

    bodies, _run_id = producer.read_replay(args.events)
    producer_report = json.loads(args.producer_report.read_text(encoding="utf-8")) if args.producer_report else None
    s3 = boto3.client("s3", region_name=args.region)
    report = verify_run(s3, bodies, bucket=args.bucket, producer_report=producer_report)
    if args.finalization_report:
        try:
            finalized = json.loads(args.finalization_report.read_text(encoding="utf-8"))
            report["finalized_snapshot"] = verify_finalized_snapshot(
                s3, finalized, args.bucket, [json.loads(body) for body in bodies])
        except Exception as exc:
            report["errors"].append({"finalized_snapshot_error": str(exc), "error_type": type(exc).__name__})
            report["validation_status"] = "FAILED"
    else:
        report["finalized_snapshot"] = {"status": "NOT_CHECKED",
                                        "reason": "Supply --finalization-report to verify compacted analytic Parquet outputs"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"validation_status": report["validation_status"], "run_id": report["run_id"],
                      "verified_events": report["verified_event_count"], "report": str(args.output)}, ensure_ascii=False))
    return 0 if report["validation_status"] == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
