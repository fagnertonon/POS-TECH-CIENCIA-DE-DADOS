#!/usr/bin/env python3
"""Analyze saved log-growth JSONs locally; never makes AWS/network calls."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path

GB = D(2)**30


def dt(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def numeric(value):
    dec = D(str(value))
    return int(dec) if dec == dec.to_integral() else float(dec)


def observed_sum(points):
    return numeric(sum((D(str(p["value"])) for p in points), D(0))) if points else None


def buckets(points, period, start, end):
    groups = {"wholly_contained": [], "partly_overlapping": [], "outside": []}
    for p in points:
        left = dt(p["timestamp"])
        right = left + timedelta(seconds=period)
        if left >= start and right <= end:
            key = "wholly_contained"
        elif left < end and right > start:
            key = "partly_overlapping"
        else:
            key = "outside"
        groups[key].append({**p, "bucket_end_exclusive_utc": right.isoformat()})
    return {name: {"points": rows, "datapoint_count": len(rows),
                   "sum_observed": observed_sum(rows)} for name, rows in groups.items()}


def stock_summary(label, snap):
    groups = snap.get("groups", snap.get("logGroups", []))
    mapped = {g["logGroupName"]: g for g in groups}
    if len(mapped) != len(groups):
        raise ValueError("Duplicate log group in snapshot: " + label)
    vals = [g.get("storedBytes") for g in groups]
    total = sum(vals) if vals and all(isinstance(v, int) for v in vals) else None
    return {"label": label, "capture_start_utc": snap.get("capture_start_utc"),
            "capture_end_utc": snap.get("capture_end_utc"),
            "account": snap.get("account"), "region": snap.get("region"),
            "group_count": len(groups), "stock_bytes_reported": total,
            "groups": mapped}


def compare_stock(a, b):
    if a["account"] != b["account"] or a["region"] != b["region"]:
        raise ValueError("Snapshot account or region differs")
    before, after = a["groups"], b["groups"]
    names = set(before) & set(after)
    rows = []
    for name in sorted(names):
        x, y = before[name], after[name]
        delta = y["storedBytes"]-x["storedBytes"] if (
            isinstance(x.get("storedBytes"), int) and isinstance(y.get("storedBytes"), int)) else None
        changed = [k for k in ["creationTime", "retentionInDays", "logGroupClass"]
                   if x.get(k) != y.get(k)]
        rows.append({"log_group": name, "net_stock_delta_bytes": delta,
                     "metadata_changed": changed})
    comparable = set(before) == set(after) and a["stock_bytes_reported"] is not None and b["stock_bytes_reported"] is not None
    return {"from": a["label"], "to": b["label"],
            "elapsed_seconds": (dt(b["capture_end_utc"])-dt(a["capture_end_utc"])).total_seconds(),
            "groups_added": sorted(set(after)-set(before)),
            "groups_removed": sorted(set(before)-set(after)),
            "net_stock_delta_bytes": b["stock_bytes_reported"]-a["stock_bytes_reported"] if comparable else None,
            "per_group": rows,
            "generated_bytes_or_per_cycle_compressed_bytes": None}


def verify_raw_metric_values(ingestion):
    raw = {}
    for page in ingestion["pages"]:
        for row in page.get("MetricDataResults", []):
            times, values = row.get("Timestamps", []), row.get("Values", [])
            if len(times) != len(values):
                raise ValueError("Raw metric timestamp/value length mismatch")
            points = raw.setdefault(row["Id"], {})
            for timestamp, value in zip(times, values):
                key = dt(timestamp).isoformat()
                dec = D(str(value))
                if key in points and points[key] != dec:
                    raise ValueError("Conflicting raw metric timestamp")
                points[key] = dec
    for series in ingestion["series"]:
        prepared = {dt(p["timestamp"]).isoformat(): D(str(p["value"])) for p in series["points"]}
        if prepared != raw.get(series["id"], {}):
            raise ValueError("Prepared metric series differs from raw API values")


def analyze(collection, snapshots, runtime, archive_rate=D(".03"), ingestion_rate=D(".5")):
    ingestion = collection["ingestion"]
    verify_raw_metric_values(ingestion)
    query_by_id = {q["Id"]: q for q in ingestion["queries"]}
    per_group = {}
    replay = {"window_start_utc": runtime["window_start_utc"],
              "window_end_utc": runtime["window_end_utc"], "metrics": {}}
    for series in ingestion["series"]:
        query = query_by_id[series["id"]]["MetricStat"]
        name = query["Metric"]["MetricName"]
        group = next(x["Value"] for x in query["Metric"]["Dimensions"] if x["Name"] == "LogGroupName")
        points = series["points"]
        entry = {"query_id": series["id"], "observed_sum": observed_sum(points),
                 "datapoint_count": len(points), "response_complete": series["response_complete"],
                 "statuses": series["statuses"], "period_seconds": query["Period"],
                 "first_bucket": points[0]["timestamp"] if points else None,
                 "last_bucket": points[-1]["timestamp"] if points else None,
                 "missing_is_zero": False}
        per_group.setdefault(group, {})[name] = entry
        if group == "/aws/lambda/fiap-inep-streaming-consumer":
            replay["metrics"][name] = buckets(points, query["Period"],
                dt(runtime["window_start_utc"]), dt(runtime["window_end_utc"]))
    totals = {}
    for metric in ["IncomingBytes", "IncomingLogEvents"]:
        measured = [g[metric]["observed_sum"] for g in per_group.values() if g[metric]["observed_sum"] is not None]
        totals[metric] = {"sum_of_reported_datapoints": sum(measured) if measured else None,
                          "groups_with_datapoints": len(measured),
                          "groups_without_datapoints": [name for name, g in per_group.items() if g[metric]["observed_sum"] is None],
                          "missing_groups_assigned_zero": False}
    sequence = [stock_summary(label, snap) for label, snap in snapshots]
    current = stock_summary("current_detailed_collection", collection["stock_snapshot"])
    sequence.append(current)
    comparisons = [compare_stock(a, b) for a, b in zip(sequence, sequence[1:])]
    # Explicit T2 -> current also spans the preview and the replay.
    t2 = next((s for s in sequence if s["label"] == "T2"), None)
    if t2:
        comparisons.append(compare_stock(t2, current))
    bytes_now = current["stock_bytes_reported"]
    archive_gib = D(bytes_now)/GB if bytes_now is not None else None
    observed_ingestion = totals["IncomingBytes"]["sum_of_reported_datapoints"]
    group_stock = [{"log_group": name, "stock_bytes_reported": g.get("storedBytes"),
                    "log_group_class": g.get("logGroupClass"),
                    "retention_in_days": g.get("retentionInDays"),
                    "retention_field_present": "retentionInDays" in g}
                   for name, g in sorted(current["groups"].items())]
    session_items = (collection.get("activity", {}).get("sessions") or {}).get("items", [])
    t2_end = dt(t2["capture_end_utc"]) if t2 else dt(ingestion["start_inclusive_utc"])
    sessions_in_interval = [s for s in session_items if s.get("CreatedOn") and
        dt(s["CreatedOn"]) >= t2_end and dt(s["CreatedOn"]) < dt(current["capture_end_utc"])]
    return {
      "schema": "fiap.log-archive-closure.v1", "account": collection["account"],
      "region": collection["region"], "collection_at_utc": collection["captured_at_utc"],
      "collection_validation": {"api_calls": len(collection["api_calls"]),
        "api_errors": len(collection["errors"]), "raw_metric_values_reconciled": True,
        "metric_queries": len(ingestion["queries"]), "metric_series": len(ingestion["series"]),
        "all_series_api_complete": all(s["response_complete"] for s in ingestion["series"]),
        "ingestion_pagination_complete": ingestion["pagination_complete"],
        "omitted_log_groups": ingestion.get("omitted_log_groups", [])},
      "ingestion_measured": {"start_inclusive_utc": ingestion["start_inclusive_utc"],
        "end_exclusive_utc": ingestion["end_exclusive_utc"], "period_seconds": 300,
        "totals": totals, "groups": per_group,
        "interpretation": "Valid observed ingestion for today's returned buckets. Missing datapoints were not imputed as zero. These bytes are uncompressed, not archive size."},
      "reported_stock": {"current_bytes": bytes_now, "groups": group_stock,
        "captures": [{k:v for k,v in s.items() if k != "groups"} for s in sequence],
        "comparisons": comparisons,
        "known_30day_stock_reference": {"quantity_gib_month": str(archive_gib) if archive_gib is not None else None,
          "usd_per_gib_month": str(archive_rate), "usd": str(archive_gib*archive_rate) if archive_gib is not None else None,
          "basis": "Existing reported stock held for one normalized month; before free quotas/credits. This is not the future increase, and retention may reduce actual stock."}},
      "ingestion_cost_reference": {"observed_uncompressed_bytes": observed_ingestion,
        "usd_per_gib": str(ingestion_rate),
        "usd_for_observed_buckets": str(D(observed_ingestion)/GB*ingestion_rate) if observed_ingestion is not None else None,
        "basis": "Observed buckets only, before free quotas/credits. Not a per-cycle or monthly cost, not archive billing, not added to Cost Explorer."},
      "lambda_replay_bucket_context": {**replay,
        "runtime_invocations": runtime["logs"]["exclusive_target_invocation_totals"],
        "positive_processing_records": runtime["logs"]["discovered_positive_records"],
        "invalid_processing_records": runtime["logs"]["discovered_invalid_records"],
        "note": "198 log events are not 198 invocations or business events. The leading 300-second bucket overlaps the replay boundary, so its bytes cannot be time-prorated or attributed exclusively from the metric."},
      "mixed_activity_context": {"sessions_started_since_T2": sessions_in_interval,
        "all_listed_sessions_terminal": all(s.get("Status") in ["TIMEOUT","STOPPED","FAILED"] for s in session_items),
        "current_job_runs_collected": bool(collection.get("activity", {}).get("jobs")),
        "note": "The shared stock interval includes a historical preview plus the replay; per-group attribution is required."},
      "storage_metric_availability": {"policy": collection["storage_tier_policy"],
        "discovery_complete": collection["stored_bytes_metrics"]["discovery"]["pagination_complete"],
        "existing_metric_identities": len(collection["stored_bytes_metrics"]["queries"]),
        "no_metric_identities_does_not_mean_zero_storage": True},
      "archive_growth_decision": {"status": "UNAVAILABLE_COMPRESSED_BYTES_PER_CYCLE",
        "quantity": None, "unit": "GiB-month", "future_30d_usd": None,
        "reason": "Reported stock remains stable while positive ingestion is measured. No per-cycle compressed archive increment or archive byte-hour attribution was recovered. STANDARD policy and no StoredBytes identities are observed limitations, not zero consumption.",
        "base_and_increment_rule": "Count the current stock once. A future increment must refer only to cohorts after the baseline; a prior replay already in the baseline must not be added again. Do not add historical archive CE metering to the same stock projection.",
        "next_evidence": "A later consistent stock/billing capture with exclusive activity, unchanged retention/deletion scope and a justified attribution window; otherwise keep unavailable."},
      "scope": {"aws_calls_made_by_analysis": 0, "previous_model_changed": False,
                "compression_ratio_inferred": False, "parquet_bytes_used": False,
                "missing_data_converted_to_zero": False},
      "official_sources": [
        "https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CloudWatch-Logs-Monitoring-CloudWatch-Metrics.html",
        "https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_LogGroup.html",
        "https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_LogStream.html",
        "https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cwl_intelligent_tier.html",
        "https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_billing.html",
        "https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/Working-with-log-groups-and-streams.html"]}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--collection", required=True)
    p.add_argument("--snapshot", action="append", default=[], help="LABEL=path")
    p.add_argument("--runtime", required=True)
    p.add_argument("--output", required=True)
    args = p.parse_args()
    files = []
    def load(path):
        file = Path(path).resolve()
        raw = file.read_bytes()
        files.append({"path": str(file), "sha256": hashlib.sha256(raw).hexdigest()})
        return json.loads(raw.decode("utf-8"))
    snapshots = [(x.split("=",1)[0], load(x.split("=",1)[1])) for x in args.snapshot]
    report = analyze(load(args.collection), snapshots, load(args.runtime))
    report["source_files"] = files
    report["analyzer_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    out = Path(args.output)
    with out.open("x", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(json.dumps({"output": str(out.resolve()), "stock_bytes": report["reported_stock"]["current_bytes"],
      "incoming_bytes_observed": report["ingestion_measured"]["totals"]["IncomingBytes"]["sum_of_reported_datapoints"],
      "log_events_observed": report["ingestion_measured"]["totals"]["IncomingLogEvents"]["sum_of_reported_datapoints"],
      "archive_growth_status": report["archive_growth_decision"]["status"]}))


if __name__ == "__main__":
    main()
