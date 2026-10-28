#!/usr/bin/env python3
"""Bounded read-only AWS log-stock/ingestion collector. Does not read log content."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import zipfile

UTC = timezone.utc
ALLOW = {
    "sts": {"get_caller_identity"},
    "logs": {"describe_log_groups", "get_storage_tier_policy"},
    "cloudwatch": {"list_metrics", "get_metric_data"},
    "glue": {"get_job_runs", "list_sessions"},
}
GROUP_PAGE_CAP = 10
METRIC_PAGE_CAP = 8
METRIC_GROUP_CAP = 50
STORED_IDENTITY_CAP = 100
STORED_LIST_PAGE_CAP = 4


def now():
    return datetime.now(UTC)


def iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_time(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Use an explicit UTC offset or Z in timestamps")
    return parsed.astimezone(UTC)


def dump(value):
    return json.dumps(value, ensure_ascii=False, indent=2,
                      default=lambda x: iso(x) if isinstance(x, datetime) else str(x),
                      allow_nan=False) + "\n"


def trim(value, fields):
    return {key: value[key] for key in fields if key in value}


class Reader:
    def __init__(self, clients):
        self.clients = clients
        self.calls = []
        self.errors = []

    def call(self, service, operation, **kwargs):
        if operation not in ALLOW.get(service, set()):
            raise ValueError("Operation outside explicit read-only allowlist")
        started = now()
        entry = {"service": service, "operation": operation,
                 "started_at_utc": iso(started)}
        self.calls.append(entry)
        try:
            result = getattr(self.clients[service], operation)(**kwargs)
            entry.update(ended_at_utc=iso(now()), status="SUCCESS",
                         response_metadata=result.get("ResponseMetadata", {}))
            return result
        except Exception as exc:
            entry.update(ended_at_utc=iso(now()), status="UNAVAILABLE")
            error = {"service": service, "operation": operation,
                     "error_type": type(exc).__name__, "message": str(exc)}
            if isinstance(getattr(exc, "response", None), dict):
                error["aws_error"] = exc.response.get("Error", {})
            self.errors.append(error)
            return None


def paginate(reader, service, operation, result_key, page_cap, **kwargs):
    rows, pages, token, seen = [], [], None, set()
    complete = False
    reason = None
    for _ in range(page_cap):
        request = dict(kwargs)
        if token:
            request["nextToken" if service == "logs" else "NextToken"] = token
        response = reader.call(service, operation, **request)
        if response is None:
            reason = "API_UNAVAILABLE"
            break
        pages.append(response)
        rows.extend(response.get(result_key, []))
        token = response.get("nextToken") or response.get("NextToken")
        if not token:
            complete = True
            break
        if token in seen:
            reason = "REPEATED_TOKEN"
            break
        seen.add(token)
    if not complete and reason is None:
        reason = "PAGE_CAP"
    return {"items": rows, "pages": pages, "pagination_complete": complete,
            "page_cap": page_cap, "page_count": len(pages), "limit_reason": reason}


def metric_data(reader, queries, start, end):
    if not queries:
        return {"queries": [], "pages": [], "series": [],
                "pagination_complete": True, "scope": "NO_EXISTING_IDENTITIES",
                "missing_is_zero": False}
    pages, token, seen, complete = [], None, set(), False
    for _ in range(METRIC_PAGE_CAP):
        request = {"MetricDataQueries": queries, "StartTime": start,
                   "EndTime": end, "ScanBy": "TimestampAscending",
                   "MaxDatapoints": 100800}
        if token:
            request["NextToken"] = token
        response = reader.call("cloudwatch", "get_metric_data", **request)
        if response is None:
            break
        pages.append(response)
        token = response.get("NextToken")
        if not token:
            complete = True
            break
        if token in seen:
            break
        seen.add(token)
    merged = {q["Id"]: {"id": q["Id"], "points": [], "statuses": [],
                        "messages": []} for q in queries}
    for page in pages:
        for result in page.get("MetricDataResults", []):
            if result.get("Id") not in merged:
                continue
            entry = merged[result["Id"]]
            entry["statuses"].append(result.get("StatusCode"))
            entry["messages"].extend(result.get("Messages", []))
            times, values = result.get("Timestamps", []), result.get("Values", [])
            if len(times) != len(values):
                entry["messages"].append({"Code": "LOCAL_LENGTH_MISMATCH"})
            entry["points"].extend({"timestamp": iso(t) if isinstance(t, datetime) else t,
                                    "value": v} for t, v in zip(times, values))
    for entry in merged.values():
        unique = {}
        conflicts = []
        for point in entry["points"]:
            key = point["timestamp"]
            if key in unique and unique[key] != point["value"]:
                conflicts.append(key)
            unique[key] = point["value"]
        entry["points"] = [{"timestamp": k, "value": v} for k, v in sorted(unique.items())]
        entry["conflicting_timestamps"] = conflicts
        entry["has_data"] = bool(entry["points"])
        entry["observed_sum"] = (sum(p["value"] for p in entry["points"])
                                 if entry["points"] else None)
        # A sum of stock observations is not a stock or a billable byte-month.
        query = next(q for q in queries if q["Id"] == entry["id"])
        if query["MetricStat"]["Metric"]["MetricName"] == "StoredBytes":
            entry.pop("observed_sum")
        entry["response_complete"] = (complete and bool(entry["statuses"])
                                      and all(s == "Complete" for s in entry["statuses"])
                                      and not conflicts)
    return {"queries": queries, "pages": pages, "series": list(merged.values()),
            "start_inclusive_utc": iso(start), "end_exclusive_utc": iso(end),
            "pagination_complete": complete, "page_cap": METRIC_PAGE_CAP,
            "page_count": len(pages), "missing_is_zero": False}


def query(ident, metric, dimensions, period, statistic):
    return {"Id": ident, "ReturnData": True,
            "MetricStat": {"Metric": {"Namespace": "AWS/Logs",
                                      "MetricName": metric,
                                      "Dimensions": dimensions},
                           "Period": period, "Stat": statistic}}


def collect(reader, region, expected_account, start, end, jobs=(), sessions=False):
    identity = reader.call("sts", "get_caller_identity")
    if not identity or identity.get("Account") != expected_account:
        raise RuntimeError("Caller account unavailable or different from expected account")
    captured = now()
    inventory = paginate(reader, "logs", "describe_log_groups", "logGroups",
                         GROUP_PAGE_CAP, limit=50)
    groups = inventory["items"]
    names = sorted({g["logGroupName"] for g in groups if "logGroupName" in g})
    selected = names[:METRIC_GROUP_CAP]
    stocks = [g.get("storedBytes") for g in groups]
    total = sum(stocks) if stocks and all(isinstance(v, int) for v in stocks) else None
    stock = {"schema": "fiap.log-stock.v1", "account": expected_account,
             "region": region, "capture_start_utc": iso(captured),
             "capture_end_utc": iso(now()), "groups": groups,
             "total_stored_bytes": total,
             "pagination_complete": inventory["pagination_complete"],
             "cloud_mutations": [], "starts_compute": False}
    ingestion_queries = []
    for name in selected:
        for metric in ["IncomingBytes", "IncomingLogEvents"]:
            ingestion_queries.append(query("i" + str(len(ingestion_queries)), metric,
                                     [{"Name": "LogGroupName", "Value": name}], 300, "Sum"))
    ingestion = metric_data(reader, ingestion_queries, start, end)
    ingestion["log_group_cap"] = METRIC_GROUP_CAP
    ingestion["omitted_log_groups"] = names[METRIC_GROUP_CAP:]
    policy = reader.call("logs", "get_storage_tier_policy")
    existing = paginate(reader, "cloudwatch", "list_metrics", "Metrics",
                        STORED_LIST_PAGE_CAP, Namespace="AWS/Logs", MetricName="StoredBytes")
    identities = {}
    excluded = []
    for metric in existing["items"]:
        dimensions = sorted(metric.get("Dimensions", []), key=lambda d: (d["Name"], d["Value"]))
        lookup = {d["Name"]: d["Value"] for d in dimensions}
        if lookup.get("LogGroupName") not in names:
            excluded.append(metric)
            continue
        key = tuple((d["Name"], d["Value"]) for d in dimensions)
        identities[key] = dimensions
    chosen = sorted(identities)[:STORED_IDENTITY_CAP]
    stored_queries = [query("s" + str(i), "StoredBytes", identities[key], 86400, "Average")
                      for i, key in enumerate(chosen)]
    # Discover first. Never create a metric or activate a storage policy.
    stored = metric_data(reader, stored_queries, start - timedelta(days=7), end)
    stored.update(identity_cap=STORED_IDENTITY_CAP,
                  identities_omitted=max(0, len(identities)-STORED_IDENTITY_CAP),
                  discovery=existing, identities_outside_groups=excluded,
                  interpretation="Daily stock observations, not a per-run compressed increment; do not sum them.")
    activity = {"jobs": {}, "sessions": None,
                "scope": "Optional bounded metadata only; no job/session is started."}
    run_fields = ["Id", "JobName", "JobRunState", "StartedOn", "CompletedOn", "LastModifiedOn",
                  "ExecutionTime", "DPUSeconds", "NumberOfWorkers", "WorkerType", "Timeout"]
    for job in jobs:
        result = paginate(reader, "glue", "get_job_runs", "JobRuns", 2,
                          JobName=job, MaxResults=10)
        result["items"] = [trim(x, run_fields) for x in result["items"]]
        # Do not export Arguments, which can contain user-supplied sensitive values.
        result.pop("pages")
        activity["jobs"][job] = result
    if sessions:
        result = paginate(reader, "glue", "list_sessions", "Sessions", 3, MaxResults=100)
        result["items"] = [trim(x, ["Id", "Status", "CreatedOn", "CompletedOn", "IdleTimeout",
                                    "Timeout", "NumberOfWorkers", "WorkerType", "ExecutionTime",
                                    "DPUSeconds"]) for x in result["items"]]
        result.pop("pages")
        activity["sessions"] = result
    return {"schema": "fiap.log-growth-readonly.v1", "account": expected_account,
            "region": region, "captured_at_utc": iso(captured), "stock_snapshot": stock,
            "log_group_inventory": inventory, "ingestion": ingestion,
            "storage_tier_policy": {"available": policy is not None, "response": policy},
            "stored_bytes_metrics": stored, "activity": activity,
            "completed_at_utc": iso(now()), "errors": reader.errors, "api_calls": reader.calls,
            "scope": {"aws_mutations": [], "content_logs_read": False,
                      "queries_started": False, "cost_explorer_called": False,
                      "glue_runs_started": False, "bigquery_reads_started": False,
                      "missing_datapoints_equal_zero": False,
                      "stock_increment_attributed_to_a_cycle": False},
            "limits": ["An absent series or unavailable API is not zero usage.",
                       "Stock can be delayed; net change does not prove generated bytes.",
                       "IncomingBytes is uncompressed ingestion, not compressed archive bytes.",
                       "Only existing StoredBytes identities were queried; no tiering was enabled.",
                       "DataProtection/ForwardedBytes do not measure archive growth.",
                       "API reads may be metered; this is a single bounded collection, not a schedule."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--expected-account", default="637246457028")
    parser.add_argument("--start", help="Timezone-aware UTC start; default today00:00UTC")
    parser.add_argument("--end", help="Exclusive end; default latest complete300-second boundary")
    parser.add_argument("--glue-job", action="append", default=[])
    parser.add_argument("--include-sessions", action="store_true")
    args = parser.parse_args()
    captured = now()
    start = parse_time(args.start) if args.start else captured.replace(hour=0, minute=0, second=0, microsecond=0)
    end = parse_time(args.end) if args.end else datetime.fromtimestamp(int(captured.timestamp())//300*300, UTC)
    if not start < end or end > captured or (end-start).total_seconds() > 2*86400:
        parser.error("Require start<end<=now and a window no longer than two days")
    if len(args.glue_job) > 10:
        parser.error("At most ten named Glue jobs")
    import boto3
    from botocore.config import Config
    config = Config(connect_timeout=10, read_timeout=20, retries={"max_attempts": 2, "mode": "standard"})
    session = boto3.Session(region_name=args.region)
    reader = Reader({name: session.client(name, config=config) for name in ALLOW})
    result = collect(reader, args.region, args.expected_account, start, end,
                     jobs=args.glue_job, sessions=args.include_sessions)
    result["collector_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    out = Path(args.output_dir).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=False)
    evidence = out/"log_growth_readonly.json"
    evidence.write_text(dump(result), encoding="utf-8")
    hashes = {evidence.name: hashlib.sha256(evidence.read_bytes()).hexdigest()}
    (out/"SHA256.json").write_text(dump(hashes), encoding="utf-8")
    archive = out/"log_growth_readonly.zip"
    with zipfile.ZipFile(archive, "x", compression=zipfile.ZIP_DEFLATED) as z:
        z.write(evidence, evidence.name)
        z.write(out/"SHA256.json", "SHA256.json")
    print(json.dumps({"output_dir": str(out), "zip": str(archive),
                      "zip_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                      "json_sha256": hashes[evidence.name], "errors": len(reader.errors),
                      "stock_bytes": result["stock_snapshot"]["total_stored_bytes"],
                      "api_calls": len(reader.calls)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
