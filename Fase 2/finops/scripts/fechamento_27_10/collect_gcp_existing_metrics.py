"""Read existing GCP telemetry only. No SQL, row reads, API/billing/IAM changes.

Run only after the human authorizes Cloud Shell's credential-access prompt.
The existing gcloud token stays in memory. Raw audit entries and credential
identifiers are never written or printed. Missing evidence is never usage zero.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import subprocess
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

PROJECT = "fiap1-512214"
SERVICE = "bigquerystorage.googleapis.com"
TABLES = ("alunos", "municipio", "uf", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf",
          "meta_alfabetizacao_municipio", "dicionario")
START = "2026-10-20T18:20:00Z"
END = "2026-10-20T18:30:00Z"
MAX_CALLS, MAX_BYTES, MAX_PAGES, MAX_OUTPUT = 40, 2 * 1024 * 1024, 3, 20000
SAFE = re.compile(r"^[A-Za-z0-9_./:{}*%+-]{1,240}$")
NUMERIC = re.compile(r"^\d{1,30}$")
BYTE_KEYS = {"scannedBytes", "serializedResponseBytes", "scanned_bytes", "serialized_response_bytes"}


def valid_window(start, end):
    a, b = (datetime.fromisoformat(v.replace("Z", "+00:00")) for v in (start, end))
    if not a.tzinfo or not b.tzinfo or not 0 < (b-a).total_seconds() <= 31*86400:
        raise ValueError("An explicit timezone and a positive window <=31 days are required")


def safe(value):
    return value if isinstance(value, str) and SAFE.fullmatch(value) and "://" not in value else None


def error_summary(status, payload):
    """Do not export server messages: they can contain emails, URLs or tokens."""
    e = payload.get("error", {}) if isinstance(payload, dict) else {}
    result = {"http_status": status, "status": safe(e.get("status"))}
    message = str(e.get("message", ""))
    if "billing" in message.lower():
        result["billing_required"] = True
    m = re.search(r"project\s+#(\d{4,24})", message)
    if m:
        result["consumer_project_number"] = m.group(1)
    for detail in e.get("details", []):
        if not isinstance(detail, dict):
            continue
        if safe(detail.get("reason")):
            result["reason"] = detail["reason"]
        metadata = detail.get("metadata", {})
        if safe(metadata.get("service")):
            result["service"] = metadata["service"]
    return result


class ReadOnlyAPI:
    def __init__(self, token):
        self.token, self.calls = token, 0

    def request(self, url, params=None, body=None):
        hosts = ("https://monitoring.googleapis.com/", "https://cloudresourcemanager.googleapis.com/",
                 "https://cloudbilling.googleapis.com/", "https://serviceusage.googleapis.com/",
                 "https://logging.googleapis.com/", "https://bigquery.googleapis.com/bigquery/v2/projects/basedosdados/datasets/br_inep_avaliacao_alfabetizacao/tables/")
        allowed_post = ("https://logging.googleapis.com/v2/entries:list",
                        f"https://cloudresourcemanager.googleapis.com/v1/projects/{PROJECT}:testIamPermissions")
        if not url.startswith(hosts) or (body is not None and url not in allowed_post):
            raise ValueError("Read-only endpoint not allowlisted")
        if self.calls >= MAX_CALLS:
            return None, {"request_cap_reached": True}
        self.calls += 1
        if params:
            url += "?" + urlencode(params)
        data = json.dumps(body).encode() if body is not None else None
        request = Request(url, data=data, headers={"Authorization": "Bearer " + self.token,
                                                  "Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=25) as response:
                raw = response.read(MAX_BYTES+1)
            if len(raw) > MAX_BYTES:
                return None, {"response_byte_cap_reached": True}
            return json.loads(raw), None
        except HTTPError as exc:
            raw = exc.read(MAX_BYTES+1)
            try:
                payload = json.loads(raw) if len(raw) <= MAX_BYTES else {}
            except (ValueError, UnicodeError):
                payload = {}
            return None, error_summary(exc.code, payload)
        except Exception as exc:
            return None, {"transport_error_type": type(exc).__name__}


def pages(api, url, key, params=None, body=None):
    result, complete, errors = [], False, []
    params, body = dict(params or {}), dict(body) if body is not None else None
    for _ in range(MAX_PAGES):
        data, error = api.request(url, params, body)
        if error:
            errors.append(error)
            break
        result.extend(data.get(key, []))
        token = data.get("nextPageToken")
        if not token:
            complete = True
            break
        (body if body is not None else params)["pageToken"] = token
    return result, {"complete": complete, "errors": errors}


def summarize_series(series, metric_type):
    totals, seen = {}, set()
    for s in series:
        labels = s.get("resource", {}).get("labels", {})
        method = safe(labels.get("method")) or "unclassified"
        if labels.get("service") not in (None, SERVICE):
            continue
        identity = hashlib.sha256(json.dumps([s.get("metric"), s.get("resource")], sort_keys=True).encode()).hexdigest()
        item = totals.setdefault(method, {"points": 0, "delta_sum": None, "max_value": None})
        for p in s.get("points", []):
            ident = (identity, json.dumps(p.get("interval", {}), sort_keys=True))
            if ident in seen:
                continue
            seen.add(ident)
            v = p.get("value", {})
            try:
                if "distributionValue" in v:
                    d = v["distributionValue"]
                    if int(d.get("count", 0)) <= 0:
                        continue
                    n = Decimal(str(d.get("mean", 0))) * int(d["count"])
                else:
                    n = Decimal(str(v.get("int64Value", v.get("doubleValue"))))
                if not n.is_finite() or n < 0:
                    continue
            except (KeyError, ValueError, TypeError, InvalidOperation):
                continue
            item["points"] += 1
            item["max_value"] = str(max(n, Decimal(item["max_value"] or "0")))
            if s.get("metricKind") == "DELTA":
                item["delta_sum"] = str(Decimal(item["delta_sum"] or "0") + n)
    return {"metric_type": metric_type, "by_method": totals,
            "measurement_status": "OBSERVED_TELEMETRY_NOT_BILLED_SKU" if any(x["points"] for x in totals.values()) else "UNAVAILABLE_NO_POINTS"}


def summarize_audit(entries):
    seen, counts, sums, byte_records, incomplete_identity = set(), Counter(), Counter(), 0, 0
    for e in entries:
        if all(e.get(k) for k in ("logName", "insertId", "timestamp")):
            ident = (e["logName"], e["insertId"], e["timestamp"])
        else:
            incomplete_identity += 1
            ident = hashlib.sha256(json.dumps(e, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if ident in seen:
            continue
        seen.add(ident)
        p = e.get("protoPayload", {})
        method = safe(p.get("methodName")) or "unclassified"
        counts[method] += 1
        # Only this documented, per-operation path is summed; arbitrary metadata
        # estimates and repeated nested counters are deliberately excluded.
        d = p.get("metadata", {}).get("tableDataRead", {})
        found = False
        for key in BYTE_KEYS:
            value = d.get(key)
            if isinstance(value, (int, str)) and not isinstance(value, bool) and NUMERIC.fullmatch(str(value)):
                sums[key] += int(value)
                found = True
        byte_records += int(found)
    return {"entries": len(seen), "methods": dict(counts), "entries_with_byte_fields": byte_records,
            "observations_missing_complete_log_identity": incomplete_identity,
            "documented_numeric_field_sums": dict(sums),
            "attribution": "Consumer project and time window only; no Glue trace correlation established",
            "measurement_status": "AUDIT_BYTES_FOUND_REQUIRES_ATTRIBUTION" if byte_records else "UNAVAILABLE_NO_BYTE_FIELDS"}


def summarize_table_metadata(row):
    result = {}
    for key in ("numRows", "numBytes", "numLongTermBytes", "numTotalLogicalBytes", "numActiveLogicalBytes",
                "lastModifiedTime", "creationTime"):
        value = row.get(key)
        if isinstance(value, (int, str)) and not isinstance(value, bool) and NUMERIC.fullmatch(str(value)):
            result[key] = str(value)
    if row.get("type") in {"TABLE", "VIEW", "EXTERNAL", "MATERIALIZED_VIEW", "SNAPSHOT", "CLONE"}:
        result["type"] = row["type"]
    if isinstance(row.get("location"), str) and re.fullmatch(r"[A-Za-z0-9-]{2,50}", row["location"]):
        result["location"] = row["location"]
    if isinstance(row.get("schema"), dict):
        schema = row["schema"]
        result["top_level_column_count"] = len(schema.get("fields", []))
        result["schema_sha256"] = hashlib.sha256(json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if isinstance(row.get("streamingBuffer"), dict):
        result["streamingBuffer"] = {k: str(v) for k, v in row["streamingBuffer"].items()
                                     if k in {"estimatedBytes", "estimatedRows", "oldestEntryTime"}
                                     and isinstance(v, (int, str)) and NUMERIC.fullmatch(str(v))}
    return result


def collect(api, start=START, end=END, source_metadata=False):
    valid_window(start, end)
    report = {"schema_version": 1, "project": PROJECT, "read_only": True,
              "window": {"start": start, "end": end}, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "starts_sql_queries": False, "starts_storage_read_sessions": False,
              "changes_billing_apis_iam": False, "errors": []}
    project, error = api.request(f"https://cloudresourcemanager.googleapis.com/v1/projects/{PROJECT}",
                                 {"fields": "projectId,projectNumber,lifecycleState"})
    if error or not project or project.get("projectId") != PROJECT:
        report["errors"].append({"operation": "project_identity", "error": error or {"identity_mismatch": True}})
        report["measurement_status"] = "BLOCKED_PROJECT_IDENTITY"
        return report
    report["project_metadata"] = {k: project[k] for k in ("projectId", "projectNumber", "lifecycleState") if k in project}
    billing, error = api.request(f"https://cloudbilling.googleapis.com/v1/projects/{PROJECT}/billingInfo",
                                 {"fields": "projectId,billingEnabled"})
    report["billing_metadata"] = billing if not error else {"error": error}
    perms, error = api.request(f"https://cloudresourcemanager.googleapis.com/v1/projects/{PROJECT}:testIamPermissions",
                               body={"permissions": ["logging.logEntries.list", "logging.privateLogEntries.list",
                                                     "logging.views.access", "monitoring.metricDescriptors.list",
                                                     "monitoring.timeSeries.list", "serviceusage.quotas.get"]})
    report["effective_permissions"] = perms.get("permissions", []) if not error else {"error": error}

    quotas, meta = pages(api, f"https://serviceusage.googleapis.com/v1beta1/projects/{project['projectNumber']}/services/{SERVICE}/consumerQuotaMetrics",
                         "metrics", {"view": "FULL", "pageSize": 100})
    report["quota_metadata"] = {**meta, "metric_count": len(quotas), "metrics": []}
    byte_quotas = []
    for q in quotas:
        units = sorted({safe(x.get("unit")) for x in q.get("consumerQuotaLimits", []) if safe(x.get("unit"))})
        metric = safe(q.get("metric"))
        if metric:
            report["quota_metadata"]["metrics"].append({"metric": metric, "units": units})
            if any("By" in u or "byte" in u.lower() for u in units) and re.search(r"read|scan", metric, re.I):
                byte_quotas.append(metric)

    descriptors, monitor_errors, catalog_complete = [], [], True
    for prefix in ("bigquerystorage.googleapis.com/", "logging.googleapis.com/user/", "serviceruntime.googleapis.com/api/"):
        values, meta = pages(api, f"https://monitoring.googleapis.com/v3/projects/{PROJECT}/metricDescriptors",
                             "metricDescriptors", {"filter": f'metric.type = starts_with("{prefix}")',
                                                   "activeOnly": "false", "pageSize": 100})
        descriptors.extend(values)
        monitor_errors.extend(meta["errors"])
        catalog_complete = catalog_complete and meta["complete"]
        if meta["errors"] and any(e.get("billing_required") for e in meta["errors"]):
            break  # Do not retry the same billing barrier for every metric.
    candidates = []
    wanted = {"serviceruntime.googleapis.com/api/response_sizes", "serviceruntime.googleapis.com/api/request_count"}
    for d in descriptors:
        typ, unit = safe(d.get("type")), safe(d.get("unit"))
        if not typ:
            continue
        read_bytes = bool(re.search(r"read|scan", typ, re.I) and re.search(r"byte", typ, re.I)
                          and unit and ("By" in unit or unit == "B"))
        if typ.startswith("bigquerystorage.googleapis.com/") or read_bytes or typ in wanted:
            candidates.append({"type": typ, "unit": unit, "kind": safe(d.get("metricKind")),
                               "value_type": safe(d.get("valueType")), "is_read_byte_candidate": read_bytes})
    report["metric_catalog"] = {"descriptors_seen": len(descriptors), "candidates": candidates[:30],
                                 "truncated": len(candidates) > 30, "errors": monitor_errors,
                                 "complete": catalog_complete, "active_only": False}
    report["time_series"] = []
    if not monitor_errors:
        chosen = [d for d in candidates if d["type"] in wanted or d["is_read_byte_candidate"]][:6]
        time_series_blocked = False
        for d in chosen:
            filt = f'metric.type="{d["type"]}"'
            if d["type"].startswith("serviceruntime.googleapis.com/api/"):
                filt += f' AND resource.type="consumed_api" AND resource.labels.service="{SERVICE}"'
            values, meta = pages(api, f"https://monitoring.googleapis.com/v3/projects/{PROJECT}/timeSeries", "timeSeries",
                                 {"filter": filt, "interval.startTime": start, "interval.endTime": end,
                                  "view": "FULL", "pageSize": 1000})
            report["time_series"].append({**summarize_series(values, d["type"]), **meta})
            if any(e.get("billing_required") for e in meta["errors"]):
                time_series_blocked = True
                break
        # Quota limits are metadata, not bytes used. A history is inspected only
        # when the service itself declares a read/scan quota with byte units.
        for metric in ([] if time_series_blocked else byte_quotas[:2]):
            filt = (f'metric.type="serviceruntime.googleapis.com/quota/rate/net_usage" AND resource.type="consumer_quota" '
                    f'AND resource.labels.service="{SERVICE}" AND metric.labels.quota_metric="{metric}"')
            values, meta = pages(api, f"https://monitoring.googleapis.com/v3/projects/{PROJECT}/timeSeries", "timeSeries",
                                 {"filter": filt, "interval.startTime": start, "interval.endTime": end,
                                  "view": "FULL", "pageSize": 1000})
            report["time_series"].append({**summarize_series(values, "serviceruntime.googleapis.com/quota/rate/net_usage"),
                                           "quota_metric": metric, **meta})
            if any(e.get("billing_required") for e in meta["errors"]):
                break

    sinks, meta = pages(api, f"https://logging.googleapis.com/v2/projects/{PROJECT}/sinks", "sinks", {"pageSize": 100})
    report["existing_log_routes"] = {**meta, "count": len(sinks), "destination_types": dict(Counter(
        "logging" if s.get("destination", "").startswith("logging.googleapis.com/") else
        "storage" if s.get("destination", "").startswith("storage.googleapis.com/") else
        "bigquery" if s.get("destination", "").startswith("bigquery.googleapis.com/") else "other" for s in sinks)),
        "disabled_count": sum(bool(s.get("disabled")) for s in sinks),
        "exclusion_count": sum(len(s.get("exclusions", [])) for s in sinks)}
    buckets, meta = pages(api, f"https://logging.googleapis.com/v2/projects/{PROJECT}/locations/-/buckets", "buckets", {"pageSize": 100})
    report["existing_log_buckets"] = {**meta, "count": len(buckets), "retention_days": sorted({
        int(b["retentionDays"]) for b in buckets if isinstance(b.get("retentionDays"), int)}),
        "restricted_field_count": sum(len(b.get("restrictedFields", [])) for b in buckets)}

    filt = (f'timestamp >= "{start}" timestamp < "{end}" '
            '(protoPayload.serviceName="bigquerystorage.googleapis.com" OR protoPayload.serviceName="bigquery.googleapis.com") '
            '(protoPayload.methodName:"ReadRows" OR protoPayload.metadata.tableDataRead.reason="READ_ROWS" '
            'OR protoPayload.metadata.tableDataRead.scannedBytes:* OR protoPayload.metadata.tableDataRead.serializedResponseBytes:*)')
    audit, meta = pages(api, "https://logging.googleapis.com/v2/entries:list", "entries", body={
        "resourceNames": [f"projects/{PROJECT}"], "filter": filt, "orderBy": "timestamp asc", "pageSize": 1000})
    report["audit"] = {**summarize_audit(audit), **meta}
    if source_metadata:
        tables = []
        fields = "type,numRows,numBytes,numLongTermBytes,numTotalLogicalBytes,numActiveLogicalBytes,lastModifiedTime,creationTime,location,schema,streamingBuffer(estimatedBytes,estimatedRows,oldestEntryTime)"
        for table in TABLES:
            row, error = api.request("https://bigquery.googleapis.com/bigquery/v2/projects/basedosdados/datasets/br_inep_avaliacao_alfabetizacao/tables/" + table,
                                     {"fields": fields})
            tables.append({"table": table, "metadata": summarize_table_metadata(row) if not error else None, "error": error})
        report["source_table_metadata"] = {"tables": tables, "purpose": "Current logical capacity for a prospective full-table-read scenario; not historical Storage Read or network egress bytes"}
    report["api_calls"] = api.calls
    report["measurement_status"] = "COLLECTED_EXISTING_EVIDENCE_REQUIRES_SEMANTIC_REVIEW"
    report["limitations"] = ["billingEnabled is current metadata, not historical cost or bytes.",
        "Empty metrics/logs, denied calls and quota limits are not zero usage.",
        "API response sizes are observed telemetry, not proof of billed GCP egress SKU.",
        "Consumer-project audit scope may omit table-owner Data Access logs.",
        "No raw audit records, tokens, email addresses, credential IDs, SQL or sink destinations were exported."]
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--start", default=START)
    parser.add_argument("--end", default=END)
    parser.add_argument("--source-metadata", action="store_true", help="Also GET metadata for the seven public source tables; no rows/query")
    args = parser.parse_args()
    valid_window(args.start, args.end)
    target = Path(args.output)
    if target.exists() or not target.parent.is_dir():
        raise SystemExit("Output must be a new file in an existing directory")
    auth = subprocess.run(["gcloud", "auth", "print-access-token", "--quiet"], capture_output=True, text=True, timeout=60)
    if auth.returncode or not auth.stdout.strip():
        raise SystemExit("Existing gcloud token unavailable; no login or authorization was attempted by this script")
    report = collect(ReadOnlyAPI(auth.stdout.strip()), args.start, args.end, args.source_metadata)
    encoded = json.dumps(report, ensure_ascii=False, separators=(",", ":")).encode()
    if len(encoded) > MAX_OUTPUT:
        raise SystemExit("Safe output exceeds cap; narrow interval/catalog before writing")
    target.write_bytes(encoded+b"\n")
    print(json.dumps({"output": str(target), "sha256": hashlib.sha256(encoded+b"\n").hexdigest(),
                      "bytes": len(encoded)+1, "status": report["measurement_status"]}))


if __name__ == "__main__":
    main()
