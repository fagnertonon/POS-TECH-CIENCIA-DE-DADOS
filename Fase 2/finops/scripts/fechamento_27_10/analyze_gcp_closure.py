"""Reconcile saved metadata/manifest and calculate a prospective Storage Read model.

Local files only. Does not call any cloud API or turn telemetry into billed egress.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, getcontext
import hashlib
import json
from pathlib import Path

getcontext().prec = 40
TABLES = ("alunos", "municipio", "uf", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf",
          "meta_alfabetizacao_municipio", "dicionario")
RUN = "jr_cdfd2933220ec75b45c04518c6e5afe93eb98915f3b88328f8bde8d7183d3700"
GCP_SHA256 = "517a120707338fcb9d0443a69e346aec1710b77b56d6b1dfbc7141539b56a642"
PRICING_URL = "https://cloud.google.com/bigquery/pricing#data_extraction_pricing"
TABLES_URL = "https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables"
METRIC_URL = "https://docs.cloud.google.com/apis/docs/monitoring"
AUDIT_URL = "https://docs.cloud.google.com/bigquery/docs/reference/storage#monitor_storage_read_api_use"


def utc(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Timestamp requires timezone")
    return dt.astimezone(timezone.utc)


def exact_int(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).isdigit():
        raise ValueError("Expected a nonnegative integer")
    return int(value)


def by_table(rows, key):
    if len(rows) != len(TABLES) or {r.get(key) for r in rows} != set(TABLES):
        raise ValueError("Exactly the seven unique source tables are required")
    return {r[key]: r for r in rows}


def analyze(gcp, bronze, cycles=4):
    if not isinstance(cycles, int) or isinstance(cycles, bool) or not 0 <= cycles <= 31:
        raise ValueError("Cycles must be an integer from 0 to 31")
    if gcp.get("project_metadata", {}).get("projectId") != "fiap1-512214":
        raise ValueError("Unexpected GCP project")
    if bronze.get("run_id") != RUN or bronze.get("status") != "SUCCEEDED" or not bronze.get("is_full_batch"):
        raise ValueError("Expected the successful complete Bronze reference")
    source = by_table(gcp["source_table_metadata"]["tables"], "table")
    target = by_table(bronze["table_metrics"], "table")
    start = utc(bronze["started_at_utc"])
    comparisons, total_bytes, total_rows, parquet_bytes = [], 0, 0, 0
    for table in TABLES:
        s, b = source[table], target[table]
        if s.get("error") or not isinstance(s.get("metadata"), dict):
            raise ValueError("Missing table metadata")
        m = s["metadata"]
        logical = exact_int(m["numTotalLogicalBytes"] if "numTotalLogicalBytes" in m else m["numBytes"])
        rows, cols = exact_int(m["numRows"]), exact_int(m["top_level_column_count"])
        modified = datetime.fromtimestamp(exact_int(m["lastModifiedTime"])/1000, timezone.utc)
        checks = {"type_is_table": m.get("type") == "TABLE", "source_location_us": m.get("location") == "US",
                  "rows_match_bronze_source": rows == exact_int(b["source_count"]),
                  "columns_match_bronze_source": cols == exact_int(b["source_column_count"]),
                  "bronze_source_matches_written_rows": exact_int(b["source_count"]) == exact_int(b["target_count"]),
                  "last_modified_before_bronze": modified <= start,
                  "logical_fields_agree": "numBytes" not in m or logical == exact_int(m["numBytes"]),
                  "no_streaming_buffer_reported": not bool(m.get("streamingBuffer"))}
        comparisons.append({"table": table, "measured_logical_bytes": logical, "metadata_rows": rows,
                            "bronze_source_rows": b["source_count"], "metadata_column_count": cols,
                            "bronze_source_column_count": b["source_column_count"],
                            "last_modified_utc": modified.isoformat(), "schema_sha256_current": m.get("schema_sha256"),
                            "checks": checks, "compatible": all(checks.values())})
        total_bytes += logical
        total_rows += rows
        parquet_bytes += exact_int(b["parquet_bytes"])
    compatible = all(x["compatible"] for x in comparisons)
    tib = Decimal(total_bytes) / Decimal(2**40)
    rate = Decimal("1.10")
    per_cycle_cost = tib*rate
    telemetry = {}
    for series in gcp.get("time_series", []):
        for method, summary in series.get("by_method", {}).items():
            if method.endswith((".ReadRows", ".CreateReadSession")):
                telemetry.setdefault(method.rsplit(".", 1)[-1], {})[
                    "api_response_sizes_distribution_sum_bytes" if series["metric_type"].endswith("response_sizes")
                    else "completed_request_count"] = summary.get("delta_sum")
    return {
        "schema_version": 1,
        "status": "STORAGE_READ_PROSPECTIVE_LINE_CLOSABLE" if compatible else "METADATA_RECONCILIATION_FAILED",
        "source_observation_utc": gcp.get("observed_at_utc"), "cloud_api_calls_by_this_analyzer": 0,
        "latest_collection": {"api_calls": gcp.get("api_calls"), "top_level_errors": gcp.get("errors"),
                              "monitoring_now_available": not gcp["metric_catalog"]["errors"] and bool(gcp.get("time_series")),
                              "billing_enabled_current": gcp["billing_metadata"].get("billingEnabled"),
                              "billing_changed_by_collection": gcp.get("changes_billing_apis_iam"),
                              "quota_metrics_seen": gcp["quota_metadata"].get("metric_count"),
                              "metric_descriptors_seen": gcp["metric_catalog"].get("descriptors_seen"),
                              "audit_entries": gcp["audit"].get("entries")},
        "collection_bounds": {"api_call_cap": 40, "pages_per_operation_cap": 3,
                              "response_size_cap_bytes": 2097152, "safe_output_size_cap_bytes": 20000,
                              "catalog_complete": gcp["metric_catalog"].get("complete"),
                              "catalog_truncated": gcp["metric_catalog"].get("truncated"),
                              "timeseries_pages_complete": all(s.get("complete") for s in gcp.get("time_series", [])),
                              "audit_pages_complete": gcp["audit"].get("complete")},
        "reconciliation": {"all_seven_compatible": compatible, "total_source_rows": total_rows,
                           "logical_bytes": total_bytes, "decimal_MB": str(Decimal(total_bytes)/10**6),
                           "GiB": str(Decimal(total_bytes)/2**30), "MiB": str(Decimal(total_bytes)/2**20),
                           "tables": comparisons,
                           "schema_limit": "Current BigQuery schema hashes are baselines; Spark and BigQuery schema serializations differ, so historical schema-hash equality was not asserted."},
        "storage_read_projection": {
            "close_projection_line": compatible,
            "quantity_label": "Measured current logical table capacity used in a prospective full-read model",
            "measurement_type": "TABLE_METADATA_NOT_HISTORICAL_BILLED_STORAGE_READ",
            "measured_logical_bytes_per_full_read": total_bytes, "modeled_full_reads_per_cycle": 1,
            "cycles_30d": cycles, "cadence_is_assumption_not_schedule_or_assignment_requirement": True,
            "modeled_TiB_per_cycle": str(tib), "modeled_TiB_30d": str(tib*cycles),
            "price_usd_per_TiB": str(rate), "gross_usd_per_cycle": str(per_cycle_cost) if compatible else None,
            "gross_usd_30d": str(per_cycle_cost*cycles) if compatible else None,
            "free_read_tier_applied": False,
            "free_tier_note": "The documented 300 TiB/month per billing account is not relied upon; remaining allowance/account eligibility was not measured. It does not cover network egress.",
            "historical_actual_scanned_bytes": None,
            "assumptions": ["Same seven tables, rows and all source columns as the measured snapshot.",
                            "One complete source read per cycle; extra reads, retries and previews are outside this modeled factor.",
                            "Bronze sourceType=table has no source column projection or row filter; MEMORY_AND_DISK caching precedes count and write.",
                            "A future source update requires refreshing this metadata measurement.",
                            "Published USD reference tariff, no discounts/free-tier assumption; current billing metadata is not evidence of historical cost."]},
        "api_telemetry": {"window": gcp["window"], "methods": telemetry,
                          "attribution": "Consumer project and 10 minute window; contains the batch and previews, not an isolated Glue run.",
                          "metric_semantics": "Public docs define response size distribution recorded at request completion; do not specify that ReadRows observations sum all streaming response messages or equal billable network bytes.",
                          "last_message_only_hypothesis_proven": False,
                          "numeric_note": "Distribution mean multiplied by count is approximate telemetry; 43649.9999999999994 may display as 43650 B without asserting wire-byte precision."},
        "egress": {"status": "PENDING_BILLED_EGRESS_QUANTITY", "billed_network_bytes": None,
                   "projected_egress_usd_30d": None, "valid_upper_bound_established": False,
                   "reason": "API response_sizes lacks documented streaming-total/SKU equivalence and batch attribution. Logical table bytes and compressed S3 Parquet bytes use different representations and cannot substitute for egress.",
                   "diagnostic_reference_only": {"bronze_parquet_bytes": parquet_bytes,
                                                 "api_readrows_response_sizes_bytes": telemetry.get("ReadRows", {}).get("api_response_sizes_distribution_sum_bytes")},
                   "recommended_extra_timeseries_get": False,
                   "extra_get_limit": "Narrower points/counts can inspect attribution, but cannot establish undocumented streaming or billing semantics; repeating it will not close the egress line.",
                   "required_evidence": "ReadRows audited serializedResponseBytes correlated to this extraction, or historical billed network SKU quantity. No new extraction/read session is recommended."},
        "effect_on_pending_lines": {"storage_read_projected": "closed_as_measured_prospective_estimate" if compatible else "pending",
                                    "gcp_egress": "pending", "log_archive_growth": "outside_this_analysis",
                                    "parent_three_pending_lines_can_reduce_to_two": compatible},
        "official_sources": {"pricing": PRICING_URL, "table_metadata": TABLES_URL,
                             "api_metric_semantics": METRIC_URL, "storage_read_audit_bytes": AUDIT_URL},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gcp", required=True)
    parser.add_argument("--bronze", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cycles", type=int, default=4)
    args = parser.parse_args()
    gcp_path, bronze_path, out = map(Path, (args.gcp, args.bronze, args.output))
    if out.exists() or not out.parent.is_dir():
        raise SystemExit("Output must be new and its parent must already exist")
    raw_gcp, raw_bronze = gcp_path.read_bytes(), bronze_path.read_bytes()
    digest = hashlib.sha256(raw_gcp).hexdigest()
    if digest != GCP_SHA256:
        raise SystemExit("GCP evidence SHA256 differs from the reviewed collection")
    result = analyze(json.loads(raw_gcp), json.loads(raw_bronze), args.cycles)
    result["inputs"] = {"gcp_sha256": digest, "gcp_bytes": len(raw_gcp), "bronze_manifest_sha256": hashlib.sha256(raw_bronze).hexdigest(),
                        "gcp_file": str(gcp_path), "bronze_manifest_file": str(bronze_path)}
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(json.dumps({"output": str(out), "status": result["status"],
                      "logical_bytes": result["reconciliation"]["logical_bytes"],
                      "storage_read_gross_30d_usd": result["storage_read_projection"]["gross_usd_30d"]}))


if __name__ == "__main__":
    main()
