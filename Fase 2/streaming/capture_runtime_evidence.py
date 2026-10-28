"""Read-only operational evidence for one new simulated streaming exercise.

No messages are received/sent/deleted, no function code URL is requested, and no
resource, schedule or job is created. Billing reconciliation remains a later task.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re

import lambda_function as consumer


FUNCTION = "fiap-inep-streaming-consumer"
QUEUE = "fiap-inep-streaming"
DLQ = "fiap-inep-streaming-dlq"
ALARMS = ["fiap-inep-streaming-dlq", "fiap-inep-streaming-rejected", "fiap-inep-streaming-age"]
REQUEST = re.compile(r"\b(?:START|END|REPORT) RequestId:\s*([A-Za-z0-9_-]+)")


def stamp(value):
    if not isinstance(value, str) or not consumer.UTC_TIMESTAMP.fullmatch(value):
        raise ValueError("Expected UTC ISO timestamp")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def collect_log_events(logs, params, *, max_pages=30):
    """Bounded pagination with duplicate-event suppression, preserving AWS order."""
    results, seen, token = [], set(), None
    for _ in range(max_pages):
        page_params = dict(params)
        if token:
            page_params["nextToken"] = token
        page = logs.filter_log_events(**page_params)
        for event in page.get("events", []):
            identity = event.get("eventId") or (event.get("logStreamName"), event.get("timestamp"), event.get("message"))
            if identity not in seen:
                seen.add(identity)
                results.append(event)
        following = page.get("nextToken")
        if not following or following == token:
            return results, True
        token = following
    return results, False


def parse_report(message):
    if not message.startswith("REPORT RequestId:"):
        return None
    match = REQUEST.search(message)
    if not match:
        return None
    result = {"request_id": match.group(1)}
    fields = {"duration_ms": ("Duration", "ms"), "billed_duration_ms": ("Billed Duration", "ms"),
              "memory_size_mb": ("Memory Size", "MB"), "max_memory_used_mb": ("Max Memory Used", "MB"),
              "init_duration_ms": ("Init Duration", "ms")}
    for name, (label, unit) in fields.items():
        found = re.search(r"(?:^|\s)" + re.escape(label) + r":\s*(\d+(?:\.\d+)?)\s*" + unit + r"\b", message)
        if found:
            result[name] = float(found.group(1))
    status = re.search(r"\bStatus:\s*(\S+)", message)
    if status:
        result["runtime_status"] = status.group(1)
    return result


def summarize_scoped_logs(events, run_id, invalid_message_id):
    """Attribute REPORT only through START/current request in the SAME stream.

    An invocation containing both target and unrelated records is disclosed as
    mixed and excluded from exclusive totals; a duration is never prorated.
    """
    active, requests = {}, {}
    positive_events, invalid_events, unassigned = [], [], []
    ordered = sorted(enumerate(events), key=lambda pair: (pair[1].get("timestamp", 0), pair[0]))
    for _index, item in ordered:
        message = item.get("message", "").strip()
        stream = item.get("logStreamName")
        found = REQUEST.search(message)
        if message.startswith("START RequestId:") and found and stream:
            request_id = found.group(1)
            active[stream] = request_id
            requests.setdefault((stream, request_id), {"request_id": request_id, "log_stream": stream, "record_scopes": []})
            continue
        report = parse_report(message)
        if report and stream:
            slot = requests.setdefault((stream, report["request_id"]),
                                       {"request_id": report["request_id"], "log_stream": stream, "record_scopes": []})
            slot["report"] = report
            continue
        if message.startswith("END RequestId:") and found and stream:
            if active.get(stream) == found.group(1):
                active.pop(stream, None)
            continue
        try:
            value = json.loads(message)
        except (ValueError, TypeError):
            continue
        if not isinstance(value, dict) or value.get("kind") != "STREAMING_EVENT":
            continue
        scope = "positive_run" if value.get("run_id") == run_id else "invalid_message" if value.get("sqs_message_id") == invalid_message_id else "unrelated"
        entry = {"timestamp": item.get("timestamp"), "event_id": value.get("event_id"),
                 "sqs_message_id": value.get("sqs_message_id"), "status": value.get("status"),
                 "error_type": value.get("error_type")}
        if scope == "positive_run":
            positive_events.append(entry)
        elif scope == "invalid_message":
            invalid_events.append(entry)
        request_id = active.get(stream) if stream else None
        if request_id:
            requests[(stream, request_id)]["record_scopes"].append(scope)
        elif scope != "unrelated":
            unassigned.append(dict(entry, scope=scope, reason="START/request context unavailable; REPORT not attributed"))
    related, exclusive = [], []
    for value in requests.values():
        scopes = set(value["record_scopes"])
        if not scopes.intersection({"positive_run", "invalid_message"}):
            continue
        value["scope_counts"] = dict(Counter(value.pop("record_scopes")))
        value["contains_unrelated_records"] = "unrelated" in scopes
        value["report_metrics_complete"] = {"duration_ms", "billed_duration_ms", "memory_size_mb", "max_memory_used_mb"}.issubset(value.get("report", {}))
        related.append(value)
        if not value["contains_unrelated_records"] and value["report_metrics_complete"]:
            exclusive.append(value["report"])
    totals = {"invocations_with_report": len(exclusive),
              "duration_ms_sum": sum(row["duration_ms"] for row in exclusive) if exclusive else None,
              "billed_duration_ms_sum": sum(row["billed_duration_ms"] for row in exclusive) if exclusive else None,
              "max_memory_used_mb": max((row.get("max_memory_used_mb", 0) for row in exclusive), default=None),
              "configured_mb_seconds_from_billed_reports": sum(row["billed_duration_ms"] / 1000 * row["memory_size_mb"] for row in exclusive) if exclusive else None}
    return {"positive_run": {"events": positive_events, "status_counts": dict(Counter(e["status"] for e in positive_events))},
            "invalid_message": {"events": invalid_events, "status_counts": dict(Counter(e["status"] for e in invalid_events)),
                                "invalid_payload_failures": sum(e["status"] == "FAILED" and e["error_type"] == "InvalidPayload" for e in invalid_events)},
            "correlated_invocations": sorted(related, key=lambda item: (item["log_stream"], item["request_id"])),
            "unassigned_target_records": unassigned, "exclusive_target_invocation_totals": totals,
            "attribution_note": "REPORT totals include only START-correlated invocations containing target records and no unrelated STREAMING_EVENT records. Mixed invocations are listed but excluded; no cost allocation or monthly extrapolation is made."}


def metric_window(cw, namespace, name, dimensions, start, end):
    result = cw.get_metric_statistics(Namespace=namespace, MetricName=name, Dimensions=dimensions,
                                      StartTime=start, EndTime=end, Period=60,
                                      Statistics=["Sum", "SampleCount", "Minimum", "Maximum", "Average"])
    return {"namespace": namespace, "metric_name": name, "dimensions": dimensions,
            "scope": "shared function/pipeline/queue and time window; NOT exclusively attributable to this run",
            "datapoints": sorted(result.get("Datapoints", []), key=lambda point: point["Timestamp"])}


def capture(session, exercise_dir):
    folder = Path(exercise_dir)
    metadata = json.loads((folder / "exercise_metadata.json").read_text(encoding="utf-8"))
    producer_report = json.loads((folder / "producer" / "producer_report.json").read_text(encoding="utf-8"))
    invalid = json.loads((folder / "invalid_semantic.json").read_text(encoding="utf-8"))
    run_id, invalid_id = metadata["run_id"], invalid["message_id"]
    if not all(isinstance(value, str) and consumer.SAFE_ID.fullmatch(value) for value in (run_id, invalid_id)):
        raise ValueError("Unsafe run/message identifier")
    if producer_report["run_id"] != run_id:
        raise ValueError("Producer and exercise refer to different runs")
    start = min(stamp(metadata["started_at_utc"]), stamp(producer_report["started_at_utc"]), stamp(invalid["event"]["event_time"])) - timedelta(seconds=5)
    end = datetime.now(timezone.utc)
    report = {"captured_at_utc": consumer.iso_utc(end), "run_id": run_id, "invalid_message_id": invalid_id,
              "scope": "operational evidence for this demand; FinOps reconciliation is deferred",
              "window_start_utc": consumer.iso_utc(start), "window_end_utc": consumer.iso_utc(end),
              "pending": [], "configuration_deviations": [], "metrics_window_context": []}
    logs, lamb, cw, sqs = (session.client(name) for name in ("logs", "lambda", "cloudwatch", "sqs"))
    base = {"logGroupName": "/aws/lambda/" + FUNCTION, "startTime": int(start.timestamp() * 1000), "endTime": int(end.timestamp() * 1000)}
    try:
        positive, positive_complete = collect_log_events(logs, dict(base, filterPattern='{ $.run_id = "' + run_id + '" }'))
        negative, negative_complete = collect_log_events(logs, dict(base, filterPattern='{ $.sqs_message_id = "' + invalid_id + '" }'))
        streams = sorted({row["logStreamName"] for row in positive + negative if row.get("logStreamName")})
        context, complete = collect_log_events(logs, dict(base, logStreamNames=streams)) if streams else ([], True)
        report["logs"] = summarize_scoped_logs(context, run_id, invalid_id)
        report["logs"]["query_pagination_complete"] = positive_complete and negative_complete and complete
        report["logs"]["discovered_positive_records"] = len(positive)
        report["logs"]["discovered_invalid_records"] = len(negative)
        report["logs"]["context_stream_count"] = len(streams)
        observed = report["logs"]
        if (observed["positive_run"]["status_counts"].get("ACCEPTED", 0) < producer_report["unique_event_count"]
                or observed["positive_run"]["status_counts"].get("DUPLICATE", 0) < producer_report["duplicate_send_count"]
                or observed["invalid_message"]["invalid_payload_failures"] < invalid.get("expected_dlq_after_max_receive_count", 5)
                or not observed["query_pagination_complete"] or observed["unassigned_target_records"]
                or any(not item["report_metrics_complete"] for item in observed["correlated_invocations"])):
            report["pending"].append("Target logs or request correlation incomplete; CloudWatch observations can arrive asynchronously")
    except Exception as exc:
        report["pending"].append({"logs_unavailable": consumer._code(exc) or type(exc).__name__})
    try:
        cfg = lamb.get_function_configuration(FunctionName=FUNCTION)
        report["lambda_configuration"] = {name: cfg.get(name) for name in (
            "FunctionName", "Runtime", "MemorySize", "Timeout", "State", "LastUpdateStatus", "LastModified", "CodeSha256")}
        report["lambda_concurrency"] = lamb.get_function_concurrency(FunctionName=FUNCTION)
        mappings = lamb.list_event_source_mappings(FunctionName=FUNCTION).get("EventSourceMappings", [])
        report["event_source_mappings"] = [{name: row.get(name) for name in (
            "UUID", "State", "EventSourceArn", "BatchSize", "FunctionResponseTypes", "ScalingConfig")} for row in mappings]
        if cfg.get("MemorySize") != 256 or cfg.get("Timeout") != 10 or report["lambda_concurrency"].get("ReservedConcurrentExecutions") != 2:
            report["configuration_deviations"].append("Expected 256MB, timeout10s and reserved concurrency2")
    except Exception as exc:
        report["pending"].append({"lambda_configuration_unavailable": consumer._code(exc) or type(exc).__name__})
    report["queues"] = {}
    for name in (QUEUE, DLQ):
        try:
            url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
            attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn", "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed", "VisibilityTimeout", "MessageRetentionPeriod", "RedrivePolicy"])["Attributes"]
            report["queues"][name] = {"scope": "shared queue, counts are not exclusive to this run", "attributes": attrs}
        except Exception as exc:
            report["pending"].append({"queue_unavailable": name, "error_type": consumer._code(exc) or type(exc).__name__})
    try:
        result = cw.describe_alarms(AlarmNames=ALARMS)
        report["alarms"] = [{name: row.get(name) for name in ("AlarmName", "StateValue", "StateUpdatedTimestamp", "StateReason",
            "ActionsEnabled", "AlarmActions", "Namespace", "MetricName", "Period", "EvaluationPeriods", "Threshold")}
            for row in result.get("MetricAlarms", [])]
        missing_alarms = sorted(set(ALARMS) - {item["AlarmName"] for item in report["alarms"]})
        if missing_alarms:
            report["pending"].append({"expected_alarms_not_observed": missing_alarms})
    except Exception as exc:
        report["pending"].append({"alarms_unavailable": consumer._code(exc) or type(exc).__name__})
    specifications = [("AWS/Lambda", name, [{"Name": "FunctionName", "Value": FUNCTION}])
                      for name in ("Invocations", "Errors", "Duration", "Throttles", "ConcurrentExecutions")]
    specifications += [("FIAP/Streaming", name, [{"Name": "Pipeline", "Value": "fiap-inep-streaming"}])
                       for name in ("Received", "Accepted", "Duplicate", "Failed", "LatencySeconds")]
    specifications += [("AWS/SQS", name, [{"Name": "QueueName", "Value": QUEUE}])
                       for name in ("NumberOfMessagesSent", "NumberOfMessagesReceived", "NumberOfMessagesDeleted", "ApproximateAgeOfOldestMessage")]
    for namespace, name, dimensions in specifications:
        try:
            report["metrics_window_context"].append(metric_window(cw, namespace, name, dimensions, start, end))
        except Exception as exc:
            report["pending"].append({"metric_unavailable": namespace + "/" + name, "error_type": consumer._code(exc) or type(exc).__name__})
    report["capture_status"] = "CONFIGURATION_DEVIATION" if report["configuration_deviations"] else "INCOMPLETE" if report["pending"] else "COMPLETE"
    return report


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exercise-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--region", default="us-east-2")
    args = parser.parse_args(argv)
    import boto3
    result = capture(boto3.Session(region_name=args.region), args.exercise_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=json_default, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"capture_status": result["capture_status"], "run_id": result["run_id"], "report": str(args.output)}))
    return 0 if result["capture_status"] == "COMPLETE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
