"""Verify one controlled semantic rejection without deleting or redriving data.

S3/log reads are scoped to the supplied identity. DLQ inspection temporarily holds
up to 30 messages and restores every receipt's visibility. No message is sent.
"""
from __future__ import annotations

import argparse
from datetime import timedelta
import hashlib
import json
from pathlib import Path

import lambda_function as consumer
import verify_simulation as positive


def inspect_dlq(sqs, queue_url, expected_id, envelope):
    receipts, inspected, restore_errors = [], [], []
    found, receive_error = None, None
    try:
        for _ in range(3):
            messages = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                VisibilityTimeout=30, AttributeNames=["All"], MessageAttributeNames=["All"],
            ).get("Messages", [])
            for message in messages:
                receipts.append(message["ReceiptHandle"])
                body = message.get("Body", "")
                item = {"message_id": message["MessageId"],
                        "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
                        "attributes": message.get("Attributes", {})}
                inspected.append(item)
                if item["message_id"] == expected_id:
                    try:
                        stored = json.loads(body, parse_constant=consumer._reject_constant)
                        item["event_sha256"] = consumer.sha256(stored)
                        item["matches_original_event"] = item["event_sha256"] == consumer.sha256(envelope)
                    except (ValueError, TypeError):
                        item["matches_original_event"] = False
                    found = item
            if found or not messages:
                break
    except Exception as exc:
        receive_error = consumer._code(exc) or type(exc).__name__
    finally:
        # A failed restoration must not prevent restoration of the other receipts.
        for receipt in receipts:
            try:
                sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=receipt, VisibilityTimeout=0)
            except Exception as exc:
                restore_errors.append(consumer._code(exc) or type(exc).__name__)
    return {"expected_message_id": expected_id, "matched": found, "messages_inspected": inspected,
            "inspection_limit": 30, "deleted_messages": 0, "redriven_messages": 0,
            "visibility_restored": len(receipts) - len(restore_errors),
            "receive_error": receive_error, "restore_errors": restore_errors}


def inspect_logs(logs, function_name, message_id, emitted):
    entries, seen, token = [], set(), None
    params = {"logGroupName": "/aws/lambda/" + function_name,
              "startTime": int((emitted - timedelta(seconds=5)).timestamp() * 1000),
              "endTime": int(consumer.utc_now().timestamp() * 1000),
              "filterPattern": '{ $.sqs_message_id = "' + message_id + '" }'}
    for _ in range(20):
        if token:
            params["nextToken"] = token
        response = logs.filter_log_events(**params)
        for entry in response.get("events", []):
            identity = entry.get("eventId") or (entry.get("timestamp"), entry.get("logStreamName"), entry.get("message"))
            if identity in seen:
                continue
            seen.add(identity)
            try:
                value = json.loads(entry.get("message", ""))
            except (ValueError, TypeError):
                continue
            if isinstance(value, dict) and value.get("kind") == "STREAMING_EVENT" and value.get("sqs_message_id") == message_id:
                entries.append({"timestamp": entry.get("timestamp"), "status": value.get("status"),
                                "error_type": value.get("error_type")})
        next_token = response.get("nextToken")
        if not next_token or next_token == token:
            return {"entries": entries, "pagination_complete": True}
        token = next_token
    return {"entries": entries, "pagination_complete": False}


def verify_negative(s3, sqs, logs, invalid_report, *, bucket="fiap-tc-fase2-2026",
                    queue_url, dlq_url, function_name="fiap-inep-streaming-consumer"):
    report = {"verified_at_utc": consumer.iso_utc(consumer.utc_now()), "simulation": True,
              "pipeline_version": consumer.PIPELINE_VERSION, "scope": "one supplied semantic rejection identity/message",
              "errors": [], "pending": [], "objects": {}}
    require = positive.require
    try:
        event = invalid_report["event"]
        message_id = invalid_report["message_id"]
        config = positive.default_config(bucket)
        envelope, emitted = consumer.parse_event(consumer.canonical_json(event).decode("utf-8"), config)
        require(isinstance(message_id, str) and consumer.SAFE_ID.fullmatch(message_id), "Unsafe message ID")
        require(type(envelope["payload"].get("peso_aluno")) in (int, float) and envelope["payload"]["peso_aluno"] == 0,
                "Expected the controlled zero-weight fixture")
        try:
            consumer.transform_payload(envelope["payload"])
        except consumer.InvalidPayload:
            pass
        else:
            raise ValueError("Input is not a semantic InvalidPayload rejection")
        expected_receives = invalid_report.get("expected_dlq_after_max_receive_count", 5)
        require(type(expected_receives) is int and expected_receives == 5, "Expected maxReceiveCount=5")
        report.update(run_id=envelope["run_id"], event_id=envelope["event_id"], sqs_message_id=message_id,
                      input_validation="InvalidPayload", expected_max_receive_count=expected_receives,
                      event_sha256=consumer.sha256(envelope))
    except (ValueError, KeyError, TypeError) as exc:
        report["errors"].append({"invalid_fixture": str(exc)})
        report["validation_status"] = "FAILED"
        return report
    paths = consumer.stage_paths(envelope, config)
    paths["claim_key"] = f"{config.idempotency_prefix}/run_id={envelope['run_id']}/event_id={envelope['event_id']}.json"
    paths["rejection_key"] = paths["receipt_key"].replace("/event_id=", "/rejected_event_id=")
    objects = {}
    for layer in ("bronze", "claim", "rejection"):
        try:
            objects[layer], report["objects"][layer] = positive.object_json(s3, bucket, paths[layer + "_key"])
        except Exception as exc:
            report["pending"].append({"layer": layer, "reason": "missing" if positive.missing_object(exc)
                                      else consumer._code(exc) or type(exc).__name__})
    try:
        for layer in ("bronze", "claim"):
            if layer not in objects:
                continue
            value = objects[layer]
            require(consumer.sha256(value.get("event")) == consumer.sha256(envelope), layer + " original envelope hash")
            ingestion = value.get("ingestion", {})
            require(ingestion.get("event_sha256") == consumer.sha256(envelope), layer + " event hash declaration")
            require(ingestion.get("payload_sha256") == consumer.sha256(envelope["payload"]), layer + " payload hash")
            require(ingestion.get("target_key") == paths["bronze_key"], layer + " target path")
            positive.timestamp(ingestion["received_at_utc"])
        if "bronze" in objects and "claim" in objects:
            require(consumer.sha256(objects["bronze"]) == consumer.sha256(objects["claim"]), "Claim/Bronze mismatch")
        if "rejection" in objects:
            expected = {"schema_version": 2, "pipeline_version": consumer.PIPELINE_VERSION,
                        "origin": consumer.ORIGIN, "simulation": True, "status": "QUALITY_REJECTED",
                        "event_sha256": consumer.sha256(envelope), "run_id": envelope["run_id"],
                        "event_id": envelope["event_id"], "bronze_key": paths["bronze_key"],
                        "rule": "valid_analytical_payload"}
            require(consumer.sha256(objects["rejection"]) == consumer.sha256(expected), "Quality rejection content/hash")
    except (ValueError, KeyError, TypeError) as exc:
        report["errors"].append({"stored_data": str(exc)})
    for layer, prefix in (("silver", config.silver_prefix), ("gold", config.gold_prefix), ("receipt", config.processed_prefix)):
        try:
            actual = positive.inventory(s3, bucket, prefix, envelope["run_id"])
            token = "/event_id=" + envelope["event_id"] + "."
            unexpected = [key for key in actual if token in key]
            report["objects"][layer] = {"expected_count": 0, "actual_count": len(unexpected), "unexpected_keys": unexpected}
            if unexpected:
                report["errors"].append({"published_after_quality_failure": layer, "keys": unexpected})
        except Exception as exc:
            report["pending"].append({"inventory_unavailable": layer, "error_type": type(exc).__name__})
    report["dlq"] = inspect_dlq(sqs, dlq_url, message_id, envelope)
    found = report["dlq"]["matched"]
    if not found:
        report["pending"].append("Expected message not yet observed in bounded DLQ inspection")
    elif not found["matches_original_event"]:
        report["errors"].append("DLQ message identity contains different event content")
    else:
        count = found.get("attributes", {}).get("ApproximateReceiveCount")
        try:
            require(int(count) >= expected_receives, "DLQ approximate receives below expected retry threshold")
        except (TypeError, ValueError) as exc:
            report["errors"].append({"dlq_attributes": str(exc)})
    if report["dlq"]["receive_error"] or report["dlq"]["restore_errors"]:
        report["pending"].append("DLQ inspection/restoration was not fully observed")
    try:
        report["logs"] = inspect_logs(logs, function_name, message_id, emitted)
        entries = report["logs"]["entries"]
        failed = sum(item["status"] == "FAILED" and item["error_type"] == "InvalidPayload" for item in entries)
        report["logs"]["observed_invalid_payload_failures"] = failed
        if any(item["status"] != "FAILED" or item["error_type"] != "InvalidPayload" for item in entries):
            report["errors"].append("The supplied message has an unexpected processing outcome")
        if failed < expected_receives or not report["logs"]["pagination_complete"]:
            report["pending"].append("CloudWatch retry observations may still be arriving")
        report["logs"]["count_note"] = "Delivery is at least once; retry observations are not distinct input messages. DLQ inspection increments its approximate receive counter."
    except Exception as exc:
        report["pending"].append({"logs_unavailable": consumer._code(exc) or type(exc).__name__})
    try:
        attributes = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"])["Attributes"]
        report["source_queue"] = {"scope": "shared account queue; these counts cannot be attributed to this run", "attributes": attributes}
        if any(int(attributes[name]) != 0 for name in ("ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed")):
            report["pending"].append("Shared source queue is nonempty; unrelated work cannot be ruled out")
    except Exception as exc:
        report["pending"].append({"source_queue_unavailable": consumer._code(exc) or type(exc).__name__})
    report["validation_status"] = "FAILED" if report["errors"] else "INCOMPLETE" if report["pending"] else "SUCCEEDED"
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--invalid-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--bucket", default="fiap-tc-fase2-2026")
    args = parser.parse_args(argv)
    import boto3

    session = boto3.Session(region_name=args.region)
    sqs = session.client("sqs")
    queue_url = sqs.get_queue_url(QueueName="fiap-inep-streaming")["QueueUrl"]
    dlq_url = sqs.get_queue_url(QueueName="fiap-inep-streaming-dlq")["QueueUrl"]
    report = verify_negative(session.client("s3"), sqs, session.client("logs"),
                             json.loads(args.invalid_report.read_text(encoding="utf-8")),
                             bucket=args.bucket, queue_url=queue_url, dlq_url=dlq_url)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"validation_status": report["validation_status"], "run_id": report.get("run_id"),
                      "invalid_message_id": report.get("sqs_message_id"), "report": str(args.output)}, ensure_ascii=False))
    return 0 if report["validation_status"] == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
