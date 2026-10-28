"""Read-only before/after capture for the isolated streaming simulation.

Only metadata is read: no Glue session/job, BigQuery query, queue receive/send,
IAM change, object download or signed Lambda code URL is requested. S3 official
prefix inventories are bounded and must be complete to support comparison.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re


ACCOUNT = "637246457028"
REGION = "us-east-2"
OFFICIAL_PREFIXES = (
    "bronze/inep/", "silver/inep/", "gold/inep/",
    "auditoria/bronze/", "auditoria/silver/", "auditoria/gold/",
)
MAX_KEYS = 10000
FUNCTION = "fiap-inep-streaming-consumer"
ROLE = "fiap-lambda-inep-streaming"
POLICY = "StreamingScopedAccess"


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def stamp(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value


def serializable(value):
    if isinstance(value, dict):
        return {key: serializable(item) for key, item in value.items()
                if key != "ResponseMetadata"}
    if isinstance(value, (list, tuple)):
        return [serializable(item) for item in value]
    return stamp(value)


def save(path, value):
    path.write_text(json.dumps(serializable(value), ensure_ascii=False, indent=2,
                               allow_nan=False) + "\n", encoding="utf-8")


def inventory(s3, bucket, prefix, max_keys=MAX_KEYS):
    """List at most max_keys+1 metadata entries and fail closed on truncation."""
    if prefix not in OFFICIAL_PREFIXES:
        raise ValueError("Only explicit official prefixes may be inventoried")
    objects, token = [], None
    while True:
        options = {"Bucket": bucket, "Prefix": prefix,
                   "MaxKeys": min(1000, max_keys + 1 - len(objects))}
        if token:
            options["ContinuationToken"] = token
        response = s3.list_objects_v2(**options)
        for entry in response.get("Contents", []):
            key = entry["Key"]
            if not key.startswith(prefix):
                raise ValueError("Inventory returned an object outside its prefix")
            objects.append({"key": key, "size": entry["Size"],
                            "etag": entry.get("ETag"),
                            "last_modified": stamp(entry.get("LastModified"))})
        if len(objects) > max_keys:
            raise ValueError(f"Official inventory exceeds {max_keys} keys: {prefix}")
        if not response.get("IsTruncated"):
            break
        next_token = response.get("NextContinuationToken")
        if not next_token or next_token == token:
            raise ValueError("Truncated inventory has no advancing continuation token")
        token = next_token
    objects.sort(key=lambda item: item["key"])
    if len({item["key"] for item in objects}) != len(objects):
        raise ValueError("Official inventory contains duplicate object keys")
    return {"bucket": bucket, "prefix": prefix, "complete": True,
            "object_count": len(objects), "total_bytes": sum(item["size"] for item in objects),
            "sha256": digest(objects), "objects": objects}


def capture(clients, output_dir, *, bucket="fiap-tc-fase2-2026",
            region=REGION, stack_name="fiap-inep-streaming"):
    """All clients are injected, so the preflight itself can be tested offline."""
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise FileExistsError("Choose a new output directory; evidence is never overwritten")
    if region != REGION:
        raise ValueError("This controlled deployment is restricted to us-east-2")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid bucket name")
    account = clients["sts"].get_caller_identity()["Account"]
    if account != ACCOUNT:
        raise ValueError("AWS account differs from the authorized account")
    output_dir.mkdir(parents=True)
    report = {"schema_version": 1, "started_at_utc": stamp(datetime.now(timezone.utc)),
              "account": account, "region": region, "bucket": bucket,
              "stack_name": stack_name, "read_only": True, "starts_compute": False,
              "checks": {}, "errors": [], "resources": {}, "official_inventory": {}}

    def check(name, actual, expected):
        passed = type(actual) is type(expected) and actual == expected
        report["checks"][name] = {"actual": actual, "expected": expected, "passed": passed}
        if not passed:
            report["errors"].append("Control differs: " + name)

    def capture_resource(name, operation):
        try:
            result = serializable(operation())
            save(output_dir / (name + ".json"), result)
            report["resources"][name] = {"file": name + ".json", "sha256": digest(result)}
            return result
        except Exception as exc:
            report["errors"].append({"resource": name, "error_type": type(exc).__name__,
                                      "error_code": str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))})
            return None

    cfn, lamb = clients["cloudformation"], clients["lambda"]
    stack = capture_resource("stack", lambda: cfn.describe_stacks(StackName=stack_name)["Stacks"][0])
    outputs = {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])} if stack else {}
    if stack:
        check("stack_ready", stack.get("StackStatus") in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}, True)
        check("stack_account_region", stack.get("StackId", "").startswith(f"arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/"), True)
    template = capture_resource("stack_template", lambda: cfn.get_template(
        StackName=stack_name, TemplateStage="Original")["TemplateBody"])
    if isinstance(template, str):
        try:
            template = json.loads(template)
        except json.JSONDecodeError:
            report["errors"].append("The deployed template must be JSON")
    if isinstance(template, dict):
        source = template.get("Resources", {}).get("ConsumerFunction", {}).get("Properties", {}).get("Code", {}).get("ZipFile")
        if isinstance(source, str):
            report["deployed_inline_source_sha256"] = hashlib.sha256(source.encode("utf-8")).hexdigest()

    function_name = outputs.get("FunctionName", FUNCTION)
    check("function_name", function_name, FUNCTION)
    # GetFunctionConfiguration never returns a presigned download URL.
    function = capture_resource("lambda_configuration", lambda: {
        key: value for key, value in lamb.get_function_configuration(FunctionName=function_name).items()
        if key in {"FunctionName", "FunctionArn", "Runtime", "Role", "Timeout", "MemorySize",
                   "State", "LastUpdateStatus", "LastModified", "CodeSha256", "CodeSize",
                   "Architectures", "EphemeralStorage", "RevisionId", "PackageType"}})
    if function:
        for field, expected in (("FunctionName", FUNCTION), ("Runtime", "python3.12"),
                                ("Timeout", 10), ("MemorySize", 256),
                                ("State", "Active"), ("LastUpdateStatus", "Successful")):
            check("lambda_" + field, function.get(field), expected)
        check("lambda_role", function.get("Role"), f"arn:aws:iam::{ACCOUNT}:role/{ROLE}")
    concurrency = capture_resource("lambda_concurrency", lambda: lamb.get_function_concurrency(FunctionName=function_name))
    if concurrency is not None:
        check("reserved_concurrency", concurrency.get("ReservedConcurrentExecutions"), 2)

    mapping_uuid = outputs.get("EventSourceMappingUuid")
    if not mapping_uuid:
        report["errors"].append("Stack output EventSourceMappingUuid is required")
    mapping = capture_resource("event_source_mapping", lambda: lamb.get_event_source_mapping(UUID=mapping_uuid)) if mapping_uuid else None
    if mapping:
        for field, expected in (("State", "Enabled"), ("BatchSize", 5), ("MaximumBatchingWindowInSeconds", 0)):
            check("mapping_" + field, mapping.get(field), expected)
        check("mapping_concurrency", mapping.get("ScalingConfig", {}).get("MaximumConcurrency"), 2)
        check("partial_batch_failures", mapping.get("FunctionResponseTypes"), ["ReportBatchItemFailures"])
        check("mapping_queue", mapping.get("EventSourceArn"), f"arn:aws:sqs:{REGION}:{ACCOUNT}:fiap-inep-streaming")
    policy = capture_resource("role_inline_policy", lambda: clients["iam"].get_role_policy(RoleName=ROLE, PolicyName=POLICY))
    if policy:
        check("inline_policy_name", policy.get("PolicyName"), POLICY)

    for label, output_key in (("source", "QueueUrl"), ("dlq", "DeadLetterQueueUrl")):
        url = outputs.get(output_key)
        if not url:
            report["errors"].append("Missing queue output " + output_key)
            continue
        expected_name = "fiap-inep-streaming" + ("-dlq" if label == "dlq" else "")
        check(label + "_queue_url", url, f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/{expected_name}")
        result = capture_resource(label + "_queue_attributes", lambda url=url: clients["sqs"].get_queue_attributes(
            QueueUrl=url, AttributeNames=["All"]))
        if not result:
            continue
        attributes = result.get("Attributes", {})
        if label == "source":
            for field, expected in (("VisibilityTimeout", "60"), ("MessageRetentionPeriod", "345600"),
                                    ("ReceiveMessageWaitTimeSeconds", "20")):
                check("source_" + field, attributes.get(field), expected)
            try:
                redrive = json.loads(attributes.get("RedrivePolicy", "{}"))
            except (TypeError, json.JSONDecodeError):
                redrive = {}
            check("source_max_receive_count", redrive.get("maxReceiveCount"), 5)
            check("source_dlq_arn", redrive.get("deadLetterTargetArn"), f"arn:aws:sqs:{REGION}:{ACCOUNT}:fiap-inep-streaming-dlq")
        else:
            check("dlq_retention", attributes.get("MessageRetentionPeriod"), "1209600")

    for prefix in OFFICIAL_PREFIXES:
        filename = "official_" + prefix.strip("/").replace("/", "_") + ".json"
        result = capture_resource(filename.removesuffix(".json"), lambda prefix=prefix: inventory(clients["s3"], bucket, prefix))
        if result:
            report["official_inventory"][prefix] = {key: result[key] for key in (
                "object_count", "total_bytes", "sha256", "complete")}
            report["official_inventory"][prefix]["file"] = filename

    report["finished_at_utc"] = stamp(datetime.now(timezone.utc))
    report["validation_status"] = "SUCCEEDED" if not report["errors"] else "FAILED"
    save(output_dir / "preflight_report.json", report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--region", default=REGION)
    parser.add_argument("--bucket", default="fiap-tc-fase2-2026")
    parser.add_argument("--stack", default="fiap-inep-streaming")
    args = parser.parse_args(argv)
    if args.region != REGION:
        parser.error("This controlled capture is restricted to us-east-2")
    import boto3
    session = boto3.Session(region_name=args.region)
    clients = {name: session.client(name) for name in (
        "sts", "cloudformation", "lambda", "iam", "sqs", "s3")}
    result = capture(clients, args.output_dir, bucket=args.bucket,
                     region=args.region, stack_name=args.stack)
    print(json.dumps({"validation_status": result["validation_status"],
                      "account": result["account"], "official_prefixes": len(result["official_inventory"]),
                      "errors": result["errors"], "report": str(args.output_dir / "preflight_report.json")}, ensure_ascii=False))
    return 0 if result["validation_status"] == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
