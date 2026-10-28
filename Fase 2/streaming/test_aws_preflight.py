"""Metadata-only preflight tests; no boto3, credentials or network required."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import aws_preflight as preflight


class MockS3:
    def __init__(self, pages=None):
        self.pages = pages or [{"Contents": [], "IsTruncated": False}]
        self.calls = []

    def list_objects_v2(self, **options):
        self.calls.append(options)
        index = int(options.get("ContinuationToken", "0"))
        return self.pages[index]


def clients():
    outputs = {
        "FunctionName": preflight.FUNCTION,
        "EventSourceMappingUuid": "mapping-id",
        "QueueUrl": f"https://sqs.{preflight.REGION}.amazonaws.com/{preflight.ACCOUNT}/fiap-inep-streaming",
        "DeadLetterQueueUrl": f"https://sqs.{preflight.REGION}.amazonaws.com/{preflight.ACCOUNT}/fiap-inep-streaming-dlq",
    }
    stack = {"StackId": f"arn:aws:cloudformation:{preflight.REGION}:{preflight.ACCOUNT}:stack/fiap-inep-streaming/id",
             "StackStatus": "UPDATE_COMPLETE", "Outputs": [{"OutputKey": key, "OutputValue": value}
                                                              for key, value in outputs.items()]}

    def queue_attributes(QueueUrl, AttributeNames):
        assert AttributeNames == ["All"]
        if QueueUrl.endswith("-dlq"):
            return {"Attributes": {"MessageRetentionPeriod": "1209600"}}
        return {"Attributes": {"VisibilityTimeout": "60", "MessageRetentionPeriod": "345600",
                               "ReceiveMessageWaitTimeSeconds": "20", "RedrivePolicy": json.dumps({
                                   "maxReceiveCount": 5, "deadLetterTargetArn":
                                   f"arn:aws:sqs:{preflight.REGION}:{preflight.ACCOUNT}:fiap-inep-streaming-dlq"})}}

    return {
        "sts": SimpleNamespace(get_caller_identity=lambda: {"Account": preflight.ACCOUNT}),
        "cloudformation": SimpleNamespace(
            describe_stacks=lambda **_kwargs: {"Stacks": [stack]},
            get_template=lambda **_kwargs: {"TemplateBody": {"Resources": {"ConsumerFunction": {
                "Properties": {"Code": {"ZipFile": "reviewed source"}}}}}}),
        "lambda": SimpleNamespace(
            get_function_configuration=lambda **_kwargs: {
                "FunctionName": preflight.FUNCTION, "Runtime": "python3.12", "Timeout": 10,
                "MemorySize": 256, "State": "Active", "LastUpdateStatus": "Successful",
                "Role": f"arn:aws:iam::{preflight.ACCOUNT}:role/{preflight.ROLE}",
                "Environment": {"Variables": {"UNEXPECTED_SECRET": "must-not-be-exported"}}},
            get_function_concurrency=lambda **_kwargs: {"ReservedConcurrentExecutions": 2},
            get_event_source_mapping=lambda **_kwargs: {
                "State": "Enabled", "BatchSize": 5, "MaximumBatchingWindowInSeconds": 0,
                "ScalingConfig": {"MaximumConcurrency": 2}, "FunctionResponseTypes": ["ReportBatchItemFailures"],
                "EventSourceArn": f"arn:aws:sqs:{preflight.REGION}:{preflight.ACCOUNT}:fiap-inep-streaming"}),
        "iam": SimpleNamespace(get_role_policy=lambda **_kwargs: {"PolicyName": preflight.POLICY,
                                                                   "PolicyDocument": {"Statement": []}}),
        "sqs": SimpleNamespace(get_queue_attributes=queue_attributes),
        "s3": MockS3(),
    }


class PreflightTests(unittest.TestCase):
    def test_bounded_paginated_inventory_is_order_independent(self):
        prefix = "gold/inep/"
        first = {"Key": prefix + "z", "Size": 4, "ETag": '"z"', "LastModified": "2026-10-27T00:00:00Z"}
        second = {"Key": prefix + "a", "Size": 7, "ETag": '"a"', "LastModified": "2026-10-26T00:00:00Z"}
        paginated = MockS3([
            {"Contents": [first], "IsTruncated": True, "NextContinuationToken": "1"},
            {"Contents": [second], "IsTruncated": False}])
        value = preflight.inventory(paginated, "bucket", prefix)
        other = preflight.inventory(MockS3([{"Contents": [second, first]}]), "bucket", prefix)
        self.assertEqual(value["sha256"], other["sha256"])
        self.assertEqual(value["object_count"], 2)
        self.assertEqual(value["total_bytes"], 11)
        self.assertEqual([item["key"] for item in value["objects"]], [prefix + "a", prefix + "z"])

    def test_inventory_over_limit_and_nonadvancing_token_fail_closed(self):
        objects = [{"Key": "bronze/inep/" + str(index), "Size": 1} for index in range(3)]
        with self.assertRaisesRegex(ValueError, "exceeds"):
            preflight.inventory(MockS3([{"Contents": objects}]), "bucket", "bronze/inep/", max_keys=2)
        with self.assertRaisesRegex(ValueError, "continuation token"):
            preflight.inventory(MockS3([{"Contents": [], "IsTruncated": True}]), "bucket", "gold/inep/")

    def test_metadata_change_alters_comparison_fingerprint(self):
        original = {"Key": "silver/inep/key", "Size": 12, "ETag": '"same"', "LastModified": "T1"}
        first = preflight.inventory(MockS3([{"Contents": [original]}]), "bucket", "silver/inep/")
        changed = dict(original, LastModified="T2")
        second = preflight.inventory(MockS3([{"Contents": [changed]}]), "bucket", "silver/inep/")
        self.assertNotEqual(first["sha256"], second["sha256"])

    def test_valid_capture_saves_six_complete_inventories_without_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "capture"
            report = preflight.capture(clients(), path)
            self.assertEqual(report["validation_status"], "SUCCEEDED", report["errors"])
            self.assertEqual(len(report["official_inventory"]), 6)
            self.assertTrue(all(value["complete"] for value in report["official_inventory"].values()))
            self.assertNotIn("must-not-be-exported", (path / "lambda_configuration.json").read_text())
            self.assertEqual(report, json.loads((path / "preflight_report.json").read_text()))
            with self.assertRaises(FileExistsError):
                preflight.capture(clients(), path)

    def test_wrong_account_or_region_prevents_capture(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "capture"
            mocks = clients()
            mocks["sts"].get_caller_identity = lambda: {"Account": "000000000000"}
            with self.assertRaisesRegex(ValueError, "authorized account"):
                preflight.capture(mocks, path)
            self.assertFalse(path.exists())
            with self.assertRaisesRegex(ValueError, "us-east-2"):
                preflight.capture(clients(), path, region="us-east-1")

    def test_drift_is_saved_and_cannot_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            mocks = clients()
            mocks["lambda"].get_function_concurrency = lambda **_kwargs: {"ReservedConcurrentExecutions": 20}
            report = preflight.capture(mocks, Path(temporary) / "capture")
            self.assertEqual(report["validation_status"], "FAILED")
            self.assertFalse(report["checks"]["reserved_concurrency"]["passed"])
            self.assertEqual(len(report["official_inventory"]), 6)

    def test_api_failure_cannot_be_reported_as_empty_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            mocks = clients()
            mocks["s3"].list_objects_v2 = lambda **_kwargs: (_ for _ in ()).throw(PermissionError("denied"))
            report = preflight.capture(mocks, Path(temporary) / "capture")
            self.assertEqual(report["validation_status"], "FAILED")
            self.assertEqual(report["official_inventory"], {})
            self.assertEqual(sum(isinstance(item, dict) and item["error_type"] == "PermissionError"
                                 for item in report["errors"]), 6)


if __name__ == "__main__":
    unittest.main()
