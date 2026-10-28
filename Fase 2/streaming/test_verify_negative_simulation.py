"""Bounded negative evidence checks never delete, send or redrive messages."""
import json
from types import SimpleNamespace
import unittest

import lambda_function as consumer
from test_lambda_function import envelope, record
from test_verify_simulation import InventoryS3
import verify_negative_simulation as verifier


class SQS:
    def __init__(self, pages):
        self.pages = list(pages)
        self.restored = []
        self.receives = 0
        self.visible = 0

    def receive_message(self, **kwargs):
        self.receives += 1
        assert kwargs["MaxNumberOfMessages"] == 10
        assert kwargs["VisibilityTimeout"] == 30
        page = self.pages.pop(0) if self.pages else []
        if isinstance(page, Exception):
            raise page
        return {"Messages": page}

    def change_message_visibility(self, **kwargs):
        assert kwargs["VisibilityTimeout"] == 0
        self.restored.append(kwargs["ReceiptHandle"])

    def get_queue_attributes(self, **_kwargs):
        return {"Attributes": {"ApproximateNumberOfMessages": str(self.visible),
                               "ApproximateNumberOfMessagesNotVisible": "0",
                               "ApproximateNumberOfMessagesDelayed": "0"}}


class Logs:
    def __init__(self, message_id, count=5):
        self.message_id, self.count = message_id, count

    def filter_log_events(self, **kwargs):
        assert self.message_id in kwargs["filterPattern"]
        return {"events": [{"eventId": str(i), "timestamp": i,
                             "message": json.dumps({"kind": "STREAMING_EVENT", "status": "FAILED",
                                                    "sqs_message_id": self.message_id, "error_type": "InvalidPayload"})}
                            for i in range(self.count)]}


class NegativeVerificationTests(unittest.TestCase):
    def setUp(self):
        self.s3 = InventoryS3()
        self.event = envelope()
        self.event["payload"]["peso_aluno"] = 0
        self.config = verifier.positive.default_config("fiap-tc-fase2-2026")
        with self.assertRaises(consumer.InvalidPayload):
            consumer.process_record(record(self.event), SimpleNamespace(aws_request_id="local-negative"), self.config, self.s3)
        self.paths = consumer.stage_paths(self.event, self.config)
        self.message_id = "invalid-message-001"
        self.fixture = {"event": self.event, "message_id": self.message_id,
                        "expected_dlq_after_max_receive_count": 5}
        self.message = {"MessageId": self.message_id, "ReceiptHandle": "expected-receipt",
                        "Body": consumer.canonical_json(self.event).decode(),
                        "Attributes": {"ApproximateReceiveCount": "6"}}
        self.sqs = SQS([[self.message]])
        self.logs = Logs(self.message_id)

    def verify(self):
        return verifier.verify_negative(self.s3, self.sqs, self.logs, self.fixture,
                                        queue_url="source-url", dlq_url="dlq-url")

    def test_success_proves_quality_gate_retries_dlq_and_preserves_visibility(self):
        before = dict(self.s3.objects)
        report = self.verify()
        self.assertEqual(report["validation_status"], "SUCCEEDED")
        self.assertEqual(report["logs"]["observed_invalid_payload_failures"], 5)
        self.assertEqual(report["dlq"]["deleted_messages"], 0)
        self.assertEqual(self.sqs.restored, ["expected-receipt"])
        self.assertEqual(self.s3.objects, before)

    def test_asynchronous_missing_dlq_and_zero_logs_are_incomplete(self):
        self.sqs = SQS([[]])
        self.logs = Logs(self.message_id, 0)
        report = self.verify()
        self.assertEqual(report["validation_status"], "INCOMPLETE")
        self.assertFalse(report["errors"])

    def test_missing_bronze_is_incomplete(self):
        del self.s3.objects[(self.config.bucket, self.paths["bronze_key"])]
        self.assertEqual(self.verify()["validation_status"], "INCOMPLETE")

    def test_corrupt_bronze_hash_is_failed(self):
        key = (self.config.bucket, self.paths["bronze_key"])
        value = json.loads(self.s3.objects[key])
        value["event"]["payload"]["peso_aluno"] = 1
        self.s3.objects[key] = consumer.canonical_json(value)
        self.assertEqual(self.verify()["validation_status"], "FAILED")

    def test_published_silver_after_rejection_is_failed(self):
        self.s3.objects[(self.config.bucket, self.paths["silver_key"])] = b"{}"
        report = self.verify()
        self.assertEqual(report["validation_status"], "FAILED")
        self.assertEqual(report["objects"]["silver"]["actual_count"], 1)

    def test_bounded_dlq_inspection_restores_unrelated_receipts_even_on_error(self):
        unrelated = [dict(self.message, MessageId=f"other-{i}", ReceiptHandle=f"receipt-{i}") for i in range(10)]
        self.sqs = SQS([unrelated, RuntimeError("temporary receive failure")])
        report = self.verify()
        self.assertEqual(report["validation_status"], "INCOMPLETE")
        self.assertEqual(len(self.sqs.restored), 10)
        self.assertLessEqual(self.sqs.receives, 3)
        self.assertEqual(report["dlq"]["deleted_messages"], 0)

    def test_nonempty_shared_queue_is_inconclusive_without_blame_on_run(self):
        self.sqs.visible = 1
        report = self.verify()
        self.assertEqual(report["validation_status"], "INCOMPLETE")
        self.assertFalse(report["errors"])
        self.assertIn("cannot be attributed", report["source_queue"]["scope"])


if __name__ == "__main__":
    unittest.main()
