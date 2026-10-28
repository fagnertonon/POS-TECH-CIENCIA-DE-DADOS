"""Testes locais, sem credenciais, rede ou recursos AWS."""
import contextlib
import io
import json
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import lambda_function as consumer


class S3Error(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.fail_bronze_once = False
        self.deny_get_missing = False
        self.get_calls = []

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch, **_kwargs):
        assert IfNoneMatch == "*", "Todo PUT precisa ser condicional"
        if self.fail_bronze_once and Key.startswith("bronze/"):
            self.fail_bronze_once = False
            raise S3Error("ServiceUnavailable")
        if (Bucket, Key) in self.objects:
            raise S3Error("PreconditionFailed")
        self.objects[(Bucket, Key)] = Body

    def get_object(self, *, Bucket, Key):
        self.get_calls.append(Key)
        if (Bucket, Key) not in self.objects:
            raise S3Error("AccessDenied" if self.deny_get_missing else "NoSuchKey")
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


def envelope():
    return {
        "schema_version": 1, "event_id": "event_001", "run_id": "sim_20261020_001",
        "entity": "alunos", "event_time": "2026-10-20T18:00:00.000000Z",
        "source": "inep_bronze_replay", "source_snapshot_run_id": "jr_original",
        "simulation": True, "payload": {"ano": 2024, "id_aluno": "123", "proficiencia": None,
          "id_escola": "12345678", "id_municipio": "3301009", "serie": "2", "rede": "3",
          "presenca": "1", "preenchimento_caderno": "1", "alfabetizado": "1", "peso_aluno": 1.33},
    }


def record(event=None, message_id="sqs_001"):
    return {"messageId": message_id, "body": json.dumps(envelope() if event is None else event)}


class ConsumerTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.context = SimpleNamespace(aws_request_id="request_001")
        self.config = consumer.Config("fiap-tc-fase2-2026", "bronze/inep_streaming", "auditoria/streaming/idempotency", frozenset({"alunos"}))

    def consume(self, item):
        return consumer.process_record(item, self.context, self.config, self.s3)

    def bronze(self):
        return {k: v for k, v in self.s3.objects.items() if k[1].startswith("bronze/")}

    def test_accept_preserves_payload_and_writes_one_bronze_object(self):
        original = envelope()
        status, stored, latency = self.consume(record(original))
        self.assertEqual(status, "ACCEPTED")
        self.assertEqual(stored["event"], original)
        self.assertEqual(len(self.bronze()), 1)
        self.assertEqual(len(self.s3.objects), 5)
        self.assertEqual(stored["event"]["payload"]["proficiencia"], None)
        self.assertGreaterEqual(latency, 0)

    def test_duplicate_retains_original_metadata_and_never_overwrites(self):
        self.consume(record())
        before = dict(self.s3.objects)
        self.context.aws_request_id = "retry_request"
        status, stored, latency = self.consume(record(message_id="different_sqs_id"))
        self.assertEqual(status, "DUPLICATE")
        self.assertEqual(self.s3.objects, before)
        self.assertEqual(stored["ingestion"]["lambda_request_id"], "request_001")
        self.assertEqual(stored["ingestion"]["sqs_message_id"], "sqs_001")
        self.assertIsNone(latency)

    def test_same_identity_changed_payload_is_blocked(self):
        self.consume(record())
        changed = envelope()
        changed["payload"]["ano"] = 2023
        with self.assertRaises(consumer.EventIdentityConflict):
            self.consume(record(changed))
        self.assertEqual(len(self.bronze()), 1)

    def test_same_identity_changed_day_is_blocked_before_second_path(self):
        self.consume(record())
        changed = envelope()
        changed["event_time"] = "2026-10-21T18:00:00Z"
        with self.assertRaises(consumer.EventIdentityConflict):
            self.consume(record(changed))
        self.assertEqual(len(self.bronze()), 1)

    def test_latency_is_measured_after_bronze_put(self):
        before_claim = datetime(2026, 10, 20, 18, 0, 1, tzinfo=timezone.utc)
        after_bronze = datetime(2026, 10, 20, 18, 0, 9, tzinfo=timezone.utc)
        with patch.object(consumer, "utc_now", side_effect=[before_claim, after_bronze]):
            status, stored, latency = self.consume(record())
        self.assertEqual(status, "ACCEPTED")
        self.assertEqual(latency, 9.0)
        self.assertEqual(stored["ingestion"]["received_at_utc"], "2026-10-20T18:00:01.000000Z")

    def test_corrupted_existing_bronze_is_not_acknowledged_as_duplicate(self):
        self.consume(record())
        key = next(iter(self.bronze()))
        corrupted = json.loads(self.s3.objects[key])
        corrupted["event"]["payload"]["ano"] = 2022
        self.s3.objects[key] = json.dumps(corrupted).encode()
        with self.assertRaises(consumer.StoredEventCorruption):
            self.consume(record())

    def test_failed_projection_is_recovered_from_durable_claim(self):
        self.s3.fail_bronze_once = True
        with self.assertRaises(S3Error):
            self.consume(record())
        self.assertEqual(len(self.bronze()), 0)
        claim_before = next(iter(self.s3.objects.values()))
        self.context.aws_request_id = "retry_request"
        status, stored, _latency = self.consume(record(message_id="retry_message"))
        self.assertEqual(status, "ACCEPTED")
        self.assertEqual(len(self.bronze()), 1)
        self.assertEqual(stored["ingestion"]["lambda_request_id"], "request_001")
        self.assertIn(claim_before, self.s3.objects.values())

    def test_recovery_needs_no_listbucket_or_get_of_missing_bronze(self):
        self.s3.deny_get_missing = True
        self.s3.fail_bronze_once = True
        with self.assertRaises(S3Error):
            self.consume(record())
        self.assertEqual(len(self.bronze()), 0)
        status, _stored, _latency = self.consume(record(message_id="retry_message"))
        self.assertEqual(status, "ACCEPTED")
        self.assertEqual(len(self.bronze()), 1)
        self.assertEqual(len(self.s3.get_calls), 1)
        self.assertTrue(self.s3.get_calls[0].startswith("auditoria/streaming/idempotency/"))

    def test_invalid_timestamps_and_ids_do_not_write(self):
        for field, value in (
            ("event_time", "2026-02-30T00:00:00Z"),
            ("event_time", "2026-10-20T18:00:00-03:00"),
            ("event_time", "2026-10-20"),
            ("event_id", "../escape"),
            ("run_id", "a/b"),
            ("schema_version", True),
        ):
            with self.subTest(field=field, value=value):
                changed = envelope()
                changed[field] = value
                with self.assertRaises(consumer.InvalidEnvelope):
                    self.consume(record(changed))
        self.assertEqual(self.s3.objects, {})

    def test_non_finite_json_rejected(self):
        changed = envelope()
        changed["payload"]["proficiencia"] = float("nan")
        with self.assertRaises(consumer.InvalidEnvelope):
            self.consume(record(changed))
        self.assertEqual(self.s3.objects, {})

    def test_partial_batch_retries_only_invalid_record_and_emits_metrics(self):
        invalid = envelope()
        invalid["schema_version"] = 99
        batch = {"Records": [record(), record(message_id="duplicate"), record(invalid, "bad")]}
        output = io.StringIO()
        with patch.object(consumer, "_client", return_value=self.s3), patch.object(consumer, "load_config", return_value=self.config), contextlib.redirect_stdout(output):
            result = consumer.lambda_handler(batch, self.context)
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "bad"}]})
        emitted = [json.loads(line) for line in output.getvalue().splitlines()]
        metric = [line for line in emitted if line.get("kind") == "STREAMING_BATCH"][0]
        self.assertEqual({name: metric[name] for name in ["Received", "Accepted", "Duplicate", "Failed"]}, {"Received": 3, "Accepted": 1, "Duplicate": 1, "Failed": 1})
        self.assertEqual(metric["_aws"]["CloudWatchMetrics"][0]["Namespace"], "FIAP/Streaming")
        self.assertEqual(len(metric["LatencySeconds"]), 1)
        self.assertNotIn('"payload"', output.getvalue())


if __name__ == "__main__":
    unittest.main()
