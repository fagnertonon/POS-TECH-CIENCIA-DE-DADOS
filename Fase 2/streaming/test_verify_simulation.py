"""Read-only verification must catch incompleteness and coherent-looking tampering."""
import gzip
import importlib.util
import json
from types import SimpleNamespace
import unittest

import lambda_function as consumer
from test_lambda_function import FakeS3, envelope, record
import verify_simulation as verifier


class InventoryS3(FakeS3):
    def get_paginator(self, name):
        if name != "list_objects_v2":
            raise AssertionError(name)
        objects = self.objects

        class Paginator:
            def paginate(self, *, Bucket, Prefix):
                yield {"Contents": [{"Key": key, "Size": len(body)}
                                    for (bucket, key), body in objects.items()
                                    if bucket == Bucket and key.startswith(Prefix)]}

        return Paginator()


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.s3 = InventoryS3()
        self.config = verifier.default_config("fiap-tc-fase2-2026")
        self.context = SimpleNamespace(aws_request_id="local-verification-test")
        self.event = envelope()
        self.body = consumer.canonical_json(self.event).decode("utf-8")
        consumer.process_record(record(self.event), self.context, self.config, self.s3)
        self.paths = consumer.stage_paths(self.event, self.config)

    def verify(self, event=None):
        body = self.body if event is None else consumer.canonical_json(event).decode("utf-8")
        return verifier.verify_run(self.s3, [body], config=self.config)

    def test_valid_and_duplicate_keep_exactly_one_effect_and_first_latency(self):
        before = dict(self.s3.objects)
        first = self.verify()
        consumer.process_record(record(self.event, "duplicate-delivery"), self.context, self.config, self.s3)
        second = self.verify()
        self.assertEqual(self.s3.objects, before)
        self.assertEqual(first["validation_status"], "SUCCEEDED")
        self.assertEqual(second["verified_event_count"], 1)
        self.assertEqual(first["end_to_end_latency_seconds"], second["end_to_end_latency_seconds"])
        self.assertTrue(all(item["actual"] == 1 for item in second["inventory"].values()))

    def test_missing_completion_receipt_is_incomplete(self):
        del self.s3.objects[(self.config.bucket, self.paths["receipt_key"])]
        report = self.verify()
        self.assertEqual(report["validation_status"], "INCOMPLETE")
        self.assertEqual(report["verified_event_count"], 0)

    def test_bool_in_place_of_integer_cannot_hide_behind_python_equality(self):
        key = (self.config.bucket, self.paths["gold_key"])
        changed = json.loads(gzip.decompress(self.s3.objects[key]))
        changed["data"]["eventos"] = True  # True == 1 in Python, but types differ in JSON.
        self.s3.objects[key] = gzip.compress(consumer.canonical_json(changed), mtime=0)
        report = self.verify()
        self.assertEqual(report["validation_status"], "FAILED")
        self.assertIn("normalized data", report["events"][0]["reason"])

    def test_extra_identity_in_same_run_is_failed(self):
        key = self.paths["silver_key"].replace("event_id=event_001", "event_id=unexpected")
        self.s3.objects[(self.config.bucket, key)] = self.s3.objects[(self.config.bucket, self.paths["silver_key"])]
        report = self.verify()
        self.assertEqual(report["validation_status"], "FAILED")
        self.assertEqual(report["inventory"]["silver"]["unexpected"], [key])

    def test_latency_requires_independent_completion_timestamp(self):
        key = (self.config.bucket, self.paths["receipt_key"])
        changed = json.loads(self.s3.objects[key])
        changed["end_to_end_latency_seconds"] += 1
        self.s3.objects[key] = consumer.canonical_json(changed)
        report = self.verify()
        self.assertEqual(report["validation_status"], "FAILED")
        self.assertIn("independent timestamps", report["events"][0]["reason"])

    def test_invalid_weight_is_failed_and_cannot_publish_analytics(self):
        for weight in (None, 0, -1, True):
            with self.subTest(weight=weight):
                s3 = InventoryS3()
                event = envelope()
                event["payload"]["peso_aluno"] = weight
                with self.assertRaises(consumer.InvalidPayload):
                    consumer.process_record(record(event), self.context, self.config, s3)
                report = verifier.verify_run(s3, [consumer.canonical_json(event).decode()], config=self.config)
                self.assertEqual(report["validation_status"], "FAILED")
                self.assertTrue(any(key.startswith("bronze/") for _, key in s3.objects))
                self.assertFalse(any(key.startswith(("silver/", "gold/")) for _, key in s3.objects))
                self.assertFalse(any("/event_id=" in key and key.startswith(self.config.processed_prefix)
                                     for _, key in s3.objects))

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "Install requirements-producer.txt for Parquet verification")
    def test_real_parquet_snapshot_reconciles_events_and_weighted_rate(self):
        import finalize_simulation as finalizer

        second = envelope()
        second["event_id"] = "event_002"
        second["payload"].update(id_aluno="456", alfabetizado="0", peso_aluno=2)
        absent = envelope()
        absent["event_id"] = "event_003"
        absent["payload"].update(id_aluno="789", id_municipio="3550308", presenca="0",
                                  preenchimento_caderno=None, alfabetizado=None, peso_aluno=None)
        for event in (second, absent):
            consumer.process_record(record(event), self.context, self.config, self.s3)
        official_key = (self.config.bucket, "gold/inep/official_snapshot.parquet")
        self.s3.objects[official_key] = b"official data must remain unchanged"
        events = [self.event, second, absent]
        report = finalizer.finalize_simulation(self.s3, events)
        result = verifier.verify_finalized_snapshot(self.s3, report, self.config.bucket, events)
        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertEqual({item["name"]: item["rows"] for item in result["datasets"]}, {"silver": 3, "gold": 2})
        rj = next(row for row in report["gold_rows"] if row["sigla_uf"] == "RJ")
        sp = next(row for row in report["gold_rows"] if row["sigla_uf"] == "SP")
        self.assertEqual(rj["taxa_alfabetizacao_simulada"], round(100 * 1.33 / 3.33, 4))
        self.assertIsNone(sp["taxa_alfabetizacao_simulada"])
        self.assertEqual(self.s3.objects[official_key], b"official data must remain unchanged")

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "Install requirements-producer.txt for Parquet verification")
    def test_report_for_another_run_cannot_validate_this_run(self):
        import finalize_simulation as finalizer

        report = finalizer.finalize_simulation(self.s3, [self.event])
        report["run_id"] = "another_run"
        with self.assertRaisesRegex(ValueError, "another run"):
            verifier.verify_finalized_snapshot(self.s3, report, self.config.bucket, [self.event])

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "Install requirements-producer.txt for Parquet verification")
    def test_parquet_content_tampering_cannot_be_reported_as_success(self):
        import finalize_simulation as finalizer

        report = finalizer.finalize_simulation(self.s3, [self.event])
        key = (self.config.bucket, report["datasets"]["gold"]["key"])
        self.s3.objects[key] += b"tampered"
        with self.assertRaisesRegex(ValueError, "content hash"):
            verifier.verify_finalized_snapshot(self.s3, report, self.config.bucket, [self.event])


if __name__ == "__main__":
    unittest.main()
