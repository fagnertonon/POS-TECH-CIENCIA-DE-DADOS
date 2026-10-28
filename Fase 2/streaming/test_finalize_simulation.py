"""Offline finalizer tests; no credentials, endpoints or cloud resources."""
import copy
from concurrent.futures import ThreadPoolExecutor
import gzip
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest

import finalize_simulation as finalizer


class S3Error(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.writes = []
        self.fail_gold_once = False
        self.lock = threading.Lock()

    def get_object(self, *, Bucket, Key):
        with self.lock:
            if (Bucket, Key) not in self.objects:
                raise S3Error("NoSuchKey")
            return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch, **kwargs):
        assert IfNoneMatch == "*"
        with self.lock:
            if self.fail_gold_once and Key.startswith("gold/inep_streaming/indicador_uf/"):
                self.fail_gold_once = False
                raise S3Error("ServiceUnavailable")
            if (Bucket, Key) in self.objects:
                raise S3Error("PreconditionFailed")
            self.objects[(Bucket, Key)] = Body
            self.writes.append((Key, kwargs))


def encode_for_test(rows, layer):
    # Core tests isolate transactional behavior from the optional native library.
    return finalizer.canonical_json({"layer": layer, "rows": rows})


def example_event(number=1, *, evaluated=True, alphabetized=True, weight=3.0):
    return {
        "schema_version": 1, "run_id": "sim_test", "event_id": "event_" + str(number),
        "source_snapshot_run_id": "jr_test", "entity": "alunos", "source": "inep_bronze_replay",
        "simulation": True, "event_time": "2026-10-27T15:00:00.000000Z",
        "payload": {"ano": 2024, "id_aluno": str(number), "id_municipio": "3550308",
                    "id_escola": "12345678", "serie": "2", "rede": "3", "caderno": "1",
                    "presenca": "1" if evaluated else "0", "preenchimento_caderno": "1",
                    "alfabetizado": "1" if alphabetized else "0", "peso_aluno": weight,
                    "proficiencia": 700.0 if evaluated else None},
    }


def install_event(s3, event):
    keys = finalizer.event_keys(event)
    raw = event["payload"]
    silver = dict(raw, sigla_uf="SP", presente=raw["presenca"] == "1",
                  caderno_preenchido=raw["preenchimento_caderno"] == "1")
    silver["avaliado"] = silver["presente"] and silver["caderno_preenchido"]
    silver["alfabetizado_avaliado"] = (raw["alfabetizado"] == "1") if silver["avaliado"] else None
    gold = {name: silver[name] for name in ("ano", "sigla_uf", "serie", "rede")}
    evaluated, alphabetized = silver["avaliado"], silver["alfabetizado_avaliado"]
    denominator = float(raw["peso_aluno"]) if evaluated else 0.0
    gold.update(eventos=1, alunos_avaliados=int(evaluated), alunos_alfabetizados=int(evaluated and alphabetized),
                soma_peso_avaliados=denominator,
                soma_peso_alfabetizados=denominator if evaluated and alphabetized else 0.0)
    bronze = {"event": event, "ingestion": {"event_sha256": finalizer.digest(event),
              "payload_sha256": finalizer.digest(raw), "target_key": keys["bronze_key"]}}
    s3.objects[(finalizer.BUCKET, keys["bronze_key"])] = finalizer.canonical_json(bronze)
    provenance = {name: event[name] for name in ("run_id", "event_id", "source_snapshot_run_id", "event_time")}
    provenance["bronze_key"] = keys["bronze_key"]
    for key_name, layer, data in (("silver_key", "silver", silver), ("gold_key", "gold_contribution", gold)):
        projection = {"schema_version": 2, "pipeline_version": finalizer.PIPELINE_VERSION,
                      "origin": finalizer.ORIGIN, "simulation": True, "layer": layer,
                      "event_sha256": finalizer.digest(event), "data": data,
                      "data_sha256": finalizer.digest(data), "provenance": provenance}
        s3.objects[(finalizer.BUCKET, keys[key_name])] = gzip.compress(finalizer.canonical_json(projection), mtime=0)
    receipt = {name: event[name] for name in ("run_id", "event_id", "source_snapshot_run_id", "event_time")}
    receipt.update(schema_version=2, pipeline_version=finalizer.PIPELINE_VERSION, status="GOLD_READY",
                   origin=finalizer.ORIGIN, simulation=True, event_sha256=finalizer.digest(event),
                   silver_data_sha256=finalizer.digest(silver), gold_data_sha256=finalizer.digest(gold),
                   completed_at_utc="2026-10-27T15:00:01.000000Z", end_to_end_latency_seconds=1.0)
    receipt.update({name: keys[name] for name in ("bronze_key", "silver_key", "gold_key")})
    s3.objects[(finalizer.BUCKET, keys["receipt_key"])] = finalizer.canonical_json(receipt)
    return keys


class FinalizerTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.events = [example_event(), example_event(2, alphabetized=False, weight=1.0)]
        self.keys = [install_event(self.s3, event) for event in self.events]

    def finalize(self, events=None, **kwargs):
        return finalizer.finalize_simulation(self.s3, self.events if events is None else events,
                                            encoder=encode_for_test, **kwargs)

    def change_json(self, key, update):
        encoded = self.s3.objects[(finalizer.BUCKET, key)]
        value = json.loads(gzip.decompress(encoded) if key.endswith(".gz") else encoded)
        update(value)
        encoded = finalizer.canonical_json(value)
        self.s3.objects[(finalizer.BUCKET, key)] = gzip.compress(encoded, mtime=0) if key.endswith(".gz") else encoded

    def test_weighted_aggregation_not_mean_of_rates_and_manifest_is_last(self):
        report = self.finalize()
        self.assertEqual(report["status"], "SUCCEEDED")
        row = report["gold_rows"][0]
        self.assertEqual(row["taxa_alfabetizacao_simulada"], 75.0)
        self.assertEqual(row["soma_peso_avaliados"], 4.0)
        self.assertEqual(row["soma_peso_alfabetizados"], 3.0)
        self.assertEqual(row["alunos_avaliados"], 2)
        self.assertEqual(row["eventos"], 2)
        self.assertEqual(self.s3.writes[-1][0], report["manifest_key"])
        self.assertEqual(len(self.s3.writes), 3)
        self.assertTrue(all(key.startswith(("silver/inep_streaming/", "gold/inep_streaming/", "auditoria/streaming/")) for key, _ in self.s3.writes))

    def test_retry_and_event_order_do_not_overwrite_or_duplicate(self):
        first = self.finalize()
        before = dict(self.s3.objects)
        second = self.finalize(list(reversed(self.events)))
        self.assertEqual(first, second)
        self.assertEqual(before, self.s3.objects)
        self.assertEqual(len(self.s3.writes), 3)

    def test_concurrent_finalizers_publish_identical_immutable_snapshot(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            reports = list(executor.map(lambda _: self.finalize(), range(2)))
        self.assertEqual(reports[0], reports[1])
        self.assertEqual(len(self.s3.writes), 3)

    def test_partial_materialization_has_no_manifest_and_retry_repairs(self):
        self.s3.fail_gold_once = True
        with self.assertRaises(S3Error):
            self.finalize()
        self.assertEqual(len(self.s3.writes), 1)
        self.assertFalse(any("/manifest.json" in key for _, key in self.s3.objects))
        report = self.finalize()
        self.assertEqual(report["status"], "SUCCEEDED")
        self.assertEqual(len(self.s3.writes), 3)

    def test_missing_gold_ready_receipt_blocks_all_new_writes(self):
        del self.s3.objects[(finalizer.BUCKET, self.keys[0]["receipt_key"])]
        with self.assertRaises(finalizer.FinalizationError):
            self.finalize()
        self.assertEqual(self.s3.writes, [])

    def test_altered_bronze_does_not_pass_matching_declared_hash(self):
        self.change_json(self.keys[0]["bronze_key"], lambda value: value["event"]["payload"].update(ano=2023))
        with self.assertRaises(finalizer.FinalizationError):
            self.finalize()
        self.assertEqual(self.s3.writes, [])

    def test_gold_with_correct_self_hash_but_wrong_math_is_blocked(self):
        def corrupt(value):
            value["data"]["soma_peso_alfabetizados"] = 99.0
            value["data_sha256"] = finalizer.digest(value["data"])
        self.change_json(self.keys[0]["gold_key"], corrupt)
        with self.assertRaises(finalizer.FinalizationError):
            self.finalize()
        self.assertEqual(self.s3.writes, [])

    def test_mixed_transform_versions_and_provenance_are_blocked(self):
        for name, value in (("pipeline_version", "1.0.0"), ("origin", "oficial"), ("simulation", False)):
            with self.subTest(name=name):
                s3 = FakeS3()
                for event in self.events:
                    install_event(s3, event)
                original = self.s3
                self.s3 = s3
                self.change_json(self.keys[0]["silver_key"], lambda item: item.update({name: value}))
                with self.assertRaises(finalizer.FinalizationError):
                    self.finalize()
                self.assertEqual(self.s3.writes, [])
                self.s3 = original

    def test_duplicate_business_key_with_different_event_ids_is_blocked(self):
        duplicate = copy.deepcopy(self.events[0])
        duplicate["event_id"] = "other_id_same_pupil"
        install_event(self.s3, duplicate)
        with self.assertRaisesRegex(finalizer.FinalizationError, "Duplicate pupil/year"):
            self.finalize(self.events + [duplicate])

    def test_empty_duplicate_and_mixed_run_inputs_are_rejected(self):
        mixed = copy.deepcopy(self.events[0])
        mixed["run_id"] = "other_run"
        for events in ([], [self.events[0], self.events[0]], [self.events[0], mixed]):
            with self.subTest(events=len(events)), self.assertRaises(finalizer.FinalizationError):
                self.finalize(events)
        self.assertEqual(self.s3.writes, [])

    def test_zero_assessed_preserves_null_rate_instead_of_false_zero(self):
        event = example_event(3, evaluated=False, weight=None)
        install_event(self.s3, event)
        report = self.finalize([event])
        row = report["gold_rows"][0]
        self.assertEqual(row["eventos"], 1)
        self.assertEqual(row["alunos_avaliados"], 0)
        self.assertEqual(row["soma_peso_avaliados"], 0.0)
        self.assertIsNone(row["taxa_alfabetizacao_simulada"])

    def test_optional_numeric_columns_have_stable_float64_logical_hash(self):
        event = example_event(4, weight=1)
        event["payload"]["proficiencia"] = 700
        install_event(self.s3, event)
        prepared = finalizer.read_run(self.s3, [event])
        row = prepared["silver_rows"][0]
        self.assertIs(type(row["peso_aluno"]), float)
        self.assertIs(type(row["proficiencia"]), float)

    def test_recorded_reference_sample_through_real_consumer_and_finalizer(self):
        import lambda_function as consumer
        # Portable historical reference, with synthetic pupil/school and event
        # identities. It is an offline test fixture, not an AWS execution.
        sample = Path(__file__).parent / "events_reference.jsonl"
        events = finalizer.read_events(sample)
        s3 = FakeS3()
        config = consumer.Config(finalizer.BUCKET, "bronze/inep_streaming",
                                 "auditoria/streaming/idempotency", frozenset({"alunos"}))
        context = SimpleNamespace(aws_request_id="offline_reference_run")
        for event in events:
            consumer.process_record({"messageId": event["event_id"],
                                     "body": finalizer.canonical_json(event).decode()}, context, config, s3)
        prepared = finalizer.read_run(s3, events)
        self.assertEqual(len(prepared["silver_rows"]), 30)
        # Independently audited reference sample: an unweighted average would
        # not reproduce these cohort results. They are not national indicators.
        for year, assessed, denominator, numerator, rate in (
            (2023, 8, 8.840219, 2.052966, 23.2230),
            (2024, 15, 16.71, 10.88, 65.1107),
        ):
            rows = [row for row in prepared["gold_rows"] if row["ano"] == year]
            self.assertEqual(sum(row["alunos_avaliados"] for row in rows), assessed)
            self.assertAlmostEqual(sum(row["soma_peso_avaliados"] for row in rows), denominator, places=5)
            self.assertAlmostEqual(sum(row["soma_peso_alfabetizados"] for row in rows), numerator, places=5)
            actual = round(100.0 * sum(row["soma_peso_alfabetizados"] for row in rows)
                           / sum(row["soma_peso_avaliados"] for row in rows), 4)
            self.assertEqual(actual, rate)

    def test_conflicting_immutable_content_never_overwrites(self):
        first = self.finalize()
        key = first["datasets"]["gold"]["key"]
        self.s3.objects[(finalizer.BUCKET, key)] = b"different bytes"
        before = dict(self.s3.objects)
        with self.assertRaisesRegex(finalizer.FinalizationError, "Existing immutable snapshot differs"):
            self.finalize()
        self.assertEqual(before, self.s3.objects)

    def test_existing_output_directory_is_rejected_before_cloud_writes(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(finalizer.FinalizationError):
                self.finalize(output_dir=folder)
        self.assertEqual(self.s3.writes, [])

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "PyArrow required for native Parquet roundtrip")
    def test_real_snappy_parquet_roundtrip_and_provenance(self):
        import pyarrow.parquet as pq
        report = finalizer.finalize_simulation(self.s3, self.events)
        for layer in ("silver", "gold"):
            key = report["datasets"][layer]["key"]
            raw = self.s3.objects[(finalizer.BUCKET, key)]
            self.assertTrue(raw.startswith(b"PAR1"))
            parquet = pq.ParquetFile(io.BytesIO(raw))
            rows = parquet.read().to_pylist()
            self.assertEqual(len(rows), report["datasets"][layer]["rows"])
            self.assertEqual(finalizer.digest(rows), report["datasets"][layer]["dataset_sha256"])
            self.assertEqual(parquet.metadata.row_group(0).column(0).compression, "SNAPPY")
            self.assertTrue(all(row["simulation"] is True and row["origin"] == finalizer.ORIGIN for row in rows))
        gold = pq.read_table(io.BytesIO(self.s3.objects[(finalizer.BUCKET, report["datasets"]["gold"]["key"])] )).to_pylist()
        self.assertEqual(gold[0]["taxa_alfabetizacao_simulada"], 75.0)
        # Same library/config produces identical immutable bytes on a retry.
        before = dict(self.s3.objects)
        finalizer.finalize_simulation(self.s3, list(reversed(self.events)))
        self.assertEqual(self.s3.objects, before)


if __name__ == "__main__":
    unittest.main()
