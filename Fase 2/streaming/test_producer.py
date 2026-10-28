import copy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location("streaming_producer", Path(__file__).with_name("producer.py"))
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


class ProducerTests(unittest.TestCase):
    def test_event_identity_stable_and_raw_payload_preserved(self):
        payload = {"id_aluno": "000001", "proficiencia": None, "peso_aluno": 1.23456789, "rede": "1"}
        sample = {"source_object_uri": "s3://b/part.parquet", "sample_index": 3, "payload": payload}
        before = copy.deepcopy(payload)
        stamp = datetime(2026, 10, 20, 18, tzinfo=timezone.utc)
        first = p.make_event(sample, "sim_test", "jr_source", stamp)
        second = p.make_event(sample, "sim_test", "jr_source", stamp)
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(first["payload"], before)
        self.assertEqual(payload, before)
        self.assertNotEqual(first["event_id"], p.make_event(sample, "sim_other", "jr_source", stamp)["event_id"])
        self.assertEqual(p.strict_json(first), p.strict_json(second))

    def test_nan_is_rejected_without_conversion(self):
        with self.assertRaisesRegex(ValueError, "JSON estrito"):
            p.strict_json({"proficiencia": float("nan")})

    def test_replay_preserves_exact_message_and_ids(self):
        sample = {"source_object_uri": "s3://b/p", "sample_index": 0, "payload": {"id_aluno": "0001"}}
        body = json.dumps(p.make_event(sample, "sim_test", "jr_source"), ensure_ascii=False, indent=None)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text(body + "\n", encoding="utf-8")
            bodies, run_id = p.read_replay(path)
        self.assertEqual(bodies, [body])
        self.assertEqual(run_id, "sim_test")

    def test_source_rejects_partial_or_inconsistent_manifest(self):
        manifest = {"status": "SUCCEEDED", "is_full_batch": False, "all_counts_match": True}
        with self.assertRaisesRegex(ValueError, "batch completo"):
            p.source_spec(manifest)

    def test_send_requires_sqs_confirmation(self):
        class Sqs:
            def send_message(self, **kwargs):
                return {}
        with self.assertRaisesRegex(RuntimeError, "não confirmou"):
            p.send_body(Sqs(), "https://sqs.us-east-2.amazonaws.com/123/q", '{"event_id":"e","run_id":"r"}', "NEW", 1)


if __name__ == "__main__":
    unittest.main()
