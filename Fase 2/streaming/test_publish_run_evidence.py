"""Evidence publication is gated before AWS writes and never overwrites history."""
import contextlib
import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import lambda_function as consumer
import publish_run_evidence as publication
from test_lambda_function import envelope


class Collision(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class ImmutableS3:
    def __init__(self):
        self.objects = {}
        self.put_attempts = []

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch, **_kwargs):
        if IfNoneMatch != "*":
            raise AssertionError("Every audit write must be conditional")
        self.put_attempts.append(Key)
        identity = Bucket, Key
        if identity in self.objects:
            raise Collision()
        self.objects[identity] = bytes(Body)

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


class PublicationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.project = self.root / "project"
        self.exercise = self.root / "exercise"
        self.paths = {
            "producer": self.exercise / "producer/producer_report.json",
            "positive": self.exercise / "verification_final.json",
            "negative": self.exercise / "verification_negative.json",
            "runtime": self.exercise / "runtime_evidence.json",
            "official_preservation": self.project / "official_preservation.json",
            "exercise": self.exercise / "exercise_metadata.json",
            "invalid_fixture": self.exercise / "invalid_semantic.json",
            "source_diagnostic": self.project / "deployment/source_diagnostic.json",
            "finalization": self.exercise / "finalized/finalization_report.json",
            "snapshot_manifest": self.exercise / "finalized/manifest.json",
            "before": self.project / "aws_before/preflight_report.json",
            "after": self.project / "aws_after/preflight_report.json",
            "deployment": self.project / "deployment/deployment_state.json",
            "approved_plan": self.project / "deployment/change_set_review.json",
        }
        self.run = "sim_gold_publication_fixture"
        self.prefix = "auditoria/streaming/runs/run_id=" + self.run
        bad = envelope()
        bad["run_id"] = "invalid_weight_fixture"
        bad["payload"]["peso_aluno"] = 0
        source_hash = hashlib.sha256(Path(publication.__file__).with_name("lambda_function.py").read_bytes()).hexdigest()
        self.docs = {
            "producer": {"status": "SUCCEEDED", "run_id": self.run},
            "positive": {"validation_status": "SUCCEEDED", "run_id": self.run,
                         "finalized_snapshot": {"status": "SUCCEEDED"}},
            "negative": {"validation_status": "SUCCEEDED", "run_id": bad["run_id"],
                         "event_id": bad["event_id"], "sqs_message_id": "invalid-message",
                         "event_sha256": consumer.sha256(bad)},
            "runtime": {"capture_status": "COMPLETE", "run_id": self.run, "invalid_message_id": "invalid-message"},
            "official_preservation": {"status": "SUCCEEDED", "authorized_iam_additions_only": True},
            "exercise": {"run_id": self.run, "source_sha256": source_hash},
            "invalid_fixture": {"event": bad, "message_id": "invalid-message"},
            "source_diagnostic": {"exact_match": True, "live_source_sha256": source_hash},
            "finalization": {"status": "SUCCEEDED", "run_id": self.run, "snapshot_id": "b" * 64,
                             "manifest_uri": "s3://fiap-tc-fase2-2026/gold-manifest.json"},
            "snapshot_manifest": {"status": "SUCCEEDED", "run_id": self.run,
                                  "source_snapshot_run_id": "batch_source", "last_gold_ready_at_utc": "2026-10-27T15:00:00Z",
                                  "expected_unique_events": 30, "bronze_events": 30, "silver_events": 30,
                                  "gold_contributions": 30, "gold_groups": 19},
            "before": {"validation_status": "SUCCEEDED"},
            "after": {"validation_status": "SUCCEEDED"},
            "deployment": {"stack_status": "UPDATE_COMPLETE"},
            "approved_plan": {"status": "REVIEWABLE"},
        }
        self.s3 = ImmutableS3()
        self.session_calls = 0

    def session(self, **_kwargs):
        self.session_calls += 1
        clients = {"s3": self.s3, "sts": SimpleNamespace(get_caller_identity=lambda: {"Account": "637246457028"})}
        return SimpleNamespace(client=lambda name: clients[name])

    def write_docs(self, values=None):
        for name, document in (values or self.docs).items():
            path = self.paths[name]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")

    def run_main(self):
        argv = ["publish_run_evidence.py", "--project-dir", str(self.project),
                "--exercise-dir", str(self.exercise), "--region", "us-east-2"]
        with patch.object(sys, "argv", argv), patch.dict(sys.modules, {"boto3": SimpleNamespace(Session=self.session)}), contextlib.redirect_stdout(io.StringIO()):
            publication.main()

    def test_failed_prerequisite_or_unverified_parquet_stops_before_any_aws_call(self):
        for name, field in (("positive", "validation_status"), ("negative", "validation_status"),
                            ("runtime", "capture_status"), ("deployment", "stack_status")):
            with self.subTest(name=name):
                changed = copy.deepcopy(self.docs)
                changed[name][field] = "INCOMPLETE"
                self.write_docs(changed)
                with self.assertRaisesRegex(ValueError, "Unsuccessful prerequisite"):
                    self.run_main()
        changed = copy.deepcopy(self.docs)
        changed["positive"]["finalized_snapshot"]["status"] = "NOT_CHECKED"
        self.write_docs(changed)
        with self.assertRaisesRegex(ValueError, "Parquet snapshot"):
            self.run_main()
        self.assertEqual(self.session_calls, 0)
        self.assertEqual(self.s3.put_attempts, [])

    def test_wrong_run_source_or_iam_gate_stops_before_aws_writes(self):
        for name, field, value in (("runtime", "run_id", "another_run"),
                                   ("source_diagnostic", "exact_match", False),
                                   ("source_diagnostic", "exact_match", "true"),
                                   ("official_preservation", "authorized_iam_additions_only", False),
                                   ("official_preservation", "authorized_iam_additions_only", "true"),
                                   ("negative", "run_id", "other_invalid_run"),
                                   ("negative", "sqs_message_id", "other_message"),
                                   ("negative", "event_sha256", "wrong_hash"),
                                   ("runtime", "invalid_message_id", "other_message"),
                                   ("source_diagnostic", "live_source_sha256", "wrong_hash"),
                                   ("exercise", "source_sha256", "wrong_hash")):
            with self.subTest(name=name, field=field, value=value):
                changed = copy.deepcopy(self.docs)
                changed[name][field] = value
                self.write_docs(changed)
                with self.assertRaises(ValueError):
                    self.run_main()
        self.assertEqual(self.session_calls, 0)
        self.assertEqual(self.s3.objects, {})

    def test_manifest_is_last_and_identical_republication_preserves_all_bytes(self):
        self.write_docs()
        self.run_main()
        first = dict(self.s3.objects)
        self.assertEqual(len(first), 15)  # Fourteen evidence files plus one commit manifest.
        self.assertEqual(self.s3.put_attempts[-1], self.prefix + "/manifest.json")
        self.run_main()
        self.assertEqual(self.s3.objects, first)
        self.assertEqual(len(self.s3.put_attempts), 30)
        result = json.loads((self.exercise / "audit_publication.json").read_text())
        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertEqual(result["manifest_key"], self.prefix + "/manifest.json")

    def test_existing_tampered_evidence_or_manifest_is_never_overwritten(self):
        self.write_docs()
        for suffix in ("evidence/producer.json", "manifest.json"):
            with self.subTest(suffix=suffix):
                self.s3 = ImmutableS3()
                identity = "fiap-tc-fase2-2026", self.prefix + "/" + suffix
                self.s3.objects[identity] = b"tampered historical object"
                with self.assertRaisesRegex(ValueError, "Existing immutable evidence differs"):
                    self.run_main()
                self.assertEqual(self.s3.objects[identity], b"tampered historical object")
                self.assertFalse((self.exercise / "audit_publication.json").exists())
                if suffix.startswith("evidence/"):
                    self.assertNotIn((identity[0], self.prefix + "/manifest.json"), self.s3.objects)


if __name__ == "__main__":
    unittest.main()
