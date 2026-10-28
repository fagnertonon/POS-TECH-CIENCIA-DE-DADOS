"""Bounded replay tests with fake AWS clients; no real messages are emitted."""
import contextlib
import base64
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
import zipfile
from unittest.mock import Mock, patch

import exercise_simulation as exercise
import producer
from test_lambda_function import envelope


class FakeSQS:
    def __init__(self):
        self.sent = []
        self.fail_at = None

    def send_message(self, **kwargs):
        if len(self.sent) == self.fail_at:
            raise RuntimeError("Simulated send failure")
        self.sent.append(kwargs)
        return {"MessageId": f"message-{len(self.sent)}", "ResponseMetadata": {"HTTPStatusCode": 200}}


class ExerciseGates(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.account = "637246457028"
        self.source = "reviewed lambda source\n"
        (self.root / "lambda_function.py").write_text(self.source, encoding="utf-8")
        self.sqs = FakeSQS()
        self.template_source = self.source
        self.stack_status = "UPDATE_COMPLETE"
        self.configuration = {"State": "Active", "LastUpdateStatus": "Successful", "MemorySize": 256, "Timeout": 10}
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("index.py", self.source)
        self.package = buffer.getvalue()
        self.configuration["CodeSha256"] = base64.b64encode(hashlib.sha256(self.package).digest()).decode()
        self.mapping = {"State": "Enabled", "BatchSize": 5, "ScalingConfig": {"MaximumConcurrency": 2},
                        "FunctionResponseTypes": ["ReportBatchItemFailures"]}
        self.concurrency = {"ReservedConcurrentExecutions": 2}
        self.queue_url = "https://sqs.us-east-2.amazonaws.com/637246457028/fiap-inep-streaming"
        self.sample = []
        for index in range(30):
            data = envelope()["payload"]
            data["id_aluno"] = str(1000 + index)
            self.sample.append({"payload": data, "source_object_uri": "s3://bucket/source.parquet", "sample_index": index})
        self.load_sample = Mock(return_value=(self.sample, {"snapshot_run_id": "jr_original"}))
        self.boto3 = SimpleNamespace(Session=lambda **_kwargs: SimpleNamespace(client=self.client))

    def client(self, name):
        if name == "sts":
            return SimpleNamespace(get_caller_identity=lambda: {"Account": self.account})
        if name == "cloudformation":
            return SimpleNamespace(
                describe_stacks=lambda **_kwargs: {"Stacks": [{"StackStatus": self.stack_status, "Outputs": [
                    {"OutputKey": "FunctionName", "OutputValue": "fiap-inep-streaming-consumer"},
                    {"OutputKey": "EventSourceMappingUuid", "OutputValue": "mapping"},
                    {"OutputKey": "QueueUrl", "OutputValue": self.queue_url}]}]},
                get_template=lambda **_kwargs: {"TemplateBody": {"Resources": {"ConsumerFunction": {"Properties": {
                    "Code": {"ZipFile": self.template_source}}}}}})
        if name == "lambda":
            return SimpleNamespace(get_function_configuration=lambda **_kwargs: self.configuration,
                                   get_event_source_mapping=lambda **_kwargs: self.mapping,
                                   get_function_concurrency=lambda **_kwargs: self.concurrency,
                                   get_function=lambda **_kwargs: {"Code": {"Location": "https://never-used.invalid/code.zip"}})
        if name == "sqs":
            return self.sqs
        if name == "s3":
            return SimpleNamespace()  # load_sample is mocked; any other S3 call fails.
        raise AssertionError("Unexpected AWS service " + name)

    def run_exercise(self):
        with patch.dict(sys.modules, {"boto3": self.boto3}), patch.object(exercise, "__file__", str(self.root / "exercise_simulation.py")), patch.object(producer, "load_sample", self.load_sample), patch.object(exercise.time, "sleep"), patch.object(exercise, "urlopen", side_effect=lambda *_args, **_kwargs: io.BytesIO(self.package)), contextlib.redirect_stdout(io.StringIO()):
            exercise.main(["--output-dir", str(self.root / "evidence")])

    def test_exactly_thirty_new_five_exact_duplicates_and_one_invalid(self):
        self.run_exercise()
        self.load_sample.assert_called_once()
        self.assertEqual(self.load_sample.call_args.args[2], 30)
        self.assertEqual(len(self.sqs.sent), 36)
        invalid = json.loads(self.sqs.sent[0]["MessageBody"])
        self.assertEqual(invalid["payload"]["peso_aluno"], 0)
        valid_bodies = [item["MessageBody"] for item in self.sqs.sent[1:31]]
        duplicates = [item["MessageBody"] for item in self.sqs.sent[31:]]
        self.assertEqual(duplicates, valid_bodies[:5])
        self.assertEqual(len({json.loads(body)["event_id"] for body in valid_bodies}), 30)
        self.assertEqual(len({json.loads(body)["run_id"] for body in valid_bodies}), 1)
        self.assertNotEqual(invalid["run_id"], json.loads(valid_bodies[0])["run_id"])
        report = json.loads((self.root / "evidence/producer/producer_report.json").read_text())
        self.assertEqual(report["status"], "SUCCEEDED")
        self.assertEqual(report["unique_event_count"], 30)
        self.assertEqual(report["confirmed_send_count"], 35)
        self.assertEqual(report["duplicate_send_count"], 5)

    def test_wrong_account_stale_stack_and_source_mismatch_send_nothing(self):
        scenarios = (("account", "000000000000"), ("stack_status", "UPDATE_IN_PROGRESS"),
                     ("template_source", "untested code"))
        for field, value in scenarios:
            with self.subTest(field=field):
                before = getattr(self, field)
                setattr(self, field, value)
                with self.assertRaises(ValueError):
                    self.run_exercise()
                setattr(self, field, before)
                self.assertEqual(self.sqs.sent, [])
                self.load_sample.assert_not_called()

    def test_lossy_template_export_requires_exact_running_utf8_source(self):
        self.source = '# Simulação: publicação\n' + self.source
        (self.root/'lambda_function.py').write_text(self.source,encoding='utf-8')
        self.template_source = self.source.encode('ascii','replace').decode()
        package = io.BytesIO()
        with zipfile.ZipFile(package,'w') as archive:
            archive.writestr('index.py',self.source)
        self.package = package.getvalue()
        self.configuration['CodeSha256'] = base64.b64encode(hashlib.sha256(self.package).digest()).decode()
        self.run_exercise()
        metadata = json.loads((self.root/'evidence/exercise_metadata.json').read_text())
        self.assertFalse(metadata['template_export_exact_match'])
        self.assertTrue(metadata['running_lambda_source_exact_match'])

    def test_lossy_running_code_is_blocked_even_if_template_has_same_conversion(self):
        self.source = '# Simulação: publicação\n' + self.source
        (self.root/'lambda_function.py').write_text(self.source,encoding='utf-8')
        self.template_source = self.source.encode('ascii','replace').decode()
        package = io.BytesIO()
        with zipfile.ZipFile(package,'w') as archive:
            archive.writestr('index.py',self.template_source)
        self.package = package.getvalue()
        self.configuration['CodeSha256'] = base64.b64encode(hashlib.sha256(self.package).digest()).decode()
        with self.assertRaisesRegex(ValueError,'Running Lambda source'):
            self.run_exercise()
        self.assertEqual(self.sqs.sent,[])
        self.load_sample.assert_not_called()

    def test_inactive_consumer_or_caps_drift_send_nothing(self):
        for field, value in (("State", "Pending"), ("MemorySize", 512), ("Timeout", 60)):
            with self.subTest(field=field):
                previous = self.configuration[field]
                self.configuration[field] = value
                with self.assertRaises(ValueError):
                    self.run_exercise()
                self.configuration[field] = previous
                self.assertEqual(self.sqs.sent, [])
                self.load_sample.assert_not_called()

    def test_source_read_failure_sends_nothing(self):
        self.load_sample.side_effect = ValueError("unapproved source manifest")
        with self.assertRaises(ValueError):
            self.run_exercise()
        self.assertEqual(self.sqs.sent, [])

    def test_mapping_or_concurrency_drift_prevents_source_read_and_send(self):
        for obj, name, value in ((self.mapping, "BatchSize", 10),
                                  (self.mapping, "FunctionResponseTypes", []),
                                  (self.mapping, "ScalingConfig", {"MaximumConcurrency": 10}),
                                  (self.concurrency, "ReservedConcurrentExecutions", 10)):
            with self.subTest(field=name):
                previous = obj[name]
                obj[name] = value
                with self.assertRaisesRegex(ValueError, "configuration drifted"):
                    self.run_exercise()
                obj[name] = previous
                self.assertEqual(self.sqs.sent, [])
                self.load_sample.assert_not_called()

    def test_running_package_checksum_or_source_drift_sends_nothing(self):
        expected_hash = self.configuration["CodeSha256"]
        self.configuration["CodeSha256"] = "different-checksum"
        with self.assertRaisesRegex(ValueError, "checksum differs"):
            self.run_exercise()
        self.configuration["CodeSha256"] = expected_hash
        changed = io.BytesIO()
        with zipfile.ZipFile(changed, "w") as archive:
            archive.writestr("index.py", "untested deployed code")
        self.package = changed.getvalue()
        self.configuration["CodeSha256"] = base64.b64encode(hashlib.sha256(self.package).digest()).decode()
        with self.assertRaisesRegex(ValueError, "Running Lambda source"):
            self.run_exercise()
        self.assertEqual(self.sqs.sent, [])
        self.load_sample.assert_not_called()

    def test_invalid_sample_or_wrong_sample_size_prevents_every_send(self):
        self.sample[-1]["payload"]["peso_aluno"] = 0
        with self.assertRaises(ValueError):
            self.run_exercise()
        self.assertEqual(self.sqs.sent, [])
        self.load_sample.return_value = (self.sample[:2], {"snapshot_run_id": "jr_original"})
        with self.assertRaisesRegex(ValueError, "thirty sampled rows"):
            self.run_exercise()
        self.assertEqual(self.sqs.sent, [])

    def test_partial_send_failure_preserves_failed_report_and_no_continuation(self):
        self.sqs.fail_at = 4
        with self.assertRaisesRegex(RuntimeError, "send failure"):
            self.run_exercise()
        self.assertEqual(len(self.sqs.sent), 4)  # One negative fixture plus three valid sends.
        report = json.loads((self.root / "evidence/producer/producer_report.json").read_text())
        self.assertEqual(report["status"], "FAILED")
        self.assertEqual(report["confirmed_send_count"], 3)
        self.assertEqual(len(report["send_results"]), 3)


if __name__ == "__main__":
    unittest.main()
