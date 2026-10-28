"""Offline gates for the explicitly prepared streaming update plan."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import deploy_simulation as deploy


class FakeCloudFormation:
    def __init__(self):
        self.changes = [{"ResourceChange": {"LogicalResourceId": name, "Action": "Modify", "Replacement": "False"}}
                        for name in ("ConsumerRole", "ConsumerFunction")]
        self.prepared, self.executed = [], []
        self.stack_status = "UPDATE_COMPLETE"

    def describe_stacks(self, **_kwargs):
        return {"Stacks": [{"StackStatus": self.stack_status, "Outputs": []}]}

    def create_change_set(self, **kwargs):
        self.prepared.append(kwargs)
        return {"Id": "immutable-change-set-id", "StackId": "reviewed-stack-id"}

    def describe_change_set(self, **_kwargs):
        return {"Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE", "Changes": self.changes}

    def execute_change_set(self, **kwargs):
        self.executed.append(kwargs)


class DeploymentGates(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.template = {"Resources": {"ConsumerFunction": {"Properties": {
            "MemorySize": 256, "Timeout": 10, "ReservedConcurrentExecutions": 2}}}}
        self.template_path = self.root / "streaming_stack.json"
        self.template_path.write_text(json.dumps(self.template), encoding="utf-8")
        self.cfn = FakeCloudFormation()
        self.account = "637246457028"
        self.boto3 = SimpleNamespace(Session=lambda **_kwargs: SimpleNamespace(client=self.client))

    def client(self, name):
        return {"sts": SimpleNamespace(get_caller_identity=lambda: {"Account": self.account}),
                "cloudformation": self.cfn}[name]

    def run_mode(self, mode, path=None, extra=()):
        with patch.dict(sys.modules, {"boto3": self.boto3}), patch.object(deploy, "__file__", str(self.root / "deploy_simulation.py")), contextlib.redirect_stdout(io.StringIO()):
            deploy.main(["--mode", mode, "--output-dir", str(path or self.root / "plan"), *extra])

    def test_prepare_reviewable_exact_modifications_never_executes(self):
        self.run_mode("prepare")
        self.assertEqual(self.cfn.executed, [])
        request = self.cfn.prepared[0]
        self.assertEqual(request["ChangeSetType"], "UPDATE")
        self.assertEqual(request["Capabilities"], ["CAPABILITY_NAMED_IAM"])
        self.assertEqual(request["Parameters"], [{"ParameterKey": "BucketName", "UsePreviousValue": True}])
        plan = json.loads((self.root / "plan/change_set_review.json").read_text())
        self.assertEqual(plan["status"], "REVIEWABLE")
        self.assertEqual(plan["template_sha256"], hashlib.sha256(self.template_path.read_bytes()).hexdigest())

    def test_replacement_addition_or_unrelated_resource_is_blocked(self):
        for index, change in enumerate((
            {"LogicalResourceId": "ConsumerFunction", "Action": "Modify", "Replacement": "True"},
            {"LogicalResourceId": "SourceQueue", "Action": "Modify", "Replacement": "False"},
            {"LogicalResourceId": "ConsumerRole", "Action": "Add", "Replacement": "False"},
        )):
            with self.subTest(change=change):
                self.cfn.changes = [{"ResourceChange": change}]
                with self.assertRaisesRegex(RuntimeError, "Unexpected change set"):
                    self.run_mode("prepare", self.root / f"blocked-{index}")
                self.assertEqual(self.cfn.executed, [])
                value = json.loads((self.root / f"blocked-{index}/change_set_review.json").read_text())
                self.assertEqual(value["status"], "BLOCKED")

    def test_compute_cap_drift_prevents_even_change_set_creation(self):
        self.template["Resources"]["ConsumerFunction"]["Properties"]["MemorySize"] = 1024
        self.template_path.write_text(json.dumps(self.template), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Compute caps"):
            self.run_mode("prepare")
        self.assertEqual(self.cfn.prepared, [])

    def test_changed_template_cannot_execute_saved_plan(self):
        self.run_mode("prepare")
        self.template_path.write_text(self.template_path.read_text() + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "source/plan changed"):
            self.run_mode("execute")
        self.assertEqual(self.cfn.executed, [])

    def test_execute_uses_exact_saved_id_and_status_is_read_only(self):
        self.run_mode("prepare")
        self.run_mode("status")
        self.assertEqual(self.cfn.executed, [])
        self.run_mode("execute")
        self.assertEqual(self.cfn.executed, [{"ChangeSetName": "immutable-change-set-id"}])

    def test_wrong_account_or_unstable_stack_cannot_prepare(self):
        self.account = "000000000000"
        with self.assertRaisesRegex(ValueError, "account"):
            self.run_mode("prepare")
        self.account = "637246457028"
        self.cfn.stack_status = "UPDATE_IN_PROGRESS"
        with self.assertRaisesRegex(ValueError, "not stable"):
            self.run_mode("prepare")
        self.assertEqual(self.cfn.prepared, [])


if __name__ == "__main__":
    unittest.main()
