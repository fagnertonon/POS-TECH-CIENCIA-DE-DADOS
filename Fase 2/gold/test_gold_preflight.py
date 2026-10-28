"""Dependency-free contract tests; no Spark session or AWS calls are made."""
import ast
from contextlib import redirect_stdout
import copy
import io
import json
import math
from pathlib import Path
import re
import sys
import unittest
from unittest.mock import patch
from urllib.parse import urlparse
import uuid
import hashlib

SOURCE = Path(__file__).with_name("gold_glue.py")
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))
NAMES = {"required_columns", "params", "type_matches", "inspect_silver", "emit_outcome",
         "audit_join", "regra", "exigir"}
CONSTANTS = {"DEFAULTS", "NOMES_SILVER", "COLUNAS_META", "NIVEIS", "NUMERIC", "ANOS_META"}
NODES = [node for node in TREE.body
         if (isinstance(node, ast.FunctionDef) and node.name in NAMES)
         or (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in CONSTANTS for t in node.targets))]
SCOPE = dict(re=re, math=math, sys=type("Arguments", (), {"argv": ["gold_glue.py"]})(),
             uuid=uuid, urlparse=urlparse, json=json, hashlib=hashlib)
exec(compile(ast.Module(body=NODES, type_ignores=[]), str(SOURCE), "exec"), SCOPE)


def fixture():
    tables = []
    for table, columns in SCOPE["required_columns"]().items():
        fields = [{"name": name, "type": "double" if kind == "numeric" else kind,
                   "nullable": True, "metadata": {}} for name, kind in columns.items()]
        tables.append({"table": table, "target_uri": f"s3://example/silver/inep/{table}/run_id=jr_fixture/",
                       "target_count": 2, "counts_match": True, "status": "SUCCEEDED",
                       "schema": {"type": "struct", "fields": fields}})
    return {"status": "SUCCEEDED", "layer": "silver", "is_full_batch": True,
            "run_id": "jr_fixture", "tables": tables}


class FakeBoto:
    def __init__(self, manifest):
        self.manifest = manifest
        self.requests = []

    def client(self, service):
        if service != "s3":
            raise AssertionError("Preflight must not create/read catalog entries")
        return self

    def get_object(self, **args):
        self.requests.append(args)
        return {"Body": io.BytesIO(json.dumps(self.manifest).encode())}


class PreflightTests(unittest.TestCase):
    def run_preflight(self, manifest=None, uri="s3://example/auditoria/silver/run_id=jr_fixture/manifest.json"):
        fake = FakeBoto(fixture() if manifest is None else manifest)
        SCOPE["boto3"] = fake
        cfg = SCOPE["params"]({"SILVER_MANIFEST_URI": uri})
        return SCOPE["inspect_silver"](None, cfg), fake

    def test_exact_successful_snapshot(self):
        lineage, fake = self.run_preflight()
        self.assertEqual(len(lineage), 7)
        self.assertEqual(len(fake.requests), 1)
        self.assertTrue(all(row["expected_count"] == 2 for row in lineage.values()))
        self.assertEqual(lineage["alunos"]["silver_run_id"], "jr_fixture")

    def test_missing_manifest_blocks_without_aws_request(self):
        with self.assertRaisesRegex(RuntimeError, "BLOCKED_MISSING_SILVER"):
            self.run_preflight(uri="")

    def test_failed_silver_is_rejected(self):
        manifest = fixture()
        manifest["status"] = "FAILED"
        with self.assertRaisesRegex(RuntimeError, "SUCCEEDED"):
            self.run_preflight(manifest)

    def test_incomplete_batch_is_rejected(self):
        manifest = fixture()
        manifest["is_full_batch"] = False
        with self.assertRaises(RuntimeError):
            self.run_preflight(manifest)

    def test_duplicate_or_missing_table_is_rejected(self):
        manifest = fixture()
        manifest["tables"][-1] = copy.deepcopy(manifest["tables"][0])
        with self.assertRaisesRegex(RuntimeError, "seven distinct"):
            self.run_preflight(manifest)

    def test_missing_evaluated_column_is_rejected(self):
        manifest = fixture()
        fields = manifest["tables"][0]["schema"]["fields"]
        manifest["tables"][0]["schema"]["fields"] = [f for f in fields if f["name"] != "avaliado"]
        with self.assertRaisesRegex(RuntimeError, "missing avaliado"):
            self.run_preflight(manifest)

    def test_wrong_boolean_type_is_rejected(self):
        manifest = fixture()
        for field in manifest["tables"][0]["schema"]["fields"]:
            if field["name"] == "avaliado":
                field["type"] = "string"
        with self.assertRaisesRegex(RuntimeError, "expected boolean"):
            self.run_preflight(manifest)

    def test_mixed_snapshots_are_rejected(self):
        manifest = fixture()
        manifest["tables"][0]["target_uri"] = "s3://example/silver/alunos/run_id=jr_other/"
        with self.assertRaisesRegex(RuntimeError, "run_id"):
            self.run_preflight(manifest)

    def test_bronze_fallback_is_rejected(self):
        manifest = fixture()
        manifest["tables"][0]["target_uri"] = "s3://example/bronze/alunos/run_id=jr_fixture/"
        with self.assertRaisesRegex(RuntimeError, "Silver layer"):
            self.run_preflight(manifest)

    def test_failed_table_count_validation_is_rejected(self):
        manifest = fixture()
        manifest["tables"][0]["counts_match"] = False
        with self.assertRaisesRegex(RuntimeError, "count-validated"):
            self.run_preflight(manifest)

    def test_unversioned_history_root_is_rejected(self):
        manifest = fixture()
        manifest["tables"][0]["target_uri"] = "s3://example/silver/alunos/"
        with self.assertRaisesRegex(RuntimeError, "one run_id"):
            self.run_preflight(manifest)

    def test_parameter_safety(self):
        for override in ({"LIMIAR_JOIN": "nan"}, {"TOLERANCIA_PP": "inf"},
                         {"GOLD_PREFIX": "bronze/inep"}, {"JOB_RUN_ID": "../../overwrite"},
                         {"DATABASE_GOLD": "tech_challenge_fase2_silver"}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                SCOPE["params"](override)

    def test_emf_outcome(self):
        SCOPE.update(CFG={"JOB_NAME": "fixture-job"}, ID_EXECUCAO="fixture-run", time=lambda: 1234.567)
        for failed, expected in ((False, 0), (True, 1)):
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertTrue(SCOPE["emit_outcome"](failed, 1.2))
            body = json.loads(output.getvalue())
            self.assertEqual(body["Failed"], expected)
            self.assertEqual(body["Layer"], "gold")
            spec = body["_aws"]["CloudWatchMetrics"][0]
            self.assertEqual(spec["Namespace"], "FIAP/Medallion")
            self.assertEqual(spec["Metrics"], [{"Name": "Failed", "Unit": "Count"}])

    def test_emf_logging_failure_does_not_mask_job_failure(self):
        SCOPE.update(CFG={"JOB_NAME": "fixture-job"}, ID_EXECUCAO="fixture-run", time=lambda: 1234.567)
        with patch("builtins.print", side_effect=OSError("closed stdout")):
            self.assertFalse(SCOPE["emit_outcome"](True, 1.2))

    def test_empty_eligible_join_is_explicit_na(self):
        class Frame:
            def join(self, *args):
                return "joined"
        SCOPE.update(JOIN_METRICS=[], LIMIAR_JOIN=0.95,
                     taxa_correspondencia=lambda *args: (0.0, 0, 0))
        self.assertEqual(SCOPE["audit_join"](Frame(), Frame(), ["year"], "left", "fixture",
                                            eligible=Frame(), universe_description="No eligible years"), "joined")
        last = SCOPE["JOIN_METRICS"][-1]
        self.assertIsNone(last["passou"])
        self.assertIsNone(last["valor_medido"])
        self.assertFalse(last["aplicavel"])
        self.assertIn("N/A", last["detalhe"])

    def test_low_eligible_match_rate_still_blocks(self):
        class Frame:
            def join(self, *args):
                raise AssertionError("A rejected business join must not execute")
        SCOPE.update(JOIN_METRICS=[], LIMIAR_JOIN=0.95,
                     taxa_correspondencia=lambda *args: (0.94, 94, 100))
        with self.assertRaisesRegex(RuntimeError, "bloqueantes"):
            SCOPE["audit_join"](Frame(), Frame(), ["year"], "left", "fixture", eligible=Frame())


if __name__ == "__main__":
    unittest.main(verbosity=2)
