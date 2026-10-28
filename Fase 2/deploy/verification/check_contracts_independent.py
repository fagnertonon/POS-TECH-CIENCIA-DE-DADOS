"""Independent static/pure contract checks. No AWS or Spark execution."""
import ast
import copy
import hashlib
import io
import json
import math
from pathlib import Path
import re
import types
import uuid
from urllib.parse import urlparse, urlsplit

ROOT = Path(__file__).resolve().parents[1]
results = []


def check(label, func):
    try:
        func()
        results.append({"test": label, "passed": True})
    except Exception as error:
        results.append({"test": label, "passed": False, "error": f"{type(error).__name__}: {error}"})


def must_reject(func):
    try:
        func()
    except (ValueError, RuntimeError):
        return
    raise AssertionError("Expected contract to reject malformed input")


gold_path = ROOT / "gold/gold_glue.py"
silver_path = ROOT / "silver/silver_glue.py"
gold_tree = ast.parse(gold_path.read_text(encoding="utf-8"))
gold_assignments = {"DEFAULTS", "NOMES_SILVER", "NUMERIC", "ANOS_META", "COLUNAS_META", "NIVEIS"}
gold_functions = {"params", "type_matches", "required_columns", "inspect_silver"}
gold_nodes = [node for node in gold_tree.body if (
    isinstance(node, ast.FunctionDef) and node.name in gold_functions
) or (
    isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in gold_assignments for target in node.targets)
)]
gold = {"math": math, "sys": types.SimpleNamespace(argv=["review"]), "re": re,
        "uuid": uuid, "urlparse": urlparse, "json": json, "hashlib": hashlib}
exec(compile(ast.Module(body=gold_nodes, type_ignores=[]), "gold-contract", "exec"), gold)

manifest = {"status": "SUCCEEDED", "layer": "silver", "is_full_batch": True,
            "run_id": "review_run", "tables": []}
for table, columns in gold["required_columns"]().items():
    fields = [{"name": name, "type": "long" if kind == "numeric" and name == "ano"
               else "double" if kind == "numeric" else kind} for name, kind in columns.items()]
    manifest["tables"].append({"table": table, "status": "SUCCEEDED", "counts_match": True,
                              "target_count": 1, "target_uri": f"s3://test/silver/inep/{table}/run_id=review_run/",
                              "schema": {"fields": fields}})


class FakeS3:
    def get_object(self, **_):
        return {"Body": io.BytesIO(json.dumps(manifest).encode())}


gold["boto3"] = types.SimpleNamespace(client=lambda _service: FakeS3())
config = gold["params"]({"SILVER_MANIFEST_URI": "s3://test/auditoria/silver/run_id=review_run/manifest.json"})
check("Gold: valid seven-table Silver contract", lambda: gold["inspect_silver"](None, config))
check("Gold: NaN join limit rejected", lambda: must_reject(lambda: gold["params"]({"LIMIAR_JOIN": "nan"})))
check("Gold: Bronze output prefix rejected", lambda: must_reject(lambda: gold["params"]({"GOLD_PREFIX": "bronze/inep"})))
check("Gold: incompatible Brazil series rejected", lambda: must_reject(lambda: gold["params"]({"SERIE_BRASIL": "3"})))
good = copy.deepcopy(manifest)
manifest["tables"][0]["schema"]["fields"] = []
check("Gold: missing required fields rejected", lambda: must_reject(lambda: gold["inspect_silver"](None, config)))
manifest = copy.deepcopy(good)
manifest["tables"][0]["target_uri"] = manifest["tables"][0]["target_uri"].replace("review_run", "another_run")
check("Gold: mixed Silver run rejected", lambda: must_reject(lambda: gold["inspect_silver"](None, config)))
manifest = copy.deepcopy(good)
manifest["status"] = "FAILED"
check("Gold: failed Silver manifest rejected", lambda: must_reject(lambda: gold["inspect_silver"](None, config)))
manifest = copy.deepcopy(good)
manifest["tables"] = manifest["tables"][:-1]
check("Gold: incomplete Silver table set rejected", lambda: must_reject(lambda: gold["inspect_silver"](None, config)))

silver_tree = ast.parse(silver_path.read_text(encoding="utf-8"))
silver_nodes = [node for node in silver_tree.body if isinstance(node, ast.FunctionDef)
                and node.name in {"parse_s3_uri", "validate_bronze_manifest"}]
silver = {"re": re, "urlsplit": urlsplit, "TABLES": tuple(gold["NOMES_SILVER"])}
exec(compile(ast.Module(body=silver_nodes, type_ignores=[]), "silver-contract", "exec"), silver)
bronze = json.loads((ROOT / "manifesto_bronze.json").read_text(encoding="utf-8"))
check("Silver: actual preserved Bronze manifest accepted", lambda: silver["validate_bronze_manifest"](bronze))
bad = copy.deepcopy(bronze)
bad["table_metrics"][0]["target_uri"] = "s3://test/bronze/alunos/run_id=wrong/"
check("Silver: wrong Bronze run URI rejected", lambda: must_reject(lambda: silver["validate_bronze_manifest"](bad)))
bad = copy.deepcopy(bronze)
bad["all_counts_match"] = False
check("Silver: nonvalidated Bronze counts rejected", lambda: must_reject(lambda: silver["validate_bronze_manifest"](bad)))
check("Silver: malformed S3 URI rejected", lambda: must_reject(lambda: silver["parse_s3_uri"]("s3://test/silver/../bronze/file")))

report = {
    "scope": "Independent static and pure manifest-contract checks. No AWS/Spark execution. Synthetic Silver fixture; actual Bronze local manifest.",
    "script_sha256": {"gold": hashlib.sha256(gold_path.read_bytes()).hexdigest(),
                      "silver": hashlib.sha256(silver_path.read_bytes()).hexdigest()},
    "results": results, "passed": all(result["passed"] for result in results),
}
(ROOT / "verification/independent_contract_results.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(report, ensure_ascii=False, indent=2))
if not report["passed"]:
    raise SystemExit(1)
