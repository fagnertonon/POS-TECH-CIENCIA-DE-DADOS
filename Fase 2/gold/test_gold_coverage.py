"""Offline tests of national coverage metadata; no Spark, AWS or source reads.

These tests execute the production pure function extracted with AST. They do
not verify the Spark cohort filter, municipality-to-UF projection, joins or
Parquet publication; those require a Spark/Glue integration execution.
"""
import ast
import copy
from pathlib import Path
import unittest
import unicodedata


SOURCE = Path(__file__).with_name("gold_glue.py")


def load_coverage_function():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    nodes = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef)
            and node.name == "resumir_cobertura_brasil")
        or (isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "UF_IBGE"
                    for target in node.targets))
    ]
    scope = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), scope)
    if "resumir_cobertura_brasil" not in scope or "UF_IBGE" not in scope:
        raise AssertionError("Production coverage function and UF_IBGE must exist")
    return scope["resumir_cobertura_brasil"], scope["UF_IBGE"]


def plain_text(value):
    return "".join(character for character in unicodedata.normalize("NFD", value.casefold())
                   if unicodedata.category(character) != "Mn")


class CoverageMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        summarize, mapping = load_coverage_function()
        cls.summarize = staticmethod(summarize)
        cls.mapping = mapping
        cls.all_states = set(mapping.values())

    def test_ibge_geographic_reference_has_all_27_distinct_states(self):
        self.assertEqual(len(self.mapping), 27)
        self.assertEqual(len(self.all_states), 27)
        self.assertEqual(self.mapping["35"], "SP")
        self.assertEqual(self.mapping["53"], "DF")
        self.assertTrue(all(len(code) == 2 and code.isdigit() for code in self.mapping))

    def test_2023_missing_sp_reports_source_coverage_without_changing_rates(self):
        # Actual diagnosed coverage: 23 student UFs, with SP present only in
        # the 24-UF results table. Missing UFs are geographic complements.
        micro = self.all_states - {"AC", "DF", "RR", "SP"}
        results = micro | {"SP"}
        row = self.summarize(2023, "2", micro, results)
        self.assertEqual((row["ano"], row["serie"], row["rede"]), (2023, "2", "5"))
        self.assertEqual(row["quantidade_ufs_microdados"], 23)
        self.assertEqual(row["ufs_sem_microdados"], ["AC", "DF", "RR", "SP"])
        self.assertEqual(row["ufs_resultados_sem_microdados"], ["SP"])
        self.assertEqual(row["ufs_microdados_sem_resultado_uf"], [])
        self.assertEqual(row["status_comparabilidade"], "COBERTURA_DIFERENTE_ENTRE_BASES")
        note = plain_text(row["nota_fonte"])
        self.assertIn("2023", note)
        self.assertTrue("sao paulo" in note or "sp" in note)
        self.assertNotIn("dentro_tolerancia", row)
        self.assertNotIn("taxa_alfabetizacao_calculada", row)

    def test_2023_new_snapshot_with_sp_does_not_keep_missing_sp_exception(self):
        states = sorted(self.all_states - {"AC", "DF", "RR"})
        row = self.summarize(2023, "2", states, states)
        self.assertEqual(row["quantidade_ufs_microdados"], 24)
        self.assertNotIn("SP", row["ufs_sem_microdados"])
        self.assertEqual(row["ufs_resultados_sem_microdados"], [])
        self.assertEqual(row["status_comparabilidade"],
                         "COBERTURA_UF_COINCIDENTE_METODOLOGIA_NAO_COMPROVADA")
        self.assertNotIn("sem sao paulo", plain_text(row["nota_fonte"]))

    def test_2024_df_in_students_without_state_result_is_separate_coverage(self):
        micro = self.all_states - {"RR"}
        results = micro - {"DF"}
        row = self.summarize(2024, "2", micro, results)
        self.assertEqual(row["quantidade_ufs_microdados"], 26)
        self.assertEqual(row["ufs_sem_microdados"], ["RR"])
        self.assertEqual(row["ufs_resultados_sem_microdados"], [])
        self.assertEqual(row["ufs_microdados_sem_resultado_uf"], ["DF"])
        self.assertEqual(row["status_comparabilidade"], "COBERTURA_DIFERENTE_ENTRE_BASES")
        note = plain_text(row["nota_fonte"])
        self.assertTrue("df" in note or "distrito federal" in note)
        self.assertNotIn("2023", note)

    def test_matching_all_27_states_does_not_prove_methodological_equivalence(self):
        row = self.summarize(2025, "2", self.all_states, self.all_states)
        self.assertEqual(row["quantidade_ufs_microdados"], 27)
        self.assertEqual(row["ufs_sem_microdados"], [])
        self.assertEqual(row["ufs_resultados_sem_microdados"], [])
        self.assertEqual(row["ufs_microdados_sem_resultado_uf"], [])
        self.assertEqual(row["status_comparabilidade"],
                         "COBERTURA_UF_COINCIDENTE_METODOLOGIA_NAO_COMPROVADA")

    def test_missing_state_reference_takes_precedence_over_different_sets(self):
        row = self.summarize(2025, "2", ["SP", "RJ"], [])
        self.assertEqual(row["ufs_resultados_uf"], [])
        self.assertEqual(row["ufs_microdados_sem_resultado_uf"], ["RJ", "SP"])
        self.assertEqual(row["status_comparabilidade"], "REFERENCIA_UF_INDISPONIVEL")

    def test_unknown_state_is_rejected_in_either_source(self):
        for micro, results in ((["SP", "XX"], ["SP"]),
                               (["SP"], ["SP", "XX"]),
                               (["35"], ["SP"]),
                               (["SP"], ["53"])):
            with self.subTest(micro=micro, results=results), self.assertRaises(ValueError):
                self.summarize(2025, "2", micro, results)

    def test_lists_are_unique_sorted_and_inputs_remain_unchanged(self):
        micro = ["SP", "RJ", "SP", "MG"]
        results = ["SP", "ES", "MG", "ES"]
        original = copy.deepcopy((micro, results))
        row = self.summarize(2025, "2", micro, results)
        self.assertEqual(row["ufs_microdados"], ["MG", "RJ", "SP"])
        self.assertEqual(row["quantidade_ufs_microdados"], 3)
        self.assertEqual(row["ufs_resultados_uf"], ["ES", "MG", "SP"])
        self.assertEqual(row["ufs_resultados_sem_microdados"], ["ES"])
        self.assertEqual(row["ufs_microdados_sem_resultado_uf"], ["RJ"])
        self.assertEqual(row["ufs_sem_microdados"], sorted(self.all_states - {"MG", "RJ", "SP"}))
        self.assertEqual((micro, results), original)


if __name__ == "__main__":
    unittest.main(verbosity=2)
