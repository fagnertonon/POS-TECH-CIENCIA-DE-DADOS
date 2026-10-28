"""Testa o contrato de snapshot sem depender de PySpark ou credenciais AWS."""
from copy import deepcopy
import json
from pathlib import Path
import unittest

from silver_glue import validate_bronze_manifest, parse_config


class BronzeSnapshotGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.actual = json.loads((Path(__file__).resolve().parents[1] / 'manifesto_bronze.json').read_text(encoding='utf-8'))

    def test_accepts_actual_validated_seven_table_snapshot(self):
        entries = validate_bronze_manifest(self.actual)
        self.assertEqual(7, len(entries))
        self.assertEqual(3867999, entries['alunos']['target_count'])

    def test_rejects_pilot_even_if_seven_paths_were_supplied(self):
        candidate = deepcopy(self.actual)
        candidate['is_full_batch'] = False
        with self.assertRaises(ValueError):
            validate_bronze_manifest(candidate)

    def test_rejects_mixed_execution_paths(self):
        candidate = deepcopy(self.actual)
        entry = candidate['table_metrics'][0]
        entry['target_uri'] = entry['target_uri'].replace(candidate['run_id'], 'another_run')
        with self.assertRaises(ValueError):
            validate_bronze_manifest(candidate)

    def test_rejects_mismatched_source_and_destination_count(self):
        candidate = deepcopy(self.actual)
        candidate['table_metrics'][0]['source_count'] += 1
        with self.assertRaises(ValueError):
            validate_bronze_manifest(candidate)

    def test_rejects_repeated_table_hiding_missing_entity(self):
        candidate = deepcopy(self.actual)
        candidate['table_metrics'][-1] = candidate['table_metrics'][0]
        with self.assertRaises(ValueError):
            validate_bronze_manifest(candidate)

    def test_rejects_failed_manifest(self):
        candidate = deepcopy(self.actual)
        candidate['status'] = 'FAILED'
        with self.assertRaises(ValueError):
            validate_bronze_manifest(candidate)

    def test_catalog_registration_can_be_disabled_without_changing_snapshot(self):
        config = parse_config(['--JOB_NAME', 'fiap-silver', '--REGISTER_CATALOG', 'false'])
        self.assertFalse(config.register_catalog)
        self.assertIn('/manifest.json', config.bronze_manifest_uri)


if __name__ == '__main__':
    unittest.main()
