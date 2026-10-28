from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import unittest

import analyze_gcp_closure as probe

ROOT=Path(__file__).resolve().parents[3]


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gcp=json.loads((ROOT/'outputs/finops_pendencias_20261027/raw/gcp_existing_metrics.json').read_text(encoding='utf-8'))
        cls.bronze=json.loads((ROOT/'outputs/manifesto_bronze.json').read_text(encoding='utf-8'))

    def test_real_metadata_reconciles_seven_and_prospective_gross_only(self):
        report=probe.analyze(self.gcp,self.bronze)
        self.assertTrue(report['reconciliation']['all_seven_compatible'])
        self.assertEqual(report['reconciliation']['total_source_rows'],3902954)
        self.assertEqual(report['reconciliation']['logical_bytes'],271547681)
        self.assertEqual(Decimal(report['storage_read_projection']['gross_usd_30d']),Decimal(4*271547681)/Decimal(2**40)*Decimal('1.10'))
        self.assertIsNone(report['storage_read_projection']['historical_actual_scanned_bytes'])
        self.assertFalse(report['storage_read_projection']['free_read_tier_applied'])

    def test_egress_stays_pending_despite_numeric_api_metric(self):
        report=probe.analyze(self.gcp,self.bronze)
        self.assertEqual(report['api_telemetry']['methods']['ReadRows']['api_response_sizes_distribution_sum_bytes'],'1860997.0')
        self.assertEqual(report['api_telemetry']['methods']['ReadRows']['completed_request_count'],'13')
        self.assertIsNone(report['egress']['billed_network_bytes'])
        self.assertIsNone(report['egress']['projected_egress_usd_30d'])
        self.assertFalse(report['egress']['valid_upper_bound_established'])
        self.assertTrue(report['latest_collection']['monitoring_now_available'])
        self.assertFalse(report['latest_collection']['billing_enabled_current'])

    def test_changed_rows_or_columns_block_closure(self):
        for field in ('numRows','top_level_column_count'):
            changed=deepcopy(self.gcp)
            changed['source_table_metadata']['tables'][0]['metadata'][field]='1'
            report=probe.analyze(changed,self.bronze)
            self.assertFalse(report['storage_read_projection']['close_projection_line'])
            self.assertIsNone(report['storage_read_projection']['gross_usd_30d'])

    def test_newer_snapshot_streaming_or_non_table_blocks_closure(self):
        for field,value in (('lastModifiedTime','1794000000000'),('type','VIEW'),
                            ('streamingBuffer',{'estimatedBytes':'10'})):
            changed=deepcopy(self.gcp)
            changed['source_table_metadata']['tables'][0]['metadata'][field]=value
            self.assertFalse(probe.analyze(changed,self.bronze)['storage_read_projection']['close_projection_line'])

    def test_missing_duplicate_tables_and_negative_quantity_rejected(self):
        changed=deepcopy(self.gcp)
        changed['source_table_metadata']['tables'][0]['table']='dicionario'
        with self.assertRaises(ValueError):probe.analyze(changed,self.bronze)
        with self.assertRaises(ValueError):probe.exact_int('-1')
        with self.assertRaises(ValueError):probe.analyze(self.gcp,self.bronze,cycles=-1)


if __name__=='__main__':unittest.main()
