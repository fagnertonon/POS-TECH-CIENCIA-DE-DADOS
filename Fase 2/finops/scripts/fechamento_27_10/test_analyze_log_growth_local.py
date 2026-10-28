import unittest
from copy import deepcopy
from datetime import datetime, timezone
import analyze_log_growth_local as a


class Tests(unittest.TestCase):
    def test_missing_metric_is_not_zero(self):
        self.assertIsNone(a.observed_sum([]))
        self.assertEqual(a.observed_sum([{"value": 0}]), 0)

    def test_replay_bucket_overlap_does_not_get_prorated(self):
        points = [{"timestamp": "2026-10-27T15:50:00Z", "value": 55325},
                  {"timestamp": "2026-10-27T15:55:00Z", "value": 4096}]
        result = a.buckets(points, 300, a.dt("2026-10-27T15:54:17.722242Z"),
                           a.dt("2026-10-27T16:00:16.337323Z"))
        self.assertEqual(result["wholly_contained"]["sum_observed"], 4096)
        self.assertEqual(result["partly_overlapping"]["sum_observed"], 55325)
        self.assertIsNone(result["outside"]["sum_observed"])

    def test_missing_stock_does_not_become_zero(self):
        snap = a.stock_summary("x", {"groups": [{"logGroupName": "x"}]})
        self.assertIsNone(snap["stock_bytes_reported"])

    def test_prepared_values_checked_against_raw(self):
        record = {"pages": [{"MetricDataResults": [{"Id": "i0",
            "Timestamps": ["2026-10-27T15:50:00Z"], "Values": [10]}]}],
            "series": [{"id": "i0", "points": [
                {"timestamp": "2026-10-27T15:50:00Z", "value": 11}]}]}
        with self.assertRaises(ValueError):
            a.verify_raw_metric_values(record)
        record["series"][0]["points"][0]["value"] = 10
        a.verify_raw_metric_values(record)

    def test_changed_scope_does_not_create_a_net_increment(self):
        x = a.stock_summary("x", {"account":"a","region":"r",
            "capture_end_utc":"2026-10-27T10:00:00Z", "groups":[{"logGroupName":"a","storedBytes":10}]})
        y = a.stock_summary("y", {"account":"a","region":"r",
            "capture_end_utc":"2026-10-27T11:00:00Z", "groups":[]})
        result = a.compare_stock(x, y)
        self.assertIsNone(result["net_stock_delta_bytes"])
        self.assertIsNone(result["generated_bytes_or_per_cycle_compressed_bytes"])


if __name__ == "__main__":
    unittest.main()
