import unittest
from datetime import datetime, timezone
import collect_log_growth_readonly as c


class Fake:
    def __init__(self, **responses):
        self.responses = responses

    def __getattr__(self, name):
        if name not in self.responses:
            raise AttributeError(name)
        def call(**kwargs):
            response = self.responses[name]
            return response(kwargs) if callable(response) else response
        return call


class Tests(unittest.TestCase):
    def test_read_allowlist(self):
        r = c.Reader({})
        for service, operation in [("logs", "put_storage_tier_policy"),
                                   ("logs", "start_query"), ("glue", "start_job_run"),
                                   ("ce", "get_cost_and_usage")]:
            with self.assertRaises(ValueError):
                r.call(service, operation)
        self.assertEqual(r.calls, [])

    def test_absent_metric_not_zero(self):
        r = c.Reader({"cloudwatch": Fake(get_metric_data={"MetricDataResults": [
            {"Id": "i0", "StatusCode": "Complete", "Timestamps": [], "Values": []}]})})
        q = c.query("i0", "IncomingBytes", [], 300, "Sum")
        t = datetime(2026, 10, 27, tzinfo=timezone.utc)
        result = c.metric_data(r, [q], t, t)
        self.assertIsNone(result["series"][0]["observed_sum"])
        self.assertFalse(result["series"][0]["has_data"])
        self.assertFalse(result["missing_is_zero"])

    def test_pagination_not_duplicated_and_caps_visible(self):
        responses = iter([{"logGroups": [{"logGroupName": "a"}], "nextToken": "a"},
                          {"logGroups": [{"logGroupName": "b"}], "nextToken": "b"}])
        r = c.Reader({"logs": Fake(describe_log_groups=lambda _: next(responses))})
        result = c.paginate(r, "logs", "describe_log_groups", "logGroups", 2)
        self.assertEqual(len(result["items"]), 2)
        self.assertFalse(result["pagination_complete"])
        self.assertEqual(result["limit_reason"], "PAGE_CAP")

    def test_stock_and_policy_unavailable_are_not_zero(self):
        r = c.Reader({"sts": Fake(get_caller_identity={"Account": "637246457028"}),
                      "logs": Fake(describe_log_groups={"logGroups": [{"logGroupName": "x"}]}),
                      "cloudwatch": Fake(get_metric_data={"MetricDataResults": []},
                                         list_metrics={"Metrics": []})})
        t = datetime(2026, 10, 27, tzinfo=timezone.utc)
        result = c.collect(r, "us-east-2", "637246457028", t, t)
        self.assertIsNone(result["stock_snapshot"]["total_stored_bytes"])
        self.assertFalse(result["storage_tier_policy"]["available"])
        self.assertIsNone(result["storage_tier_policy"]["response"])
        self.assertFalse(result["scope"]["stock_increment_attributed_to_a_cycle"])

    def test_stock_observations_never_summed(self):
        t = datetime(2026, 10, 27, tzinfo=timezone.utc)
        r = c.Reader({"cloudwatch": Fake(get_metric_data={"MetricDataResults": [
            {"Id": "s0", "StatusCode": "Complete", "Timestamps": [t], "Values": [12]}]})})
        result = c.metric_data(r, [c.query("s0", "StoredBytes", [], 86400, "Average")], t, t)
        self.assertNotIn("observed_sum", result["series"][0])


if __name__ == "__main__":
    unittest.main()
