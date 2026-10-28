"""Request attribution must not turn shared CloudWatch activity into run costs."""
import json
import unittest

import capture_runtime_evidence as evidence


def line(message, when, stream="stream-a"):
    return {"message": message, "timestamp": when, "logStreamName": stream, "eventId": f"{stream}-{when}"}


def event(run=None, message_id="normal-message", status="ACCEPTED"):
    value = {"kind": "STREAMING_EVENT", "status": status, "sqs_message_id": message_id}
    if run:
        value["run_id"] = run
    else:
        value["error_type"] = "InvalidPayload"
    return json.dumps(value)


def report(request, duration=4, billed=5, memory=70):
    return (f"REPORT RequestId: {request}\tDuration: {duration} ms\tBilled Duration: {billed} ms"
            f"\tMemory Size: 256 MB\tMax Memory Used: {memory} MB\tInit Duration: 100.50 ms")


class RuntimeEvidenceTests(unittest.TestCase):
    def test_only_target_requests_contribute_report_totals(self):
        logs = [line("START RequestId: related Version: $LATEST", 1), line(event("run-1"), 2),
                line("END RequestId: related", 3), line(report("related"), 4),
                line("START RequestId: unrelated Version: $LATEST", 5), line(event("another-run"), 6),
                line("END RequestId: unrelated", 7), line(report("unrelated", 999, 1000, 200), 8),
                line("START RequestId: negative Version: $LATEST", 1, "stream-b"),
                line(event(message_id="invalid-1", status="FAILED"), 2, "stream-b"),
                line("END RequestId: negative", 3, "stream-b"), line(report("negative", 2, 3, 60), 4, "stream-b")]
        result = evidence.summarize_scoped_logs(logs, "run-1", "invalid-1")
        totals = result["exclusive_target_invocation_totals"]
        self.assertEqual(totals["invocations_with_report"], 2)
        self.assertEqual(totals["duration_ms_sum"], 6)
        self.assertEqual(totals["billed_duration_ms_sum"], 8)
        self.assertEqual(totals["max_memory_used_mb"], 70)
        self.assertEqual(result["positive_run"]["status_counts"], {"ACCEPTED": 1})
        self.assertEqual(result["invalid_message"]["invalid_payload_failures"], 1)
        self.assertEqual(result["correlated_invocations"][0]["report"]["init_duration_ms"], 100.5)

    def test_mixed_batch_and_missing_start_cannot_be_exclusively_attributed(self):
        logs = [line("START RequestId: mixed Version: $LATEST", 1), line(event("run-1"), 2),
                line(event("another-run"), 3), line("END RequestId: mixed", 4), line(report("mixed"), 5),
                line(event("run-1"), 6), line(report("unscoped", 500, 501), 7)]
        result = evidence.summarize_scoped_logs(logs, "run-1", "invalid-1")
        self.assertEqual(result["exclusive_target_invocation_totals"]["invocations_with_report"], 0)
        self.assertIsNone(result["exclusive_target_invocation_totals"]["billed_duration_ms_sum"])
        self.assertEqual(len(result["correlated_invocations"]), 1)
        self.assertTrue(result["correlated_invocations"][0]["contains_unrelated_records"])
        self.assertEqual(len(result["unassigned_target_records"]), 1)
        self.assertEqual(len(result["positive_run"]["events"]), 2)

    def test_log_pagination_deduplicates_and_is_bounded(self):
        first = line("first", 1)
        second = line("second", 2)

        class Logs:
            calls = 0

            def filter_log_events(self, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return {"events": [first], "nextToken": "page-2"}
                self.assert_token = kwargs["nextToken"]
                return {"events": [first, second], "nextToken": "page-2"}

        client = Logs()
        events, complete = evidence.collect_log_events(client, {"logGroupName": "group"})
        self.assertEqual(events, [first, second])
        self.assertTrue(complete)
        self.assertEqual(client.calls, 2)
        self.assertEqual(client.assert_token, "page-2")
        client = Logs()
        events, complete = evidence.collect_log_events(client, {}, max_pages=1)
        self.assertFalse(complete)
        self.assertEqual(client.calls, 1)


if __name__ == "__main__":
    unittest.main()
