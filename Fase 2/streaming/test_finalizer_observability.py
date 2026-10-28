"""Execution timing and failure persistence, with in-memory AWS clients."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import finalize_simulation as f
from test_finalize_simulation import FakeS3, S3Error, example_event, install_event, encode_for_test


class ObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.s3=FakeS3()
        self.events=[example_event(),example_event(2,alphabetized=False,weight=1.0)]
        for event in self.events:install_event(self.s3,event)

    def run_observed(self,**kwargs):
        return f.finalize_observed(self.s3,self.events,encoder=encode_for_test,**kwargs)

    def audits(self):
        return [json.loads(v) for (bucket,key),v in self.s3.objects.items() if key.startswith(f.EXECUTION_PREFIX+'/')]

    def test_success_persists_utc_monotonic_duration_counts_and_stages(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder=Path(tmp)/'new'
            report=self.run_observed(output_dir=folder)
            metrics=report['execution_metrics']
            self.assertEqual(metrics,self.audits()[0])
            self.assertEqual(json.loads((folder/'execution_metrics.json').read_bytes()),metrics)
            self.assertEqual(json.loads((folder/'finalization_report.json').read_bytes())['execution_metrics'],metrics)
        self.assertEqual(metrics['status'],'SUCCEEDED')
        self.assertEqual(metrics['output_silver_rows'],2)
        self.assertEqual(metrics['output_gold_groups'],1)
        self.assertEqual([s['stage'] for s in metrics['stages']],
                         ['validate_and_read','encode_parquet','publish_silver','publish_gold','publish_manifest'])
        self.assertTrue(all(s['status']=='SUCCEEDED' and s['duration_seconds']>=0 for s in metrics['stages']))
        self.assertGreaterEqual(metrics['duration_seconds'],sum(s['duration_seconds'] for s in metrics['stages']))
        self.assertGreater(f._timestamp(metrics['started_at_utc']),f._timestamp(self.events[0]['event_time']))

    def test_retry_creates_new_audit_but_keeps_identical_analytical_snapshot(self):
        first=self.run_observed()
        analytical={k:v for k,v in self.s3.objects.items() if not k[1].startswith(f.EXECUTION_PREFIX+'/')}
        second=self.run_observed()
        self.assertNotEqual(first['execution_metrics']['attempt_id'],second['execution_metrics']['attempt_id'])
        self.assertEqual(first['manifest_key'],second['manifest_key'])
        self.assertEqual(first['datasets'],second['datasets'])
        self.assertEqual(analytical,{k:v for k,v in self.s3.objects.items() if not k[1].startswith(f.EXECUTION_PREFIX+'/')})
        self.assertEqual(len(self.audits()),2)

    def test_failed_publish_records_failure_and_does_not_publish_manifest(self):
        self.s3.fail_gold_once=True
        with tempfile.TemporaryDirectory() as tmp:
            folder=Path(tmp)/'failed'
            with self.assertRaises(S3Error):self.run_observed(output_dir=folder)
            self.assertEqual(json.loads((folder/'execution_metrics.json').read_bytes())['status'],'FAILED')
        metrics=self.audits()[0]
        self.assertEqual(metrics['error_type'],'S3Error')
        self.assertEqual(metrics['stages'][-1]['stage'],'publish_gold')
        self.assertEqual(metrics['stages'][-1]['status'],'FAILED')
        self.assertFalse(any(k.endswith('/manifest.json') for _,k in self.s3.objects))

    def test_quality_failure_has_no_new_analytical_publication_and_is_not_swallowed(self):
        del self.s3.objects[(f.BUCKET,f.event_keys(self.events[0])['receipt_key'])]
        before=set(self.s3.objects)
        with self.assertRaises(f.FinalizationError):self.run_observed()
        self.assertEqual(self.audits()[0]['status'],'FAILED')
        self.assertTrue(all(key.startswith(f.EXECUTION_PREFIX+'/') for _,key in set(self.s3.objects)-before))

    def test_audit_write_error_is_visible_not_success(self):
        original=self.s3.put_object
        def deny(**kwargs):
            if kwargs['Key'].startswith(f.EXECUTION_PREFIX+'/'):raise S3Error('AccessDenied')
            return original(**kwargs)
        with tempfile.TemporaryDirectory() as tmp,patch.object(self.s3,'put_object',side_effect=deny):
            folder=Path(tmp)/'new'
            with self.assertRaisesRegex(f.FinalizationError,'execution metrics could not be persisted'):
                self.run_observed(output_dir=folder)
            self.assertEqual(json.loads((folder/'audit_persistence_failure.json').read_bytes())['status'],'FAILED')

    def test_cli_uses_observed_path_and_actual_snappy_encoder(self):
        with tempfile.TemporaryDirectory() as tmp:
            events=Path(tmp)/'events.jsonl'
            events.write_bytes(b'\n'.join(f.canonical_json(e) for e in self.events)+b'\n')
            out=Path(tmp)/'evidence'
            with patch('boto3.client',return_value=self.s3),contextlib.redirect_stdout(io.StringIO()) as captured:
                f.main(['--events',str(events),'--output-dir',str(out)])
            report=json.loads(captured.getvalue())
            self.assertEqual(report['execution_metrics']['status'],'SUCCEEDED')
            self.assertTrue((out/'gold.parquet').read_bytes().startswith(b'PAR1'))
            self.assertEqual(len(self.audits()),1)

    def test_cli_failure_sends_sanitized_notice_and_still_fails(self):
        del self.s3.objects[(f.BUCKET,f.event_keys(self.events[0])['receipt_key'])]
        notices=[]
        class SNS:
            def publish(self,**kwargs):notices.append(kwargs);return {'MessageId':'test-message'}
        with tempfile.TemporaryDirectory() as tmp:
            events=Path(tmp)/'events.jsonl'
            events.write_bytes(b'\n'.join(f.canonical_json(e) for e in self.events)+b'\n')
            out=Path(tmp)/'evidence'
            topic='arn:aws:sns:us-east-2:637246457028:fiap-inep-alertas-operacionais'
            with patch('boto3.client',side_effect=lambda service,**kw:self.s3 if service=='s3' else SNS()):
                with self.assertRaises(f.FinalizationError):
                    f.main(['--events',str(events),'--output-dir',str(out),'--notification-topic-arn',topic])
            notice=json.loads(notices[0]['Message'])
            self.assertEqual(notice['status'],'FAILED')
            self.assertNotIn('payload',notice)
            self.assertEqual(json.loads((out/'failure_notification.json').read_bytes())['status'],'ACCEPTED_BY_SNS')

    def test_notification_destination_cannot_escape_project(self):
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):
            f.main(['--events','not-read','--output-dir','not-created',
                    '--notification-topic-arn','arn:aws:sns:us-east-1:123456789012:other'])


if __name__=='__main__':unittest.main()
