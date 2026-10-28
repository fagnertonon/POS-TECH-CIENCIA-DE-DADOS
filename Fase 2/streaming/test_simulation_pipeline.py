"""Falhas entre etapas, concorrência e isolamento da simulação; sem rede."""
import gzip
import io
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import threading
import unittest
from unittest.mock import patch

import lambda_function as c
from test_lambda_function import FakeS3, S3Error, envelope, record


class AtomicS3(FakeS3):
    def __init__(self, fail_at=None):
        super().__init__()
        self.lock = threading.Lock()
        self.fail_at = fail_at

    def put_object(self, **kwargs):
        with self.lock:
            if self.fail_at and kwargs['Key'].startswith(self.fail_at):
                self.fail_at = None
                raise S3Error('ServiceUnavailable')
            return super().put_object(**kwargs)


class SimulationTests(unittest.TestCase):
    def setUp(self):
        from types import SimpleNamespace
        self.config = c.Config('fiap-tc-fase2-2026', 'bronze/inep_streaming',
                               'auditoria/streaming/idempotency', frozenset({'alunos'}))
        self.context = SimpleNamespace(aws_request_id='req')

    def consume(self, s3, ev=None):
        return c.process_record(record(ev), self.context, self.config, s3)

    def test_retry_repairs_every_interrupted_projection_and_ack_only_after_receipt(self):
        for prefix, prior_count in [('silver/', 2), ('gold/', 3), ('auditoria/streaming/processed/', 4)]:
            with self.subTest(prefix=prefix):
                s3 = AtomicS3(prefix)
                with self.assertRaises(S3Error):
                    self.consume(s3)
                self.assertEqual(len(s3.objects), prior_count)
                status, _, latency = self.consume(s3)
                self.assertEqual(status, 'ACCEPTED')
                self.assertIsNotNone(latency)
                before = dict(s3.objects)
                self.assertEqual(self.consume(s3)[0], 'DUPLICATE')
                self.assertEqual(s3.objects, before)
                self.assertEqual(len(s3.objects), 5)

    def test_concurrent_same_event_has_one_logical_effect(self):
        s3 = AtomicS3()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.consume(s3), range(10)))
        self.assertEqual(len(s3.objects), 5)
        self.assertEqual(sum(r[2] is not None for r in results), 1)
        self.assertEqual(sum(r[0] == 'ACCEPTED' for r in results), 1)

    def test_concurrent_different_events_never_overwrite_an_aggregate(self):
        s3 = AtomicS3()
        evs = [dict(envelope(), event_id=f'event_{i}') for i in range(20)]
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda ev: self.consume(s3, ev), evs))
        gold = [json.loads(gzip.decompress(v))['data'] for (_, k), v in s3.objects.items() if k.startswith('gold/')]
        self.assertEqual(len(gold), 20)
        self.assertEqual(sum(r['eventos'] for r in gold), 20)
        self.assertAlmostEqual(sum(r['soma_peso_avaliados'] for r in gold), 26.6)

    def test_invalid_semantics_preserves_bronze_and_durable_quality_failure(self):
        for field, value in [('peso_aluno', 0), ('peso_aluno', -1), ('peso_aluno', None),
                             ('peso_aluno', True), ('presenca', 'x'), ('alfabetizado', None),
                             ('id_municipio', '9900000'), ('rede', '5'), ('serie', '3'),
                             ('rede', []), ('presenca', {}), ('peso_aluno', 10**400)]:
            with self.subTest(field=field, value=value):
                s3, ev = AtomicS3(), envelope()
                ev['payload'][field] = value
                with self.assertRaises(c.InvalidPayload):
                    self.consume(s3, ev)
                self.assertEqual(len(s3.objects), 3)  # claim, Bronze, quality rejection
                self.assertFalse(any(k.startswith(('silver/', 'gold/')) for _, k in s3.objects))
                self.assertTrue(any('rejected_event_id=' in k for _, k in s3.objects))

    def test_absent_pupil_keeps_null_literacy_and_no_weight_contribution(self):
        ev = envelope()
        ev['payload'].update(presenca='0', preenchimento_caderno=None, alfabetizado=None, peso_aluno=None)
        silver, gold = c.transform_payload(ev['payload'])
        self.assertFalse(silver['avaliado'])
        self.assertIsNone(silver['alfabetizado_avaliado'])
        self.assertEqual(gold['soma_peso_avaliados'], 0)
        self.assertEqual(gold['alunos_avaliados'], 0)

    def test_tampered_derived_or_receipt_is_not_acknowledged(self):
        for prefix in ['silver/', 'gold/', 'auditoria/streaming/processed/']:
            with self.subTest(prefix=prefix):
                s3 = AtomicS3()
                self.consume(s3)
                key = next(k for k in s3.objects if k[1].startswith(prefix))
                compressed = key[1].endswith('.gz')
                obj = json.loads(gzip.decompress(s3.objects[key]) if compressed else s3.objects[key])
                if compressed:
                    obj['data']['ano'] = 2023
                else:
                    obj['end_to_end_latency_seconds'] = 0
                raw = c.canonical_json(obj)
                s3.objects[key] = gzip.compress(raw) if compressed else raw
                with self.assertRaises(c.StoredEventCorruption):
                    self.consume(s3)

    def test_gold_clock_read_after_durable_gold_put(self):
        s3 = AtomicS3()
        before, after = [datetime(2026,10,20,18,0,s,tzinfo=timezone.utc) for s in (1,12)]
        def clock():
            return after if any(k.startswith('gold/') for _, k in s3.objects) else before
        with patch.object(c, 'utc_now', side_effect=clock):
            status, stored, latency = self.consume(s3)
        self.assertEqual(latency, 12)
        self.assertEqual(stored['analytical_receipt']['completed_at_utc'], c.iso_utc(after))

    def test_future_emission_cannot_be_reported_as_zero_latency(self):
        s3, ev = AtomicS3(), envelope()
        ev['event_time'] = '2026-10-22T18:00:00Z'
        with patch.object(c, 'utc_now', return_value=datetime(2026,10,20,18,tzinfo=timezone.utc)):
            with self.assertRaises(c.InvalidEnvelope):
                self.consume(s3, ev)
        self.assertEqual(s3.objects, {})

    def test_json_boolean_is_not_equal_to_numeric_contribution(self):
        s3 = AtomicS3()
        self.consume(s3)
        key = next(k for k in s3.objects if k[1].startswith('gold/'))
        obj = json.loads(gzip.decompress(s3.objects[key]))
        obj['data']['eventos'] = True
        s3.objects[key] = gzip.compress(c.canonical_json(obj))
        with self.assertRaises(c.StoredEventCorruption):
            self.consume(s3)

    def test_official_prefix_configuration_is_blocked(self):
        from dataclasses import replace
        for field, value in [('prefix','bronze/inep'),('silver_prefix','silver/inep'),
                             ('gold_prefix','gold/inep'),('processed_prefix','auditoria/gold')]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                replace(self.config, **{field: value})


if __name__ == '__main__':
    unittest.main()
