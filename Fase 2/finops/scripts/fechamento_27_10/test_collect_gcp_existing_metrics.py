import json
import unittest

import collect_gcp_existing_metrics as probe


class FakeAPI:
    def __init__(self, billing_error=False):
        self.calls, self.requests, self.billing_error = 0, [], billing_error

    def request(self, url, params=None, body=None):
        self.calls += 1
        self.requests.append((url, params, body))
        if url.endswith('/v1/projects/'+probe.PROJECT):
            return {'projectId':probe.PROJECT,'projectNumber':'947840284476','lifecycleState':'ACTIVE'}, None
        if url.endswith('/billingInfo'):
            return {'projectId':probe.PROJECT,'billingEnabled':False}, None
        if url.endswith(':testIamPermissions'):
            return {'permissions':['logging.logEntries.list']}, None
        if url.endswith('/consumerQuotaMetrics'):
            return {'metrics':[{'metric':'bigquerystorage.googleapis.com/read_requests',
                                'consumerQuotaLimits':[{'unit':'1/min/{project}'}]}]}, None
        if url.endswith('/metricDescriptors'):
            if self.billing_error:
                return None, {'http_status':403,'status':'PERMISSION_DENIED','billing_required':True}
            if 'serviceruntime' in params['filter']:
                return {'metricDescriptors':[{'type':'serviceruntime.googleapis.com/api/response_sizes',
                                             'unit':'By','metricKind':'DELTA','valueType':'DISTRIBUTION'}]}, None
            return {'metricDescriptors':[]}, None
        if url.endswith('/timeSeries'):
            return {'timeSeries':[]}, None
        if url.endswith('/sinks'):
            return {'sinks':[{'destination':'storage.googleapis.com/PRIVATE_BUCKET',
                              'filter':'private@example.invalid SECRET','disabled':False}]}, None
        if url.endswith('/buckets'):
            return {'buckets':[{'name':'PRIVATE_PATH','retentionDays':30,'restrictedFields':['SECRET']}]}, None
        if url.endswith('/entries:list'):
            return {'entries':[]}, None
        if '/tables/' in url:
            return {'type':'TABLE','numRows':'10','numBytes':'50','lastModifiedTime':'1790000000000'}, None
        raise AssertionError(url)


class Tests(unittest.TestCase):
    def test_denied_monitoring_stops_repeated_calls_and_never_means_zero(self):
        api=FakeAPI(billing_error=True)
        report=probe.collect(api)
        self.assertEqual(len([r for r in api.requests if r[0].endswith('/metricDescriptors')]),1)
        self.assertFalse(any(r[0].endswith('/timeSeries') for r in api.requests))
        self.assertFalse(report['metric_catalog']['complete'])
        self.assertEqual(report['audit']['measurement_status'],'UNAVAILABLE_NO_BYTE_FIELDS')
        self.assertEqual(report['audit']['documented_numeric_field_sums'],{})
        self.assertFalse(report['billing_metadata']['billingEnabled'])
        for secret in ('PRIVATE_BUCKET','PRIVATE_PATH','private@example.invalid','SECRET'):
            self.assertNotIn(secret,json.dumps(report))

    def test_deduplicates_distribution_points_and_strips_credentials(self):
        point={'interval':{'startTime':'a','endTime':'b'},
               'value':{'distributionValue':{'mean':123.5,'count':'2'}}}
        row={'metricKind':'DELTA','resource':{'labels':{'service':probe.SERVICE,
             'method':'google.cloud.bigquery.storage.v1.BigQueryRead.ReadRows','credential_id':'SECRET'}},
             'metric':{'type':'M'},'points':[point,point]}
        report=probe.summarize_series([row,row],'M')
        item=next(iter(report['by_method'].values()))
        self.assertEqual(item['delta_sum'],'247.0')
        self.assertEqual(item['points'],1)
        self.assertNotIn('SECRET',json.dumps(report))
        self.assertEqual(probe.summarize_series([],'M')['measurement_status'],'UNAVAILABLE_NO_POINTS')

    def test_audit_only_documented_numeric_path_dedup_no_raw(self):
        row={'logName':'private/example','insertId':'ID','timestamp':'NOW',
             'protoPayload':{'methodName':'google.cloud.bigquery.storage.v1.BigQueryRead.ReadRows',
                'metadata':{'tableDataRead':{'scannedBytes':'123','serializedResponseBytes':'45',
                                            'fields':['SECRET'],'readStream':'private@example.invalid'},
                            'unrelated':{'scannedBytes':'9999','private_key':'SECRET'}}}}
        report=probe.summarize_audit([row,row])
        self.assertEqual(report['entries'],1)
        self.assertEqual(report['documented_numeric_field_sums'],{'scannedBytes':123,'serializedResponseBytes':45})
        self.assertNotIn('SECRET',json.dumps(report))
        self.assertNotIn('private@example.invalid',json.dumps(report))

    def test_metadata_only_optional_seven_tables_and_catalog_not_recent_only(self):
        api=FakeAPI()
        report=probe.collect(api,source_metadata=True)
        self.assertEqual(len(report['source_table_metadata']['tables']),7)
        self.assertTrue(all(r[1]['activeOnly']=='false' for r in api.requests if r[0].endswith('/metricDescriptors')))
        self.assertLessEqual(len(json.dumps(report).encode()),probe.MAX_OUTPUT)
        self.assertTrue(all(not r[2] or r[0].endswith((':testIamPermissions','/entries:list')) for r in api.requests))

    def test_window_errors_endpoint_guards_and_sanitized_errors(self):
        with self.assertRaises(ValueError):
            probe.valid_window('2026-10-01T00:00:00','2026-10-02T00:00:00Z')
        with self.assertRaises(ValueError):
            probe.valid_window('2026-09-01T00:00:00Z','2026-10-27T00:00:00Z')
        with self.assertRaises(ValueError):
            probe.ReadOnlyAPI('SECRET').request('https://serviceusage.googleapis.com/v1/projects/x/services/y:enable',body={})
        error=probe.error_summary(403,{'error':{'status':'PERMISSION_DENIED',
            'message':'Billing disabled for project #947840284476 https://bad.invalid SECRET private@example.invalid'}})
        self.assertTrue(error['billing_required'])
        self.assertEqual(error['consumer_project_number'],'947840284476')
        self.assertNotIn('SECRET',json.dumps(error))
        self.assertIsNone(probe.safe('https://bad.invalid/SECRET'))

    def test_pagination_cap_explicit(self):
        class Endless:
            def request(self,*args): return {'entries':[], 'nextPageToken':'again'},None
        values, meta=probe.pages(Endless(),'unused','entries',body={})
        self.assertEqual(values,[])
        self.assertFalse(meta['complete'])

    def test_source_metadata_allowlist_no_raw_schema_or_payload(self):
        result=probe.summarize_table_metadata({'type':'TABLE','numBytes':'500','numRows':'7',
             'location':'US','schema':{'fields':[{'name':'SECRET','type':'STRING'}]},
             'private_key':'SECRET','streamingBuffer':{'estimatedBytes':'2','private_key':'SECRET'}})
        self.assertEqual(result['numBytes'],'500')
        self.assertEqual(result['top_level_column_count'],1)
        self.assertEqual(result['streamingBuffer'],{'estimatedBytes':'2'})
        self.assertNotIn('SECRET',json.dumps(result))


if __name__=='__main__':
    unittest.main()
