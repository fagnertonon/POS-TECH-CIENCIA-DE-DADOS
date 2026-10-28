"""Read-only FIAP log storage follow-up. No job, query, retention or billing changes."""
import argparse
import base64
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
import textwrap

import boto3
from botocore.config import Config

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output-dir', type=Path, required=True)
parser.add_argument('--baseline-dir', type=Path, default=Path('/home/cloudshell-user/finops_20261027_01'))
args = parser.parse_args()
out = args.output_dir.expanduser().resolve()
out.mkdir(parents=True, exist_ok=False)
baseline_dir = args.baseline_dir.expanduser().resolve()
baseline = json.loads((baseline_dir / 'log_groups.json').read_text())
scope = json.loads((baseline_dir / 'collection_scope.json').read_text())
start = datetime.fromisoformat(scope['observed_at_utc'])
session = boto3.Session(region_name='us-east-2')
config = Config(retries={'mode': 'standard', 'max_attempts': 2}, connect_timeout=10, read_timeout=25)
clients = {}
def client(service):
    if service not in clients:
        clients[service] = session.client(service, config=config)
    return clients[service]

assert client('sts').get_caller_identity()['Account'] == '637246457028'
errors = []
calls = {}
def invoke(service, operation, **params):
    key = service + '.' + operation
    calls[key] = calls.get(key, 0) + 1
    return getattr(client(service), operation)(**params)

def attempt(service, operation, default=None, **params):
    try:
        return invoke(service, operation, **params)
    except Exception as error:
        errors.append({'operation': service + '.' + operation, 'error_type': type(error).__name__, 'message': str(error)[:700]})
        return default

def save(name, value):
    (out / name).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')

observed_start = datetime.now(timezone.utc)
groups = []
token = None
while True:
    params = {'nextToken': token} if token else {}
    response = invoke('logs', 'describe_log_groups', **params)
    groups.extend(g for g in response.get('logGroups', []) if g['logGroupName'].startswith('/aws-glue/') or g['logGroupName'] == '/aws/lambda/fiap-inep-streaming-consumer')
    token = response.get('nextToken')
    if not token:
        break
observed_end = datetime.now(timezone.utc)
save('log_groups_current.json', groups)
save('log_groups_baseline.json', baseline)

incoming = []
last_streams = []
period = 300 if observed_end - start <= timedelta(days=2) else 3600
for group in groups:
    name = group['logGroupName']
    metric = attempt('cloudwatch', 'get_metric_statistics', Namespace='AWS/Logs', MetricName='IncomingBytes',
                     Dimensions=[{'Name': 'LogGroupName', 'Value': name}], StartTime=start, EndTime=observed_end,
                     Period=period, Statistics=['Sum'], Unit='Bytes')
    incoming.append({'log_group': name, 'query_start_utc': start, 'query_end_exclusive_utc': observed_end,
                     'period_seconds': period, 'response': metric})
    streams = attempt('logs', 'describe_log_streams', logGroupName=name, orderBy='LastEventTime', descending=True, limit=1)
    last_streams.append({'log_group': name, 'response': streams})
save('incoming_bytes_since_baseline.json', incoming)
save('last_stream_metadata.json', last_streams)

# Use existing historical storage metrics only. Do not enable Intelligent Tiering.
storage_metrics = []
token = None
while True:
    params = {'Namespace': 'AWS/Logs', 'MetricName': 'StoredBytes'}
    if token:
        params['NextToken'] = token
    response = attempt('cloudwatch', 'list_metrics', **params)
    if response is None:
        break
    storage_metrics.extend(m for m in response.get('Metrics', []) if any(d.get('Name') == 'LogGroupName' and d.get('Value') in {g['logGroupName'] for g in groups} for d in m.get('Dimensions', [])))
    token = response.get('NextToken')
    if not token:
        break
historical_storage = []
for metric in storage_metrics:
    response = attempt('cloudwatch', 'get_metric_statistics', **metric, StartTime=observed_end - timedelta(days=14),
                       EndTime=observed_end, Period=86400, Statistics=['Average', 'Maximum', 'SampleCount'])
    historical_storage.append({'metric': metric, 'response': response})
save('existing_stored_bytes_metrics.json', historical_storage)

recent_runs = []
for name in ['fiap-bronze-inep-batch', 'fiap-silver-inep-batch', 'fiap-gold-inep-batch', 'fiap-ingestao-bigquery-uf']:
    response = attempt('glue', 'get_job_runs', JobName=name, MaxResults=25)
    recent_runs.append({'job_name': name, 'runs': [{k: r[k] for k in ['Id', 'JobRunState', 'StartedOn', 'CompletedOn', 'ExecutionTime', 'DPUSeconds'] if k in r}
                                               for r in (response or {}).get('JobRuns', [])]})
save('recent_glue_runs.json', recent_runs)
sessions = attempt('glue', 'list_sessions', MaxResults=25)
save('recent_session_metadata.json', {'sessions': [{k: s[k] for k in ['Id', 'Status', 'CreatedOn', 'CompletedOn', 'ExecutionTime', 'DPUSeconds'] if k in s}
                                               for s in (sessions or {}).get('Sessions', [])], 'next_token_present': bool((sessions or {}).get('NextToken'))})

trail = []
for event_name in ['DeleteLogGroup', 'DeleteLogStream', 'PutRetentionPolicy', 'DeleteRetentionPolicy']:
    response = attempt('cloudtrail', 'lookup_events', LookupAttributes=[{'AttributeKey': 'EventName', 'AttributeValue': event_name}],
                       StartTime=start, EndTime=observed_end, MaxResults=50)
    trail.append({'event_name': event_name, 'next_token_present': bool((response or {}).get('NextToken')),
                  'events': [{k: e[k] for k in ['EventId', 'EventName', 'EventTime', 'Resources', 'EventSource'] if k in e}
                             for e in (response or {}).get('Events', [])], 'available': response is not None})
save('retention_deletion_event_metadata.json', trail)

prior = {g['logGroupName']: g for g in baseline}
current = {g['logGroupName']: g for g in groups}
rows = []
for name in sorted(set(prior) | set(current)):
    before, after = prior.get(name), current.get(name)
    rows.append({'log_group': name, 'baseline_stored_bytes': before.get('storedBytes') if before else None,
                 'current_stored_bytes': after.get('storedBytes') if after else None,
                 'delta_bytes': after['storedBytes'] - before['storedBytes'] if before and after else None,
                 'baseline_retention_days': before.get('retentionInDays') if before else None,
                 'current_retention_days': after.get('retentionInDays') if after else None,
                 'retention_unchanged': before.get('retentionInDays') == after.get('retentionInDays') if before and after else None,
                 'same_creation_time': before.get('creationTime') == after.get('creationTime') if before and after else None})
summary = {'account': '637246457028', 'region': 'us-east-2', 'baseline_reference_utc': start,
           'baseline_time_precision': 'Collection start only; original query had no per-group timestamps.',
           'current_capture_start_utc': observed_start, 'current_capture_end_utc': observed_end,
           'elapsed_seconds_approx': (observed_end - start).total_seconds(),
           'baseline_total_bytes': sum(g['storedBytes'] for g in baseline),
           'current_total_bytes': sum(g['storedBytes'] for g in groups),
           'delta_total_bytes': sum(g['storedBytes'] for g in groups) - sum(g['storedBytes'] for g in baseline),
           'existing_storage_metric_series_count': len(storage_metrics),
           'incoming_bytes_in_available_samples': sum(p['Sum'] for r in incoming if r['response'] for p in r['response'].get('Datapoints', [])),
           'rows': rows, 'api_calls': calls, 'errors_count': len(errors),
           'starts_jobs': False, 'reads_log_messages': False, 'starts_queries': False,
           'changes_retention': False, 'changes_iam': False, 'enables_billing': False,
           'cost_explorer_calls': 0,
           'limitations': ['Passive storage delta is not automatically future growth per pipeline cycle.',
                           'Metric availability and stock updates may lag. Missing datapoints are not proof of zero ingestion.',
                           'No expiry/deletion/recreation must be verified before attributing net change to generated logs.',
                           'CloudTrail lookups are bounded to one page per event type and may not be exhaustive.']}
save('summary.json', summary)
save('errors.json', errors)
digests = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.iterdir())}
save('SHA256.json', digests)
package = {p.name: json.loads(p.read_text()) for p in sorted(out.iterdir())}
packed = gzip.compress(json.dumps(package, ensure_ascii=False, separators=(',', ':')).encode())
print('FINOPS_LOGS_SUMMARY ' + json.dumps({k: summary[k] for k in ['baseline_total_bytes', 'current_total_bytes', 'delta_total_bytes', 'current_capture_end_utc', 'existing_storage_metric_series_count', 'incoming_bytes_in_available_samples', 'api_calls', 'errors_count']}, default=str))
print('FINOPS_LOGS_EXPORT_BEGIN')
print('\n'.join(textwrap.wrap(base64.b64encode(packed).decode(), 100)))
print('FINOPS_LOGS_EXPORT_END')
print('FINOPS_LOGS_GZIP_SHA256 ' + hashlib.sha256(packed).hexdigest())
