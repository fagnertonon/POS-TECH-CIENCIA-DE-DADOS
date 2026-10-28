"""Read-only evidence check of a completed SQS -> Lambda -> S3 replay.

Needs existing S3 GetObject/ListBucket, SQS GetQueueAttributes/ReceiveMessage/
ChangeMessageVisibility, Lambda GetFunctionConfiguration, logs FilterLogEvents,
cloudwatch GetMetricStatistics/DescribeAlarms permissions. It grants no access.
DLQ messages are inspected and returned to visibility; none is deleted.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import statistics

import lambda_function as consumer


def stamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)


def digest(value):
    return consumer.sha256(value)


def distribution(values):
    if not values:
        return {'count': 0}
    ordered = sorted(values)
    return {'count': len(values), 'min': ordered[0], 'max': ordered[-1],
            'mean': statistics.fmean(values), 'median': statistics.median(values),
            'p95_nearest_rank': ordered[max(0, (95 * len(ordered) + 99) // 100 - 1)]}


def inventory(s3, bucket, prefix):
    return [obj for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix)
            for obj in page.get('Contents', [])]


def inspect_dlq(sqs, url, expected_id, config):
    """Temporarily hold up to 30 messages so a later page cannot return the same one."""
    receipts, summaries, found = [], [], None
    try:
        for _ in range(3):
            messages = sqs.receive_message(
                QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=30,
                AttributeNames=['All'], MessageAttributeNames=['All'],
            ).get('Messages', [])
            for message in messages:
                receipts.append(message['ReceiptHandle'])
                body = message.get('Body', '')
                invalid, error_type = False, None
                try:
                    consumer.parse_event(body, config)
                except Exception as exc:
                    invalid, error_type = True, type(exc).__name__
                entry = {
                    'message_id': message['MessageId'], 'attributes': message.get('Attributes', {}),
                    'body_sha256': hashlib.sha256(body.encode()).hexdigest(),
                    'body_bytes': len(body.encode()), 'contract_invalid': invalid,
                    'validation_error_type': error_type,
                }
                summaries.append(entry)
                if message['MessageId'] == expected_id:
                    found = entry
            if found or not messages:
                break
    finally:
        # Restore every inspected message, including unrelated evidence.
        for receipt in receipts:
            sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=receipt, VisibilityTimeout=0)
    return {'expected_message_id': expected_id, 'matched': found,
            'messages_inspected': summaries, 'deleted_messages': 0,
            'visibility_restored': len(receipts)}


def observe(args, logs, cw, start, finish, run_id, invalid_message_id):
    batches, record_events, latencies, runtime_errors, invalid_errors = [], [], [], [], []
    params = {'logGroupName': f'/aws/lambda/{args.function_name}',
              'startTime': int((start - timedelta(seconds=5)).timestamp() * 1000),
              'endTime': int(finish.timestamp() * 1000), 'interleaved': True}
    token = None
    while True:
        if token:
            params['nextToken'] = token
        response = logs.filter_log_events(**params)
        for record in response.get('events', []):
            message = record.get('message', '').strip()
            try:
                payload = json.loads(message)
            except (ValueError, TypeError):
                if any(label in message for label in ('[ERROR]', 'Task timed out', 'Runtime.ExitError', 'Runtime.ImportModuleError')):
                    runtime_errors.append({'timestamp': record.get('timestamp'),
                                           'message_excerpt': message[:400]})
                continue
            if not isinstance(payload, dict):
                continue
            kind = payload.get('kind')
            if kind == 'STREAMING_EVENT' and payload.get('run_id') == run_id:
                record_events.append({key: payload.get(key) for key in
                                      ('status', 'run_id', 'event_id', 'sqs_message_id', 'target_key')})
            if kind == 'STREAMING_EVENT' and payload.get('sqs_message_id') == invalid_message_id:
                invalid_errors.append({key: payload.get(key) for key in
                                       ('status', 'sqs_message_id', 'error_type')})
            if kind in ('STREAMING_BATCH', 'STREAMING_LATENCY'):
                if kind == 'STREAMING_BATCH':
                    batches.append({key: payload.get(key) for key in
                                    ('invocation_id', 'Received', 'Accepted', 'Duplicate', 'Failed')})
                values = payload.get('LatencySeconds', [])
                if not isinstance(values, list):
                    values = [values]
                latencies.extend(float(value) for value in values if type(value) in (int, float))
        next_token = response.get('nextToken')
        if not next_token or next_token == token:
            break
        token = next_token
    metrics = {}
    for metric in ('Received', 'Accepted', 'Duplicate', 'Failed', 'LatencySeconds'):
        response = cw.get_metric_statistics(
            Namespace='FIAP/Streaming', MetricName=metric,
            Dimensions=[{'Name': 'Pipeline', 'Value': 'fiap-inep-streaming'}],
            StartTime=start - timedelta(seconds=5), EndTime=finish, Period=60,
            Statistics=['Sum', 'SampleCount', 'Minimum', 'Maximum', 'Average'],
        )
        metrics[metric] = sorted(
            [{key: (value.isoformat() if isinstance(value, datetime) else value)
              for key, value in point.items()} for point in response.get('Datapoints', [])],
            key=lambda item: item['Timestamp'],
        )
    alarm_response = cw.describe_alarms(AlarmNames=[
        'fiap-inep-streaming-dlq', 'fiap-inep-streaming-rejected', 'fiap-inep-streaming-age',
    ])
    alarms = [{key: alarm.get(key) for key in (
        'AlarmName', 'StateValue', 'StateReason', 'ActionsEnabled', 'AlarmActions',
        'Namespace', 'MetricName', 'Period', 'EvaluationPeriods', 'Threshold', 'ComparisonOperator',
    )} for alarm in alarm_response.get('MetricAlarms', [])]
    return {
        'window_start_utc': start.isoformat(), 'window_end_utc': finish.isoformat(),
        'event_logs_scope': 'exact run_id; invalid failures matched by SQS message ID',
        'emf_and_cloudwatch_scope': 'function and time window, not exclusively run_id',
        'event_status_counts': {name: sum(item['status'] == name for item in record_events)
                                for name in ('ACCEPTED', 'DUPLICATE', 'FAILED')},
        'invalid_message_failures': invalid_errors, 'runtime_errors': runtime_errors,
        'emf_batches': batches, 'emf_latency_seconds': distribution(latencies),
        'cloudwatch_metric_datapoints': metrics, 'alarms': alarms,
    }


def verify(args, s3, sqs, logs, cw, lamb):
    errors, pending = [], []
    report = {'verified_at_utc': datetime.now(timezone.utc).isoformat(),
              'region': args.region, 'function_name': args.function_name,
              'errors': errors, 'pending': pending, 'events': []}
    producer = json.loads(Path(args.producer_report).read_text(encoding='utf-8'))
    raw_events = Path(args.events).read_text(encoding='utf-8').splitlines()
    config = consumer.Config(args.bucket, args.prefix, args.idempotency_prefix, consumer.ALL_ENTITIES)
    parsed = [consumer.parse_event(body, config) for body in raw_events]
    envelopes = [event for event, _ in parsed]
    if not envelopes:
        raise ValueError('No unique events in input')
    run_id = producer.get('run_id')
    report['run_id'] = run_id
    if producer.get('status') != 'SUCCEEDED':
        errors.append('Producer did not finish successfully')
    if len(envelopes) != producer.get('unique_event_count'):
        errors.append('Unique event count differs from producer report')
    if len({event['event_id'] for event in envelopes}) != len(envelopes):
        errors.append('events.jsonl contains duplicate event IDs')
    if any(event['run_id'] != run_id for event in envelopes):
        errors.append('Input events contain different run IDs')
    sources = {event['source_snapshot_run_id'] for event in envelopes}
    if len(sources) != 1 or any(event['simulation'] is not True or event['source'] != 'inep_bronze_replay' for event in envelopes):
        errors.append('Input simulation/source provenance is inconsistent')
    if producer.get('source', {}).get('snapshot_run_id') not in (None, next(iter(sources))):
        errors.append('Producer source snapshot differs from events')
    sends = producer.get('send_results', [])
    if producer.get('confirmed_send_count') != len(sends) or len(sends) < len(envelopes):
        errors.append('Confirmed sends are missing or inconsistent')
    expected_duplicates = producer.get('duplicate_send_count', 0)
    if sum(item.get('kind') == 'DUPLICATE' for item in sends) != expected_duplicates:
        errors.append('Duplicate sends do not match producer report')
    ids = {event['event_id'] for event in envelopes}
    if any(item.get('event_id') not in ids or item.get('run_id') != run_id for item in sends):
        errors.append('Send results contain unrelated events')
    report['producer'] = {key: producer.get(key) for key in (
        'status', 'run_id', 'started_at_utc', 'finished_at_utc', 'unique_event_count',
        'duplicate_send_count', 'confirmed_send_count', 'interval_seconds', 'source',
    )}
    expected_keys, expected_claims, raw_latencies = set(), set(), []
    for event, event_time in parsed:
        target_key = (f'{args.prefix}/entity={event["entity"]}/event_date={event_time.date().isoformat()}'
                      f'/run_id={run_id}/event_id={event["event_id"]}.json')
        claim_key = f'{args.idempotency_prefix}/run_id={run_id}/event_id={event["event_id"]}.json'
        expected_keys.add(target_key)
        expected_claims.add(claim_key)
        entry = {'event_id': event['event_id'], 'target_uri': f's3://{args.bucket}/{target_key}',
                 'claim_uri': f's3://{args.bucket}/{claim_key}', 'event_sha256': digest(event),
                 'payload_sha256': digest(event['payload']), 'event_time': event['event_time']}
        report['events'].append(entry)
        for kind, key in (('bronze', target_key), ('claim', claim_key)):
            try:
                result = s3.get_object(Bucket=args.bucket, Key=key)
            except Exception as exc:
                code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
                if code in ('NoSuchKey', '404', 'NotFound'):
                    pending.append(f'Missing {kind} object for event {event["event_id"]}')
                    continue
                raise
            stored = json.loads(result['Body'].read())
            ingestion = stored.get('ingestion', {})
            checks = (
                digest(stored.get('event')) == entry['event_sha256'], stored.get('event') == event,
                ingestion.get('event_sha256') == entry['event_sha256'],
                ingestion.get('payload_sha256') == entry['payload_sha256'],
                ingestion.get('target_key') == target_key,
                result.get('Metadata', {}).get('event-sha256') == entry['event_sha256'],
            )
            if not all(checks):
                errors.append(f'{kind} envelope/hash/path mismatch for {event["event_id"]}')
            entry[f'{kind}_verified'] = all(checks)
            if kind == 'bronze':
                last_modified = result['LastModified'].astimezone(timezone.utc)
                raw_latency = (last_modified - event_time).total_seconds()
                raw_latencies.append(raw_latency)
                entry['s3_last_modified_utc'] = last_modified.isoformat()
                entry['s3_latency_seconds_raw'] = raw_latency
                entry['s3_latency_seconds_clamped'] = max(0.0, raw_latency)
                entry['bytes'] = result.get('ContentLength')
    actual_objects = [obj for obj in inventory(s3, args.bucket, args.prefix + '/')
                      if f'/run_id={run_id}/' in obj['Key']]
    actual_keys = {obj['Key'] for obj in actual_objects}
    claim_objects = inventory(s3, args.bucket, f'{args.idempotency_prefix}/run_id={run_id}/')
    actual_claims = {obj['Key'] for obj in claim_objects}
    for name, expected, actual in (('Bronze', expected_keys, actual_keys), ('claims', expected_claims, actual_claims)):
        if actual - expected:
            errors.append(f'Unexpected {name} objects: {len(actual - expected)}')
        if expected - actual:
            pending.append(f'Missing {name} objects: {len(expected - actual)}')
    report['s3_inventory'] = {'unique_events_expected': len(envelopes),
                              'bronze_object_count': len(actual_objects),
                              'claim_object_count': len(claim_objects),
                              'bronze_bytes': sum(obj['Size'] for obj in actual_objects)}
    report['s3_latency_seconds'] = {
        'raw': distribution(raw_latencies), 'clamped': distribution([max(0.0, x) for x in raw_latencies]),
        'limitation': 'S3 LastModified has second resolution; raw values may be slightly negative. EMF latency is measured by Lambda only after the Bronze PUT succeeds.',
    }
    attributes = ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed']
    report['queue_attributes'] = sqs.get_queue_attributes(QueueUrl=args.queue_url, AttributeNames=attributes)['Attributes']
    report['dlq_attributes_before_inspection'] = sqs.get_queue_attributes(QueueUrl=args.dlq_url, AttributeNames=attributes)['Attributes']
    if any(int(report['queue_attributes'].get(name, '0')) > 0 for name in attributes):
        pending.append('Source queue still contains visible/in-flight/delayed messages (approximate counters)')
    if args.invalid_message_id:
        report['dlq_evidence'] = inspect_dlq(sqs, args.dlq_url, args.invalid_message_id, config)
        matched = report['dlq_evidence']['matched']
        if not matched:
            pending.append('Expected invalid message not found in DLQ yet')
        elif not matched['contract_invalid']:
            errors.append('Expected invalid DLQ message actually passes envelope validation')
    function = lamb.get_function_configuration(FunctionName=args.function_name)
    report['lambda_configuration'] = {key: function.get(key) for key in (
        'FunctionName', 'FunctionArn', 'Runtime', 'Role', 'Handler', 'Timeout', 'MemorySize',
        'CodeSha256', 'LastModified', 'State', 'LastUpdateStatus',
    )}
    if function.get('State') != 'Active' or function.get('LastUpdateStatus') != 'Successful':
        pending.append('Lambda is not active with a successful last update')
    start = stamp(producer['started_at_utc'])
    finish = datetime.now(timezone.utc)
    report['observability'] = observe(args, logs, cw, start, finish, run_id, args.invalid_message_id)
    obs = report['observability']
    if obs['event_status_counts']['ACCEPTED'] < len(envelopes):
        pending.append('Accepted event logs have not all appeared')
    if obs['event_status_counts']['DUPLICATE'] < expected_duplicates:
        pending.append('Intentional duplicate handling has not all appeared in logs')
    if obs['emf_latency_seconds']['count'] < len(envelopes):
        pending.append('EMF latency samples have not all appeared')
    if obs['runtime_errors']:
        errors.append('Lambda runtime errors occurred in the observation window')
    if args.invalid_message_id and not obs['invalid_message_failures']:
        pending.append('Expected invalid-message failures not yet visible in Lambda logs')
    if any(not points for points in obs['cloudwatch_metric_datapoints'].values()):
        pending.append('CloudWatch custom metrics have not all propagated')
    report['validation_status'] = 'FAILED' if errors else ('INCOMPLETE' if pending else 'SUCCEEDED')
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('producer-report', 'events', 'queue-url', 'dlq-url', 'output'):
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--invalid-message-id')
    parser.add_argument('--region', default='us-east-2')
    parser.add_argument('--function-name', default='fiap-inep-streaming-consumer')
    parser.add_argument('--bucket', default='fiap-tc-fase2-2026')
    parser.add_argument('--prefix', default='bronze/inep_streaming')
    parser.add_argument('--idempotency-prefix', default='auditoria/streaming/idempotency')
    args = parser.parse_args(argv)
    args.prefix, args.idempotency_prefix = args.prefix.strip('/'), args.idempotency_prefix.strip('/')
    return args


def main(argv=None):
    import boto3
    args = parse_args(argv)
    try:
        report = verify(args, *(boto3.client(name, region_name=args.region)
                                for name in ('s3', 'sqs', 'logs', 'cloudwatch', 'lambda')))
    except Exception as exc:
        report = {'verified_at_utc': datetime.now(timezone.utc).isoformat(),
                  'validation_status': 'FAILED', 'errors': [type(exc).__name__], 'pending': [],
                  'aws_error_code': str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    print(json.dumps({key: report.get(key) for key in ('validation_status', 'run_id', 's3_inventory', 'errors', 'pending')}, ensure_ascii=False))
    return 0 if report['validation_status'] == 'SUCCEEDED' else 1


if __name__ == '__main__':
    raise SystemExit(main())
