"""Run a controlled replay and one intentionally invalid-message test in AWS."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import uuid

import boto3

import producer


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stack-name', default='fiap-inep-streaming')
    p.add_argument('--region', default='us-east-2')
    p.add_argument('--output-dir', default='streaming-evidence')
    p.add_argument('--events', type=int, default=30)
    p.add_argument('--interval-seconds', type=float, default=1.0)
    p.add_argument('--duplicates', type=int, default=5)
    args = p.parse_args()
    root = Path(__file__).resolve().parent
    evidence = Path(args.output_dir)
    if evidence.exists():
        raise FileExistsError('Choose a new output directory; previous evidence is preserved')
    cfn = boto3.client('cloudformation', region_name=args.region)
    stack = cfn.describe_stacks(StackName=args.stack_name)['Stacks'][0]
    if stack['StackStatus'] not in ('CREATE_COMPLETE', 'UPDATE_COMPLETE'):
        raise RuntimeError('CloudFormation deployment is not complete')
    deployed = cfn.get_template(StackName=args.stack_name, TemplateStage='Original')['TemplateBody']
    if isinstance(deployed, str):
        deployed = json.loads(deployed)
    local_source = (root / 'lambda_function.py').read_text(encoding='utf-8')
    cloud_source = deployed['Resources']['ConsumerFunction']['Properties']['Code']['ZipFile']
    if hashlib.sha256(local_source.encode()).digest() != hashlib.sha256(cloud_source.encode()).digest():
        raise RuntimeError('Deployed Lambda source differs from the reviewed local source')
    outputs = {o['OutputKey']: o['OutputValue'] for o in stack['Outputs']}
    lamb = boto3.client('lambda', region_name=args.region)
    function = lamb.get_function_configuration(FunctionName=outputs['FunctionName'])
    mapping = lamb.get_event_source_mapping(UUID=outputs['EventSourceMappingUuid'])
    if function['State'] != 'Active' or function['LastUpdateStatus'] != 'Successful' or mapping['State'] != 'Enabled':
        raise RuntimeError('Consumer and SQS mapping must be active before sending events')
    evidence.mkdir(parents=True)
    (evidence / 'infra_state.json').write_text(json.dumps({
        'stack_id': stack['StackId'], 'stack_status': stack['StackStatus'], 'outputs': outputs,
        'consumer_source_sha256': hashlib.sha256(local_source.encode()).hexdigest(),
        'runtime': function['Runtime'], 'mapping_state': mapping['State'],
        'mapping_response_types': mapping.get('FunctionResponseTypes'),
    }, indent=2) + '\n', encoding='utf-8')
    invalid = {
        'schema_version': 1, 'event_id': str(uuid.uuid4()), 'run_id': 'invalid_' + uuid.uuid4().hex,
        'entity': 'alunos', 'event_time': producer.iso_utc(datetime.now(timezone.utc)),
        'source': 'inep_bronze_replay', 'simulation': True,
        'source_snapshot_run_id': 'jr_cdfd2933220ec75b45c04518c6e5afe93eb98915f3b88328f8bde8d7183d3700',
        'payload': 'INTENTIONALLY_INVALID_OBJECT_TYPE_FOR_DLQ_TEST',
    }
    sqs = boto3.client('sqs', region_name=args.region)
    result = sqs.send_message(QueueUrl=outputs['QueueUrl'], MessageBody=producer.strict_json(invalid))
    invalid_report = {'message_id': result['MessageId'], 'sent_at_utc': invalid['event_time'],
                      'expected_failure': 'InvalidEnvelope: payload must be an object',
                      'body_sha256': hashlib.sha256(producer.strict_json(invalid).encode()).hexdigest(),
                      'event': invalid}
    (evidence / 'invalid_message.json').write_text(json.dumps(invalid_report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'CONTROLLED_TEST_STARTED', 'invalid_message_id': result['MessageId'],
                      'evidence_directory': str(evidence)}), flush=True)
    subprocess.run([
        sys.executable, str(root / 'producer.py'), '--region', args.region,
        '--queue-url', outputs['QueueUrl'], '--events', str(args.events),
        '--interval-seconds', str(args.interval_seconds), '--duplicates', str(args.duplicates),
        '--output-dir', str(evidence / 'producer'),
    ], check=True)
    print(json.dumps({'status': 'PRODUCER_COMPLETED', 'expected_unique_events': args.events,
                      'intentional_duplicate_sends': args.duplicates, 'invalid_message_id': result['MessageId'],
                      'next_step': 'Wait for five failed deliveries and verify the invalid message in the DLQ.'}), flush=True)


if __name__ == '__main__':
    main()
