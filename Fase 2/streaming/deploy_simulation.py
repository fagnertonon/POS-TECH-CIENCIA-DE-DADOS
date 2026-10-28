"""Prepare/review a bounded UPDATE change set; execute only the saved, exact plan.

No Glue, BigQuery, new queue, scheduler or service is created. The change set is
rejected if it replaces or changes anything except the existing role/function.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['prepare', 'execute', 'status'], required=True)
    p.add_argument('--output-dir', required=True)
    p.add_argument('--region', default='us-east-2')
    p.add_argument('--stack-name', default='fiap-inep-streaming')
    args = p.parse_args(argv)
    if args.region != 'us-east-2' or args.stack_name != 'fiap-inep-streaming':
        raise ValueError('This reviewed deployment is restricted to the existing Ohio stack')
    import boto3
    session = boto3.Session(region_name=args.region)
    if session.client('sts').get_caller_identity()['Account'] != '637246457028':
        raise ValueError('Unexpected AWS account')
    cfn = session.client('cloudformation')
    out = Path(args.output_dir)
    plan_path = out / 'change_set_review.json'
    template_path = Path(__file__).with_name('streaming_stack.json')
    template_text = template_path.read_text(encoding='utf-8')
    template_hash = hashlib.sha256(template_text.encode()).hexdigest()
    if args.mode == 'prepare':
        if out.exists():
            raise FileExistsError('Use a new evidence directory')
        preflight_path = Path(__file__).with_name('preflight_report.json')
        # Explicit state gate is supplied separately by aws_preflight.py; the
        # change set itself does not run code or alter the execution role.
        stack = cfn.describe_stacks(StackName=args.stack_name)['Stacks'][0]
        if stack['StackStatus'] not in ('CREATE_COMPLETE', 'UPDATE_COMPLETE'):
            raise ValueError('Stack is not stable')
        template = json.loads(template_text)
        function = template['Resources']['ConsumerFunction']['Properties']
        if (function['MemorySize'], function['Timeout'], function['ReservedConcurrentExecutions']) != (256, 10, 2):
            raise ValueError('Compute caps changed')
        name = 'sim-gold-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        result = cfn.create_change_set(StackName=args.stack_name, ChangeSetName=name,
            ChangeSetType='UPDATE', TemplateBody=template_text,
            Capabilities=['CAPABILITY_NAMED_IAM'],
            Parameters=[{'ParameterKey':'BucketName','UsePreviousValue':True}],
            Description='Reviewed simulation-only Silver/Gold projection; preserve official INEP')
        out.mkdir(parents=True)
        for _ in range(60):
            review = cfn.describe_change_set(ChangeSetName=result['Id'])
            if review['Status'] in ('CREATE_COMPLETE','FAILED'):
                break
            time.sleep(1)
        else:
            raise TimeoutError('Change set not ready; do not execute blindly')
        changes = [c['ResourceChange'] for c in review.get('Changes', [])]
        allowed = {'ConsumerFunction', 'ConsumerRole'}
        approved = review['Status'] == 'CREATE_COMPLETE' and bool(changes) and all(
            c['LogicalResourceId'] in allowed and c['Action'] == 'Modify'
            and c.get('Replacement') == 'False' for c in changes)
        plan = {'status': 'REVIEWABLE' if approved else 'BLOCKED',
                'change_set_id': result['Id'], 'stack_id': result['StackId'],
                'template_sha256': template_hash, 'changes': changes,
                'cloudformation_status':review['Status'], 'reason':review.get('StatusReason'),
                'compute_caps': {'memory_mb':256,'timeout_seconds':10,'max_concurrency':2},
                'new_iam_object_prefixes': ['silver/inep_streaming/entity=alunos/*',
                    'gold/inep_streaming/contribuicoes/*','auditoria/streaming/processed/*'],
                'iam_actions': ['s3:GetObject','s3:PutObject'],
                'no_official_prefix_access':True, 'no_scheduler':True}
        save(plan_path, plan)
        print(json.dumps(plan, default=str))
        if not approved:
            raise RuntimeError('Unexpected change set; not executable by this script')
    else:
        plan = json.loads(plan_path.read_text(encoding='utf-8'))
        if plan['status'] != 'REVIEWABLE' or plan['template_sha256'] != template_hash:
            raise ValueError('Local source/plan changed; prepare a new reviewed plan')
        if args.mode == 'execute':
            review = cfn.describe_change_set(ChangeSetName=plan['change_set_id'])
            if review.get('ExecutionStatus') != 'AVAILABLE':
                raise ValueError('Reviewed change set is not available')
            cfn.execute_change_set(ChangeSetName=plan['change_set_id'])
            save(out / 'execution_started.json', {'executed_at_utc':datetime.now(timezone.utc).isoformat(),
                                                  'change_set_id':plan['change_set_id']})
        stack = cfn.describe_stacks(StackName=args.stack_name)['Stacks'][0]
        state = {'stack_status':stack['StackStatus'], 'outputs':stack.get('Outputs',[])}
        save(out / 'deployment_state.json', state)
        print(json.dumps(state))


if __name__ == '__main__':
    main()
