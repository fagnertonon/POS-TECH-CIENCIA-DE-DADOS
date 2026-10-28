"""Verify migration without publishing tests, changing routing or invoking compute."""
from pathlib import Path
from datetime import datetime,timezone
import argparse,json,hashlib
import migrate_existing_alerts as migration

def main():
    p=argparse.ArgumentParser();p.add_argument('--phase',required=True,choices=['start-drift','verify']);p.add_argument('--work-dir',required=True,type=Path);a=p.parse_args()
    plan=migration.verify_files();baseline=migration.load(migration.BASE/'before.json')
    import boto3
    from botocore.config import Config
    session=boto3.Session(region_name=migration.REGION);cfg=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=20)
    clients={s:session.client(s,config=cfg) for s in ['sts','cloudformation','sns','events','cloudwatch','lambda']}
    if clients['sts'].get_caller_identity()['Account']!=migration.ACCOUNT:raise ValueError('Wrong account')
    cf=clients['cloudformation'];work=a.work_dir
    if a.phase=='start-drift':
        path=work/'drift_requests.json'
        if path.exists():raise FileExistsError('Drift requests already started; use saved IDs')
        requests={name:cf.detect_stack_drift(StackName=name) for name in [migration.DESTINATION,migration.STREAMING]}
        migration.save_new(path,requests);print(json.dumps({'drift_started':{k:v['StackDriftDetectionId'] for k,v in requests.items()}}));return
    path=work/'migration_verified.json'
    if path.exists():raise FileExistsError('Final evidence already exists')
    report={'verified_at_utc':migration.now(),'account':migration.ACCOUNT,'region':migration.REGION,
            'status':'VERIFYING','read_only':True,'new_emails_sent':0,'new_subscriptions_created':0}
    report['routing']=migration.guard_routing(clients,baseline)
    report['stacks']={name:cf.describe_stacks(StackName=name)['Stacks'][0] for name in [migration.DESTINATION,migration.STREAMING]}
    assert report['stacks'][migration.DESTINATION]['StackStatus']=='IMPORT_COMPLETE'
    assert report['stacks'][migration.STREAMING]['StackStatus']=='UPDATE_COMPLETE'
    report['resources']={name:cf.list_stack_resources(StackName=name) for name in report['stacks']}
    assert all(not v.get('NextToken') for v in report['resources'].values())
    imported={r['LogicalResourceId']:r for r in report['resources'][migration.DESTINATION]['StackResourceSummaries']}
    expected={r['LogicalResourceId']:r for r in plan['import_existing_resources']}
    assert set(imported)==set(expected)
    for logical,resource in imported.items():
        assert resource['ResourceType']==expected[logical]['ResourceType']
        expected_id=next(iter(expected[logical]['ResourceIdentifier'].values()))
        allowed={expected_id}
        if logical=='BatchFailureRule':allowed.add('fiap-inep-batch-failures')
        assert resource['PhysicalResourceId'] in allowed,(logical,resource['PhysicalResourceId'])
        assert resource['ResourceStatus']=='IMPORT_COMPLETE'
    before={r['LogicalResourceId']:(r['PhysicalResourceId'],r['ResourceType']) for r in baseline['streaming_resources']['StackResourceSummaries']}
    after={r['LogicalResourceId']:(r['PhysicalResourceId'],r['ResourceType']) for r in report['resources'][migration.STREAMING]['StackResourceSummaries']}
    assert before==after,'Streaming resource identity changed'
    lamb=clients['lambda'].get_function_configuration(FunctionName='fiap-inep-streaming-consumer')
    for key in ['FunctionName','FunctionArn','Runtime','Role','Timeout','MemorySize','CodeSha256','CodeSize','LastModified','RevisionId']:
        assert lamb[key]==baseline['lambda_config'][key],key
    report['lambda_configuration_unchanged']={k:lamb[k] for k in baseline['lambda_config'] if k in lamb}
    report['templates']={name:cf.get_template(StackName=name,TemplateStage='Original')['TemplateBody'] for name in report['stacks']}
    for key,file in [(migration.DESTINATION,'import_template.json'),(migration.STREAMING,'streaming_update_template.json')]:
        template=report['templates'][key]
        if isinstance(template,str):template=json.loads(template)
        assert template==migration.load(migration.BASE/file),'Native template differs from exact reviewed template: '+key
    if (work/'drift_requests.json').exists():
        report['drift']={};requests=migration.load(work/'drift_requests.json')
        for name,request in requests.items():
            status=cf.describe_stack_drift_detection_status(StackDriftDetectionId=request['StackDriftDetectionId'])
            if status['DetectionStatus']=='DETECTION_IN_PROGRESS':
                print(json.dumps({'status':'DRIFT_STILL_RUNNING','stack':name}));return
            assert status['DetectionStatus']=='DETECTION_COMPLETE',status
            drifts=cf.describe_stack_resource_drifts(StackName=name)
            assert not drifts.get('NextToken')
            bad=[r for r in drifts['StackResourceDrifts'] if r['StackResourceDriftStatus'] not in ['IN_SYNC','NOT_CHECKED']]
            assert not bad,bad
            report['drift'][name]={'status':status,'resources':drifts['StackResourceDrifts']}
    report.update(status='MIGRATION_VERIFIED',imported_existing_resources=len(imported),
                  existing_streaming_resources_preserved=len(after),
                  subscribed_email_arn_preserved=plan['subscription_preserved_arn'],
                  topic_policy_and_eventbridge_target_preserved=True,
                  lambda_code_and_configuration_preserved=True,
                  jobs_started=0,replays_started=0,project_total_ceiling_usd=140,
                  actual_billing_not_refreshed=True)
    migration.save_new(path,report)
    data=path.read_bytes()
    print(json.dumps({'output':str(path),'sha256':hashlib.sha256(data).hexdigest(),'status':report['status'],
      'stacks':{k:v['StackStatus'] for k,v in report['stacks'].items()},'imported':len(imported),
      'subscription_preserved':True,'routing_preserved':True,'lambda_preserved':True,'new_emails':0}))

if __name__=='__main__':main()
