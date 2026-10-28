"""Guarded adoption of existing alerts: exact IMPORT, then three-alarm UPDATE.

Never subscribes/publishes, invokes Lambda, edits IAM, starts a job, polls SQS or writes S3.
CloudFormation execution is allowed only after a native change set passes scope checks.
"""
from pathlib import Path
from datetime import datetime,timezone
import argparse,hashlib,json

ACCOUNT='637246457028';REGION='us-east-2'
DESTINATION='fiap-inep-observabilidade';STREAMING='fiap-inep-streaming'
TOPIC=f'arn:aws:sns:{REGION}:{ACCOUNT}:fiap-inep-alertas-operacionais'
BASE=Path(__file__).resolve().parent
SOURCE_PLAN_SHA='8098aa82131a3e21b39b7d6b5d5a29e8afc0c96d961aab7caa115e95497d57fd'
UPDATE_IDS={'DeadLetterAlarm','RejectedEventAlarm','QueueAgeAlarm'}

def now():return datetime.now(timezone.utc).isoformat()
def sha(data):return hashlib.sha256(data).hexdigest()
def encode(v):return (json.dumps(v,ensure_ascii=False,indent=2,default=str)+'\n').encode()
def load(path):return json.loads(Path(path).read_bytes())
def save_new(path,value):
    with Path(path).open('xb') as out:out.write(encode(value))

def verify_files(base=BASE):
    data=(base/'migration_plan.json').read_bytes()
    if sha(data)!=SOURCE_PLAN_SHA:raise ValueError('Migration plan differs from the reviewed exact plan')
    plan=json.loads(data)
    if plan['account']!=ACCOUNT or plan['region']!=REGION:raise ValueError('Unexpected account/region in plan')
    for name,expected in plan['template_hashes'].items():
        if sha((base/name).read_bytes())!=expected:raise ValueError('Changed migration source: '+name)
    if sha((base/'before.json').read_bytes())!=plan['before_snapshot_sha256']:
        raise ValueError('Changed baseline')
    return plan

def validate_changes(review,phase,plan):
    if review.get('Status')!='CREATE_COMPLETE' or review.get('ExecutionStatus')!='AVAILABLE':
        raise ValueError('Change set not ready/available: '+str(review.get('StatusReason',review.get('Status'))))
    if review.get('NextToken'):raise ValueError('Incomplete change-set page')
    changes=[item['ResourceChange'] for item in review.get('Changes',[])]
    expected={item['LogicalResourceId']:item['ResourceType'] for item in plan['import_existing_resources']} if phase=='import' else {name:'AWS::CloudWatch::Alarm' for name in UPDATE_IDS}
    if len(changes)!=len(expected) or {x['LogicalResourceId'] for x in changes}!=set(expected):
        raise ValueError('Unexpected resource set in native change set')
    for change in changes:
        if change['ResourceType']!=expected[change['LogicalResourceId']]:raise ValueError('Unexpected resource type')
        if phase=='import':
            if change['Action']!='Import':raise ValueError('Import must not create/modify/delete resources')
            if change.get('Replacement') not in (None,'False'):raise ValueError('Unexpected import replacement')
        elif change['Action']!='Modify' or change.get('Replacement')!='False':
            raise ValueError('Only in-place modifications of the three existing alarms are allowed')
    return changes

def guard_routing(clients,baseline):
    if clients['sts'].get_caller_identity()['Account']!=ACCOUNT:raise ValueError('Wrong AWS account')
    topic=clients['sns'].get_topic_attributes(TopicArn=TOPIC)['Attributes']
    if topic['Owner']!=ACCOUNT or json.loads(topic['Policy'])!=json.loads(baseline['topic']['Attributes']['Policy']):
        raise ValueError('SNS owner/policy changed; stop for review')
    subs=clients['sns'].list_subscriptions_by_topic(TopicArn=TOPIC)
    if subs.get('NextToken') or subs['Subscriptions']!=baseline['subscriptions']['Subscriptions']:
        raise ValueError('Subscriber set changed; stop before any import/update')
    for sub in subs['Subscriptions']:
        attrs=clients['sns'].get_subscription_attributes(SubscriptionArn=sub['SubscriptionArn'])['Attributes']
        if attrs.get('PendingConfirmation')!='false':raise ValueError('Existing subscription no longer confirmed')
    rule=clients['events'].describe_rule(Name='fiap-inep-batch-failures')
    for field in ['Name','Arn','State','EventBusName']:
        if rule[field]!=baseline['rule'][field]:raise ValueError('Rule changed: '+field)
    if json.loads(rule['EventPattern'])!=json.loads(baseline['rule']['EventPattern']):raise ValueError('Rule filter changed')
    targets=clients['events'].list_targets_by_rule(Rule=rule['Name'])
    if targets.get('NextToken') or targets['Targets']!=baseline['targets']['Targets']:raise ValueError('Targets changed')
    alarms=clients['cloudwatch'].describe_alarms(AlarmNames=[x['AlarmName'] for x in baseline['alarms']['MetricAlarms']])
    old={a['AlarmName']:a for a in baseline['alarms']['MetricAlarms']}
    if {a['AlarmName'] for a in alarms['MetricAlarms']}!=set(old):raise ValueError('Alarm set changed')
    volatile={'StateValue','StateReason','StateReasonData','StateUpdatedTimestamp','StateTransitionedTimestamp',
              'AlarmConfigurationUpdatedTimestamp'}
    for alarm in alarms['MetricAlarms']:
        before={k:v for k,v in old[alarm['AlarmName']].items() if k not in volatile}
        after={k:v for k,v in alarm.items() if k not in volatile}
        if before!=after:raise ValueError('Alarm definition changed: '+alarm['AlarmName'])
    return {'routing_matches_baseline':True,'confirmed_subscription_arn':subs['Subscriptions'][0]['SubscriptionArn'],
            'alarm_names':sorted(old),'new_email':False,'checked_at_utc':now()}

def review_native(cf,state,phase,plan):
    review=cf.describe_change_set(ChangeSetName=state['change_set_id'])
    expected=DESTINATION if phase=='import' else STREAMING
    if not review.get('StackId','').startswith(f'arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/{expected}/'):
        raise ValueError('Wrong native change-set stack')
    changes=validate_changes(review,phase,plan)
    return review,changes

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--phase',required=True,choices=['prepare-import','review-import','execute-import','prepare-update','review-update','execute-update','status'])
    parser.add_argument('--work-dir',required=True,type=Path);args=parser.parse_args()
    plan=verify_files();baseline=load(BASE/'before.json')
    import boto3
    from botocore.config import Config
    config=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=20)
    session=boto3.Session(region_name=REGION)
    clients={service:session.client(service,config=config) for service in ['sts','cloudformation','sns','events','cloudwatch','lambda']}
    if clients['sts'].get_caller_identity()['Account']!=ACCOUNT:raise ValueError('Wrong AWS account')
    cf=clients['cloudformation'];work=args.work_dir
    if args.phase=='status':
        report={'observed_at_utc':now(),'statuses':{},'events':{},'read_only':True}
        for name in [DESTINATION,STREAMING]:
            try:
                report['statuses'][name]=cf.describe_stacks(StackName=name)['Stacks'][0]
                report['events'][name]=cf.describe_stack_events(StackName=name)['StackEvents'][:20]
            except Exception as error:report['statuses'][name]={'error_code':getattr(error,'response',{}).get('Error',{}).get('Code',type(error).__name__)}
        work.mkdir(parents=True,exist_ok=True)
        path=work/('status_'+datetime.now(timezone.utc).strftime('%H%M%S%f')+'.json');save_new(path,report)
        print(json.dumps({'output':str(path),'statuses':{k:v.get('StackStatus',v.get('error_code')) for k,v in report['statuses'].items()}}));return
    phase=args.phase.rsplit('-',1)[1]
    state_path=work/(phase+'_change_set.json')
    if args.phase.startswith('prepare-'):
        if state_path.exists():raise FileExistsError('Preparation already recorded; do not duplicate')
        work.mkdir(parents=True,exist_ok=True)
        routing=guard_routing(clients,baseline)
        save_new(work/(phase+'_routing_guard.json'),routing)
        name='fiap-alerts-'+phase+'-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        file='import_template.json' if phase=='import' else 'streaming_update_template.json'
        template=(BASE/file).read_text(encoding='utf-8')
        validated=cf.validate_template(TemplateBody=template)
        save_new(work/(phase+'_template_validation.json'),validated)
        if phase=='import':
            # Native IMPORT must be used; CreateStack would recreate/re-subscribe.
            try:cf.describe_stacks(StackName=DESTINATION)
            except cf.exceptions.ClientError as error:
                if error.response['Error']['Code']!='ValidationError' or 'does not exist' not in error.response['Error']['Message']:raise
            else:raise ValueError('Destination stack now exists; inspect before importing')
            for item in plan['import_existing_resources']:
                identifier=next(iter(item['ResourceIdentifier'].values()))
                try:owner=cf.describe_stack_resources(PhysicalResourceId=identifier)
                except cf.exceptions.ClientError as error:
                    if error.response['Error']['Code']!='ValidationError' or not any(x in error.response['Error']['Message'].lower() for x in ['does not exist','not found']):raise
                else:
                    if owner.get('StackResources'):raise ValueError('Resource already managed by CloudFormation: '+item['LogicalResourceId'])
            response=cf.create_change_set(StackName=DESTINATION,ChangeSetName=name,ChangeSetType='IMPORT',
                TemplateBody=template,ResourcesToImport=plan['import_existing_resources'],
                Description='Import existing operational alerts only; no new subscriptions, service resources or jobs.')
        else:
            imported=cf.describe_stacks(StackName=DESTINATION)['Stacks'][0]
            if imported['StackStatus']!='IMPORT_COMPLETE':raise ValueError('Import must complete before streaming update')
            stack=cf.describe_stacks(StackName=STREAMING)['Stacks'][0]
            if stack['StackStatus']!='UPDATE_COMPLETE':raise ValueError('Streaming stack not stable')
            actual=cf.get_template(StackName=STREAMING,TemplateStage='Original')['TemplateBody']
            if isinstance(actual,str):actual=json.loads(actual)
            baseline_template=baseline['streaming_template']['TemplateBody']
            if isinstance(baseline_template,str):baseline_template=json.loads(baseline_template)
            if actual!=baseline_template:raise ValueError('Streaming source changed since baseline')
            parameters=[{'ParameterKey':p['ParameterKey'],'UsePreviousValue':True} for p in stack.get('Parameters',[])]
            parameters.append({'ParameterKey':'AlertTopicArn','ParameterValue':TOPIC})
            response=cf.create_change_set(StackName=STREAMING,ChangeSetName=name,ChangeSetType='UPDATE',
                TemplateBody=template,Parameters=parameters,Capabilities=['CAPABILITY_NAMED_IAM'],
                Description='Record existing SNS actions on three alarms; no changes to Lambda, role, queues or data.')
        state={'phase':phase,'created_at_utc':now(),'change_set_id':response['Id'],'stack_id':response['StackId'],
               'reviewed_plan_sha256':SOURCE_PLAN_SHA,'template_file':file,'template_sha256':sha((BASE/file).read_bytes())}
        save_new(state_path,state);print(json.dumps(state));return
    state=load(state_path)
    if state['reviewed_plan_sha256']!=SOURCE_PLAN_SHA:raise ValueError('Unreviewed saved state')
    review,changes=review_native(cf,state,phase,plan)
    print(json.dumps({'phase':phase,'native_status':review['Status'],'execution_status':review['ExecutionStatus'],
                     'changes':changes,'scope_gate':'PASS'},default=str))
    if args.phase.startswith('execute-'):
        if (work/(phase+'_execution_started.json')).exists():raise FileExistsError('Execution already requested')
        routing=guard_routing(clients,baseline)
        save_new(work/(phase+'_pre_execute_routing.json'),routing)
        response=cf.execute_change_set(ChangeSetName=state['change_set_id'],ClientRequestToken='fiap-alerts-'+phase+'-'+sha(state['change_set_id'].encode())[:32])
        save_new(work/(phase+'_execution_started.json'),{'requested_at_utc':now(),'change_set_id':state['change_set_id'],
          'changes':changes,'response':response,'emails_sent_by_script':0})
        print(json.dumps({'phase':phase,'execution_requested':True,'change_set_id':state['change_set_id']}))
    else:
        file=work/(phase+'_native_review.json')
        if file.exists():raise FileExistsError('Native review already saved; use the saved review or execute')
        save_new(file,review)

if __name__=='__main__':main()
