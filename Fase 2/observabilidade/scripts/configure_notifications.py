"""Prepare/review first; apply only the explicit project notification plan."""
import argparse, hashlib, json, re
from datetime import datetime, timezone
from pathlib import Path

ACCOUNT='637246457028'
REGION='us-east-2'
TOPIC_NAME='fiap-inep-alertas-operacionais'
TOPIC=f'arn:aws:sns:{REGION}:{ACCOUNT}:{TOPIC_NAME}'
RULE_NAME='fiap-inep-batch-failures'
RULE=f'arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}'
ALARM_NAMES=['fiap-gold-inep-failure','fiap-silver-inep-failure',
 'fiap-inep-streaming-dlq','fiap-inep-streaming-rejected','fiap-inep-streaming-age']
JOBS=['fiap-bronze-inep-batch','fiap-silver-inep-batch','fiap-gold-inep-batch']
PATTERN={'source':['aws.glue'],'detail-type':['Glue Job State Change'],
 'detail':{'jobName':JOBS,'state':['FAILED','TIMEOUT','STOPPED','ERROR','EXPIRED']}}

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':')).encode()
def sha(value):return hashlib.sha256(canonical(value)).hexdigest()

def plan(email):
    if not re.fullmatch(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}',email):raise ValueError('Invalid email')
    return {'schema':'fiap.operational-notifications.v1','account':ACCOUNT,'region':REGION,
        'topic_arn':TOPIC,'rule_arn':RULE,'alarm_names':ALARM_NAMES,'job_names':JOBS,
        'event_pattern':PATTERN,'recipient_masked':email[0]+'***@'+email.rsplit('@',1)[-1],
        'recipient_sha256':hashlib.sha256(email.encode()).hexdigest(),
        'notifications':['SNS subscription confirmation','Operational failure/latency/DLQ alerts','One identified CloudWatch routing test'],
        'role_changes':[],'jobs_started':[],'schedules_created':[],
        'topic_policy_additions':[
            {'Sid':'OnlyProjectCloudWatchAlarms','Effect':'Allow','Principal':{'Service':'cloudwatch.amazonaws.com'},
             'Action':'sns:Publish','Resource':TOPIC,'Condition':{'StringEquals':{'aws:SourceAccount':ACCOUNT},
              'ArnEquals':{'aws:SourceArn':[f'arn:aws:cloudwatch:{REGION}:{ACCOUNT}:alarm:{n}' for n in ALARM_NAMES]}}},
            {'Sid':'OnlyEventBridgeFromThisAccount','Effect':'Allow','Principal':{'Service':'events.amazonaws.com'},
             'Action':'sns:Publish','Resource':TOPIC,'Condition':{'StringEquals':{'aws:SourceAccount':ACCOUNT}}}],
        'scope_note':'SNS contains operational metadata only; no pupil payloads. EventBridge publishes restricted to this account and topic. Rule matches the three named jobs, without a schedule.',
        'cost_reference':{'SNS_requests_usd_per_million':'0.50','SNS_email_usd_per_100000':'2.00',
         'example_100_notifications_gross_usd':'0.00205',
         'note':'Illustration before free allowances; not measured usage or a billing cap. Existing alarm count unchanged. AWS service events on the default bus have no ingestion charge.'}}

def update_alarm(existing):
    # These five existing alarms use simple, single-metric definitions. Retain
    # every supported input from the live object instead of rebuilding thresholds.
    if existing.get('AlarmName') not in ALARM_NAMES:raise ValueError('Unapproved alarm')
    if existing.get('Metrics') or existing.get('AlarmType')=='CompositeAlarm':raise ValueError('Unexpected alarm type')
    allowed={'AlarmName','AlarmDescription','ActionsEnabled','OKActions','AlarmActions','InsufficientDataActions',
      'MetricName','Namespace','Statistic','ExtendedStatistic','Dimensions','Period','Unit','EvaluationPeriods',
      'DatapointsToAlarm','Threshold','ComparisonOperator','TreatMissingData','EvaluateLowSampleCountPercentile',
      'ThresholdMetricId'}
    params={k:v for k,v in existing.items() if k in allowed and v is not None}
    for actions in ['AlarmActions','OKActions','InsufficientDataActions']:
        if any(x!=TOPIC for x in params.get(actions,[])):raise ValueError('Unexpected existing alarm action; stop for review')
    params.update(ActionsEnabled=True,AlarmActions=[TOPIC])
    return params

def run(clients,email,output):
    folder=Path(output)
    folder.mkdir(parents=True,exist_ok=False)
    approved=plan(email)
    (folder/'plan.json').write_bytes(canonical(approved)+b'\n')
    if clients['sts'].get_caller_identity()['Account']!=ACCOUNT:raise ValueError('Wrong account')
    alarms=clients['cloudwatch'].describe_alarms(AlarmNames=ALARM_NAMES)['MetricAlarms']
    if {a['AlarmName'] for a in alarms}!=set(ALARM_NAMES):raise ValueError('Expected all five existing alarms')
    updates=[update_alarm(a) for a in alarms]  # All validation before first mutation.
    (folder/'alarms_before.json').write_text(json.dumps(alarms,default=str,indent=2)+'\n',encoding='utf-8')
    topics=clients['sns'].list_topics()
    if topics.get('NextToken'):raise ValueError('Topic listing incomplete; stop for review')
    if any(t.get('TopicArn')==TOPIC for t in topics.get('Topics',[])):
        raise ValueError('Target topic already exists; inspect its policy/subscribers before a reviewed retry')
    try:
        existing=clients['events'].describe_rule(Name=RULE_NAME)
    except Exception as e:
        if getattr(e,'response',{}).get('Error',{}).get('Code')!='ResourceNotFoundException':raise
    else:
        if json.loads(existing.get('EventPattern','{}'))!=PATTERN:raise ValueError('Existing rule differs; stop for review')
    created=clients['sns'].create_topic(Name=TOPIC_NAME,Tags=[{'Key':'Project','Value':'FIAP-TC-Fase2'}])
    if created['TopicArn']!=TOPIC:raise ValueError('Unexpected topic owner/region')
    attributes=clients['sns'].get_topic_attributes(TopicArn=TOPIC)['Attributes']
    (folder/'topic_before.json').write_text(json.dumps(attributes,indent=2)+'\n',encoding='utf-8')
    policy=json.loads(attributes['Policy'])
    managed_sids={s['Sid'] for s in approved['topic_policy_additions']}
    policy['Statement']=[s for s in policy.get('Statement',[]) if s.get('Sid') not in managed_sids]+approved['topic_policy_additions']
    clients['sns'].set_topic_attributes(TopicArn=TOPIC,AttributeName='Policy',AttributeValue=json.dumps(policy))
    clients['events'].put_rule(Name=RULE_NAME,EventPattern=json.dumps(PATTERN),State='ENABLED',
        Description='FIAP: operational failures of the three batch jobs; no schedule')
    target=clients['events'].put_targets(Rule=RULE_NAME,Targets=[{'Id':'OperationalSNS','Arn':TOPIC,
        'InputTransformer':{'InputPathsMap':{'job':'$.detail.jobName','run':'$.detail.jobRunId',
          'state':'$.detail.state','time':'$.time'},
          'InputTemplate':'{"kind":"FIAP_BATCH_FAILURE","job":<job>,"run_id":<run>,"status":<state>,"occurred_at":<time>,"action":"Inspect job logs and persistent quality report before retry"}'},
        'RetryPolicy':{'MaximumRetryAttempts':3,'MaximumEventAgeInSeconds':3600}}])
    if target.get('FailedEntryCount',0):raise ValueError('EventBridge target was not accepted')
    for params in updates:clients['cloudwatch'].put_metric_alarm(**params)
    # Do not resend confirmations for an existing endpoint on a rerun.
    subscriptions=[];token=None
    for _ in range(3):
        response=clients['sns'].list_subscriptions_by_topic(TopicArn=TOPIC,**({'NextToken':token} if token else {}))
        subscriptions.extend(response.get('Subscriptions',[]));token=response.get('NextToken')
        if not token:break
    if token:raise ValueError('Subscription listing incomplete; stop before subscribing')
    matching=[s for s in subscriptions if s.get('Protocol')=='email' and s.get('Endpoint')==email]
    result={'topic_arn':TOPIC,'rule_arn':RULE,'alarm_names':ALARM_NAMES,'plan_sha256':sha(approved),
            'status':'CONFIGURED_AWAITING_SUBSCRIPTION_AND_RECEIPT','subscription_created':not bool(matching)}
    if matching:result['subscription_arn']=matching[0]['SubscriptionArn']
    else:result.update(clients['sns'].subscribe(TopicArn=TOPIC,Protocol='email',Endpoint=email,ReturnSubscriptionArn=True))
    result['finished_at_utc']=datetime.now(timezone.utc).isoformat()
    (folder/'result.json').write_bytes(canonical(result)+b'\n')
    return result

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--email',required=True)
    p.add_argument('--output-dir',required=True);p.add_argument('--apply',action='store_true')
    a=p.parse_args()
    if not a.apply:
        out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=False)
        (out/'plan.json').write_bytes(canonical(plan(a.email))+b'\n')
        print(json.dumps({'status':'PLAN_ONLY','plan_sha256':sha(plan(a.email))}));return
    import boto3
    from botocore.config import Config
    cfg=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=15)
    clients={s:boto3.client(s,region_name=REGION,config=cfg) for s in ['sts','sns','cloudwatch','events']}
    print(json.dumps(run(clients,a.email,a.output_dir)))

if __name__=='__main__':main()
