"""Build exact import/update templates from a saved live snapshot; local only."""
from pathlib import Path
import copy,hashlib,json

BASE=Path(__file__).resolve().parent
ACCOUNT='637246457028'
REGION='us-east-2'
STACK='fiap-inep-streaming'
DESTINATION='fiap-inep-observabilidade'
TOPIC=f'arn:aws:sns:{REGION}:{ACCOUNT}:fiap-inep-alertas-operacionais'
STREAM_ALARMS={'DeadLetterAlarm':'fiap-inep-streaming-dlq',
               'RejectedEventAlarm':'fiap-inep-streaming-rejected','QueueAgeAlarm':'fiap-inep-streaming-age'}
ALARM_FIELDS={'AlarmName','AlarmDescription','ActionsEnabled','OKActions','AlarmActions',
 'InsufficientDataActions','MetricName','Namespace','Statistic','ExtendedStatistic','Dimensions',
 'Period','Unit','EvaluationPeriods','DatapointsToAlarm','Threshold','ComparisonOperator',
 'TreatMissingData','EvaluateLowSampleCountPercentile','ThresholdMetricId'}

def encode(value):return (json.dumps(value,ensure_ascii=False,indent=2)+'\n').encode('utf-8')
def sha(data):return hashlib.sha256(data).hexdigest()
def load(name):return json.loads((BASE/name).read_bytes())
def main():
    source=load('before.json')
    assert source['account']==ACCOUNT and source['region']==REGION
    assert source['errors']==[{'key':'destination_stack','operation':'cloudformation:describe_stacks',
      'code':'ValidationError','message':f'Stack with id {DESTINATION} does not exist'}]
    assert not source['streaming_resources'].get('NextToken')
    assert not source['subscriptions'].get('NextToken') and not source['targets'].get('NextToken')
    subs=source['subscriptions']['Subscriptions'];assert len(subs)==1
    subscription=subs[0]
    assert subscription['TopicArn']==TOPIC and subscription['Protocol']=='email'
    attrs=source['subscription_attributes_0']['Attributes']
    assert attrs['PendingConfirmation']=='false' and attrs['SubscriptionArn']==subscription['SubscriptionArn']
    assert sha(subscription['Endpoint'].encode())=='48c4f6955cf77e73245356ebd7ef01bbcc41bc35cd7d809f36779d3884561fb4'
    alarms={a['AlarmName']:a for a in source['alarms']['MetricAlarms']};assert len(alarms)==5
    for alarm in alarms.values():
        assert alarm['ActionsEnabled'] and alarm['AlarmActions']==[TOPIC]
        assert not alarm.get('Metrics')
    assert source['topic']['Attributes']['Owner']==ACCOUNT
    assert source['rule']['State']=='ENABLED' and not source['rule'].get('ScheduleExpression')
    prior=Path(__file__).resolve().parents[1]/'observabilidade_20261027/raw/notifications_verified_04.json'
    old=json.loads(prior.read_bytes())
    policy=json.loads(source['topic']['Attributes']['Policy'])
    assert policy==old['topic_policy'],'Unexpected live policy change'
    assert source['targets']['Targets']==old['targets'],'Unexpected live target change'
    assert json.loads(source['rule']['EventPattern'])==json.loads(old['rule']['EventPattern'])
    def symbolize(value):
        if isinstance(value,list):return [symbolize(x) for x in value]
        if isinstance(value,dict):return {k:symbolize(v) for k,v in value.items()}
        return {'Ref':'AlertTopic'} if value==TOPIC else value
    def resource(kind,props):
        return {'Type':kind,'DeletionPolicy':'Retain','UpdateReplacePolicy':'Retain','Properties':props}
    topic_props={'TopicName':TOPIC.rsplit(':',1)[1]}
    display=source['topic']['Attributes'].get('DisplayName')
    if display:topic_props['DisplayName']=display
    tags=source['topic_tags'].get('Tags',[])
    if tags:topic_props['Tags']=tags
    sub_props={'TopicArn':{'Ref':'AlertTopic'},'Protocol':'email','Endpoint':subscription['Endpoint']}
    for key in ['DeliveryPolicy','FilterPolicy','RedrivePolicy']:
        if attrs.get(key):sub_props[key]=json.loads(attrs[key])
    rule=source['rule']
    rule_props={key:copy.deepcopy(rule[key]) for key in ['Name','Description','State','EventBusName'] if key in rule}
    rule_props['EventPattern']=json.loads(rule['EventPattern'])
    rule_props['Targets']=symbolize(source['targets']['Targets'])
    tags=source['rule_tags'].get('Tags',[])
    if tags:rule_props['Tags']=tags
    resources={
      'AlertTopic':resource('AWS::SNS::Topic',topic_props),
      'AlertSubscription':resource('AWS::SNS::Subscription',sub_props),
      'AlertTopicPolicy':resource('AWS::SNS::TopicInlinePolicy',{'TopicArn':{'Ref':'AlertTopic'},'PolicyDocument':symbolize(policy)}),
      'BatchFailureRule':resource('AWS::Events::Rule',rule_props),
    }
    for logical,name in [('SilverFailureAlarm','fiap-silver-inep-failure'),('GoldFailureAlarm','fiap-gold-inep-failure')]:
        resources[logical]=resource('AWS::CloudWatch::Alarm',symbolize({k:v for k,v in alarms[name].items() if k in ALARM_FIELDS}))
    template={'AWSTemplateFormatVersion':'2010-09-09',
      'Description':'FIAP: importacao de recursos operacionais existentes; preservar assinatura e configuracao, sem criar alertas adicionais.',
      'Resources':resources}
    identifiers={
      'AlertTopic':{'TopicArn':TOPIC},'AlertSubscription':{'Arn':subscription['SubscriptionArn']},
      'AlertTopicPolicy':{'TopicArn':TOPIC},'BatchFailureRule':{'Arn':rule['Arn']},
      'SilverFailureAlarm':{'AlarmName':'fiap-silver-inep-failure'},
      'GoldFailureAlarm':{'AlarmName':'fiap-gold-inep-failure'},
    }
    import_types={x['ResourceType']:set(x['ResourceIdentifiers']) for x in source['import_identifiers']['ResourceIdentifierSummaries']}
    to_import=[]
    for name,identifier in identifiers.items():
        kind=resources[name]['Type'];assert set(identifier)<=import_types[kind]
        to_import.append({'ResourceType':kind,'LogicalResourceId':name,'ResourceIdentifier':identifier})
    current=source['streaming_template']['TemplateBody']
    if isinstance(current,str):current=json.loads(current)
    update=copy.deepcopy(current)
    assert set(update['Parameters'])=={'BucketName'}
    update['Parameters']['AlertTopicArn']={'Type':'String','Default':TOPIC,'AllowedValues':[TOPIC],
      'Description':'Topico operacional existente desta conta/regiao; sem novo destinatario.'}
    for logical,name in STREAM_ALARMS.items():
        props=update['Resources'][logical]['Properties'];assert props['AlarmName']==name
        props.update(ActionsEnabled=True,AlarmActions=[{'Ref':'AlertTopicArn'}],
                     OKActions=alarms[name].get('OKActions',[]),
                     InsufficientDataActions=alarms[name].get('InsufficientDataActions',[]))
    changed=[name for name in current['Resources'] if current['Resources'][name]!=update['Resources'][name]]
    assert set(changed)==set(STREAM_ALARMS)
    assert set(current['Resources'])==set(update['Resources'])
    assert current['Outputs']==update['Outputs']
    package={'import_template.json':template,'resources_to_import.json':to_import,
             'streaming_update_template.json':update}
    for name,value in package.items():
        target=BASE/name
        with target.open('xb') as f:f.write(encode(value))
    plan={'schema':'fiap.cfn-alert-migration.v1','account':ACCOUNT,'region':REGION,
      'before_snapshot_sha256':sha((BASE/'before.json').read_bytes()),
      'import_stack':DESTINATION,'streaming_stack':STACK,'import_existing_resources':to_import,
      'streaming_modified_logical_ids':changed,'new_operational_resources':[],
      'delete_or_replace_resources':[],'new_subscriptions':[],'new_emails':[],
      'iam_changes':[],'job_runs':[],'replays':[],'project_total_ceiling_usd':140,
      'template_hashes':{name:sha((BASE/name).read_bytes()) for name in package},
      'subscription_preserved_arn':subscription['SubscriptionArn'],
      'approved_scope':'Human explicitly requested migration of existing alerts to CloudFormation.',
      'status':'LOCAL_PLAN_READY_FOR_NATIVE_CHANGE_SET_REVIEW'}
    with (BASE/'migration_plan.json').open('xb') as f:f.write(encode(plan))
    print(json.dumps({'status':plan['status'],'import_count':len(to_import),'streaming_changes':changed,
                     'plan_sha256':sha(encode(plan)),'new_operational_resources':0,'emails':0}))

if __name__=='__main__':main()
