"""Bounded read-only AWS snapshot for importing existing operational alerts.

Does not publish, subscribe, change permissions, start jobs, invoke Lambda or read SQS messages.
"""
import argparse,hashlib,json
from pathlib import Path
from datetime import datetime,timezone

ACCOUNT='637246457028'
REGION='us-east-2'
STACK='fiap-inep-streaming'
DESTINATION='fiap-inep-observabilidade'
TOPIC=f'arn:aws:sns:{REGION}:{ACCOUNT}:fiap-inep-alertas-operacionais'
RULE='fiap-inep-batch-failures'
ALARMS=['fiap-gold-inep-failure','fiap-silver-inep-failure','fiap-inep-streaming-dlq',
        'fiap-inep-streaming-rejected','fiap-inep-streaming-age']
TYPES=['AWS::SNS::Topic','AWS::SNS::Subscription','AWS::SNS::TopicInlinePolicy',
       'AWS::Events::Rule','AWS::CloudWatch::Alarm']

def digest(data):return hashlib.sha256(data).hexdigest()
def collect(clients):
    account=clients['sts'].get_caller_identity()['Account']
    if account!=ACCOUNT:raise ValueError('Unexpected account')
    result={'account':account,'region':REGION,'started_at_utc':datetime.now(timezone.utc).isoformat(),
            'read_only':True,'mutations':[],'emails_sent':0,'calls':[],'errors':[]}
    def read(key,client,operation,**kwargs):
        result['calls'].append(client+':'+operation)
        try:
            data=getattr(clients[client],operation)(**kwargs)
            data.pop('ResponseMetadata',None)
            result[key]=data;return data
        except Exception as error:
            item={'key':key,'operation':client+':'+operation,
                  'code':getattr(error,'response',{}).get('Error',{}).get('Code',type(error).__name__),
                  'message':getattr(error,'response',{}).get('Error',{}).get('Message','')[:2000]}
            result['errors'].append(item);return None
    read('streaming_stack','cloudformation','describe_stacks',StackName=STACK)
    read('streaming_template','cloudformation','get_template',StackName=STACK,TemplateStage='Original')
    read('streaming_resources','cloudformation','list_stack_resources',StackName=STACK)
    read('destination_stack','cloudformation','describe_stacks',StackName=DESTINATION)
    read('alarms','cloudwatch','describe_alarms',AlarmNames=ALARMS)
    read('topic','sns','get_topic_attributes',TopicArn=TOPIC)
    subscriptions=read('subscriptions','sns','list_subscriptions_by_topic',TopicArn=TOPIC)
    if subscriptions and not subscriptions.get('NextToken'):
        for index,item in enumerate(subscriptions.get('Subscriptions',[])):
            arn=item['SubscriptionArn']
            if arn not in ('PendingConfirmation','Deleted','pending confirmation'):
                read('subscription_attributes_'+str(index),'sns','get_subscription_attributes',SubscriptionArn=arn)
    read('topic_tags','sns','list_tags_for_resource',ResourceArn=TOPIC)
    rule=read('rule','events','describe_rule',Name=RULE)
    read('targets','events','list_targets_by_rule',Rule=RULE)
    if rule:read('rule_tags','events','list_tags_for_resource',ResourceARN=rule['Arn'])
    read('lambda_config','lambda','get_function_configuration',FunctionName='fiap-inep-streaming-consumer')
    # GetFunctionConfiguration has no signed code URL; keep only code/configuration metadata.
    if result.get('lambda_config'):
        source=result['lambda_config'];allowed=['FunctionName','FunctionArn','Runtime','Role','Timeout',
                 'MemorySize','CodeSha256','CodeSize','LastModified','State','LastUpdateStatus','RevisionId']
        result['lambda_config']={key:source[key] for key in allowed if key in source}
    for type_name in TYPES:
        data=read('type_'+type_name,'cloudformation','describe_type',Type='RESOURCE',TypeName=type_name)
        if data and data.get('Schema'):
            schema=json.loads(data['Schema'])
            result['type_'+type_name]={key:data[key] for key in ['Type','TypeName','Arn','DefaultVersionId'] if key in data}
            result['type_'+type_name].update({key:schema.get(key) for key in ['primaryIdentifier','readOnlyProperties','createOnlyProperties','handlers']})
    template=(result.get('streaming_template') or {}).get('TemplateBody')
    if isinstance(template,str):template=json.loads(template)
    if template:
        sample={'AWSTemplateFormatVersion':'2010-09-09','Resources':{
          'AlertTopic':{'Type':'AWS::SNS::Topic','DeletionPolicy':'Retain','Properties':{'TopicName':'fiap-inep-alertas-operacionais'}},
          'AlertSubscription':{'Type':'AWS::SNS::Subscription','DeletionPolicy':'Retain','Properties':{'TopicArn':TOPIC,'Protocol':'email','Endpoint':'placeholder@example.invalid'}},
          'AlertTopicPolicy':{'Type':'AWS::SNS::TopicInlinePolicy','DeletionPolicy':'Retain','Properties':{'TopicArn':TOPIC,'PolicyDocument':{'Version':'2012-10-17','Statement':[]}}},
          'BatchFailureRule':{'Type':'AWS::Events::Rule','DeletionPolicy':'Retain','Properties':{'Name':RULE,'EventPattern':{'source':['aws.glue']}}},
          'GoldFailureAlarm':{'Type':'AWS::CloudWatch::Alarm','DeletionPolicy':'Retain','Properties':{'AlarmName':'fiap-gold-inep-failure','ComparisonOperator':'GreaterThanOrEqualToThreshold','EvaluationPeriods':1,'MetricName':'Failed','Namespace':'FIAP/Medallion','Period':60,'Statistic':'Sum','Threshold':1}},
        }}
        read('import_identifiers','cloudformation','get_template_summary',TemplateBody=json.dumps(sample))
    result['finished_at_utc']=datetime.now(timezone.utc).isoformat()
    return result

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True,type=Path);a=parser.parse_args()
    if a.output.exists():raise FileExistsError('Use a fresh output file')
    import boto3
    from botocore.config import Config
    session=boto3.Session(region_name=REGION)
    config=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=15)
    clients={service:session.client(service,config=config) for service in ['sts','cloudformation','cloudwatch','sns','events','lambda']}
    result=collect(clients)
    data=(json.dumps(result,indent=2,ensure_ascii=False,default=str)+'\n').encode('utf-8')
    with a.output.open('xb') as out:out.write(data)
    print(json.dumps({'output':str(a.output),'sha256':digest(data),'read_only':True,
                     'calls':len(result['calls']),'errors':result['errors'],'account':result['account']}))

if __name__=='__main__':main()
