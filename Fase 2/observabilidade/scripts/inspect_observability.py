"""Bounded read-only alarm/SNS discovery; no email, subscription or policy changes."""
import argparse, hashlib, json
from datetime import datetime, timezone
from pathlib import Path

ACCOUNT='637246457028'
ALARMS=['fiap-silver-inep-failure','fiap-gold-inep-failure',
        'fiap-inep-streaming-dlq','fiap-inep-streaming-rejected','fiap-inep-streaming-age']

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args()
    import boto3
    from botocore.config import Config
    cfg=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=15)
    clients={s:boto3.client(s,region_name='us-east-2',config=cfg) for s in ['sts','sns','cloudwatch']}
    result={'captured_at_utc':datetime.now(timezone.utc).isoformat(),'mutations':[], 'calls':[],'errors':[]}
    def read(service,operation,**params):
        result['calls'].append(service+':'+operation)
        try:return getattr(clients[service],operation)(**params)
        except Exception as e:
            response=getattr(e,'response',{})
            result['errors'].append({'operation':service+':'+operation,
               'code':response.get('Error',{}).get('Code'),
               'message':response.get('Error',{}).get('Message'),
               'request_id':response.get('ResponseMetadata',{}).get('RequestId')})
            return None
    identity=read('sts','get_caller_identity')
    if not identity or identity.get('Account')!=ACCOUNT:raise ValueError('Account could not be verified')
    result['account']=ACCOUNT
    topics=read('sns','list_topics')
    result['topics']=[x['TopicArn'] for x in topics.get('Topics',[])] if topics is not None else None
    result['topics_complete']=topics is not None and not topics.get('NextToken')
    subs=read('sns','list_subscriptions')
    result['subscriptions_complete']=subs is not None and not subs.get('NextToken')
    result['subscriptions']=[] if subs is not None else None
    for sub in (subs or {}).get('Subscriptions',[]):
        endpoint=sub.get('Endpoint','')
        masked=endpoint[:1]+'***@'+endpoint.rsplit('@',1)[-1] if '@' in endpoint else '<redacted>'
        result['subscriptions'].append({k:sub.get(k) for k in ['TopicArn','Protocol','SubscriptionArn','Owner']}|
            {'endpoint_masked':masked,'endpoint_sha256':hashlib.sha256(endpoint.encode()).hexdigest()})
    alarms=read('cloudwatch','describe_alarms',AlarmNames=ALARMS)
    result['alarms']=[{k:r.get(k) for k in ['AlarmName','AlarmArn','AlarmDescription','ActionsEnabled',
        'AlarmActions','OKActions','InsufficientDataActions','StateValue','Namespace','MetricName',
        'Dimensions','Statistic','Period','EvaluationPeriods','DatapointsToAlarm','Threshold',
        'ComparisonOperator','TreatMissingData']} for r in (alarms or {}).get('MetricAlarms',[])]
    result['alarm_query_succeeded']=alarms is not None
    result['collector_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    out=Path(a.output)
    with out.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps({'output':str(out),'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),
        'calls':len(result['calls']),'errors':len(result['errors'])}))

if __name__=='__main__':main()
