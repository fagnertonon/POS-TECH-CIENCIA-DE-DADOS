"""Read and verify notification routing. Does not confirm subscriptions or send."""
import argparse,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
import configure_notifications as n

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args()
    import boto3
    from botocore.config import Config
    cfg=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=15)
    clients={s:boto3.client(s,region_name=n.REGION,config=cfg) for s in ['sts','sns','cloudwatch','events']}
    assert clients['sts'].get_caller_identity()['Account']==n.ACCOUNT
    base=Path('/home/cloudshell-user/observability_notifications_apply_20261027')
    intended=json.loads((base/'plan.json').read_bytes())
    before=json.loads((base/'alarms_before.json').read_bytes())
    before_by_name={r['AlarmName']:n.update_alarm(r) for r in before}
    actual=clients['cloudwatch'].describe_alarms(AlarmNames=n.ALARM_NAMES)['MetricAlarms']
    checked=[]
    for row in actual:
        comparable={k:row[k] for k in before_by_name[row['AlarmName']] if k in row}
        expected=before_by_name[row['AlarmName']]
        assert comparable==expected,(row['AlarmName'],'Unexpected alarm configuration')
        checked.append({k:row[k] for k in ['AlarmName','AlarmArn','ActionsEnabled','AlarmActions','StateValue']})
    assert len(checked)==5
    topic=clients['sns'].get_topic_attributes(TopicArn=n.TOPIC)['Attributes']
    policy=json.loads(topic['Policy'])
    statements={x.get('Sid'):x for x in policy['Statement']}
    assert topic['Owner']==n.ACCOUNT
    assert all(statements[x['Sid']]==x for x in intended['topic_policy_additions'])
    rule=clients['events'].describe_rule(Name=n.RULE_NAME)
    assert rule['Arn']==n.RULE and rule['State']=='ENABLED'
    assert json.loads(rule['EventPattern'])==n.PATTERN and not rule.get('ScheduleExpression')
    targets=clients['events'].list_targets_by_rule(Rule=n.RULE_NAME)['Targets']
    assert len(targets)==1 and targets[0]['Arn']==n.TOPIC
    subscriptions=clients['sns'].list_subscriptions_by_topic(TopicArn=n.TOPIC)
    assert not subscriptions.get('NextToken')
    safe=[]
    for sub in subscriptions.get('Subscriptions',[]):
        endpoint=sub.get('Endpoint','')
        assert sub['Protocol']=='email' and hashlib.sha256(endpoint.encode()).hexdigest()==intended['recipient_sha256']
        safe.append({'protocol':'email','endpoint_masked':intended['recipient_masked'],
          'endpoint_sha256':intended['recipient_sha256'],'subscription_arn':sub['SubscriptionArn'],
          'confirmed':sub['SubscriptionArn'] not in ['PendingConfirmation','Deleted','pending confirmation']})
    result={'status':'ROUTING_VERIFIED','captured_at_utc':datetime.now(timezone.utc).isoformat(),
     'plan_sha256':n.sha(intended),'alarms':checked,'topic_arn':n.TOPIC,'topic_owner':topic['Owner'],
     'subscriptions':safe,'all_subscriptions_confirmed':bool(safe) and all(x['confirmed'] for x in safe),
     'topic_policy':policy,'rule':{k:rule.get(k) for k in ['Name','Arn','State','EventPattern']},
     'targets':targets,'verification_mutations':[],'emails_sent_by_verifier':0}
    out=Path(a.output)
    with out.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps({'output':str(out),'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),
        'status':result['status'],'confirmed':result['all_subscriptions_confirmed']}))

if __name__=='__main__':main()
