"""Read-only evidence for the single authorized alarm-state notification test."""
import argparse,hashlib,json
from datetime import datetime,timedelta,timezone
from pathlib import Path

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args()
    home=Path('/home/cloudshell-user')
    request=json.loads((home/'observability_notification_test_20261027/request.json').read_bytes())
    history=json.loads((home/'notification_alarm_history_20261027_01.json').read_bytes())
    routing=json.loads((home/'notifications_verified_20261027_04.json').read_bytes())
    assert routing['all_subscriptions_confirmed']
    start=datetime.fromisoformat(request['started_at_utc'])
    matching=[]
    for item in history['items']:
        data=item['data']
        published=json.loads(data.get('publishedMessage','{}'))
        notice=json.loads(published.get('default','{}'))
        if (data.get('actionState')=='Succeeded' and not data.get('error')
            and data.get('notificationResource')==request['topic_arn']
            and notice.get('AlarmName')==request['alarm_name']
            and notice.get('NewStateReason')==request['reason']
            and notice.get('NewStateValue')=='ALARM'
            and datetime.fromisoformat(item['timestamp'])>=start-timedelta(seconds=5)):
            matching.append({'timestamp':item['timestamp'],'summary':item['summary'],
              'notification_resource':data['notificationResource'],'action_state':data['actionState'],
              'alarm_name':notice['AlarmName'],'state_reason':notice['NewStateReason'],
              'state_change_time':notice['StateChangeTime']})
    assert matching and history['complete'],'Successful test action not established'
    import boto3
    from botocore.config import Config
    cw=boto3.client('cloudwatch',region_name='us-east-2',
        config=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=15))
    alarm=cw.describe_alarms(AlarmNames=[request['alarm_name']])['MetricAlarms'][0]
    assert alarm['AlarmActions']==[request['topic_arn']] and alarm['ActionsEnabled']
    stats={}
    end=datetime.now(timezone.utc)
    for metric in ['NumberOfNotificationsDelivered','NumberOfNotificationsFailed']:
        response=cw.get_metric_statistics(Namespace='AWS/SNS',MetricName=metric,
            Dimensions=[{'Name':'TopicName','Value':'fiap-inep-alertas-operacionais'}],
            StartTime=(start-timedelta(minutes=2)).replace(second=0,microsecond=0),
            EndTime=end+timedelta(seconds=1),Period=60,Statistics=['Sum'])
        points=response.get('Datapoints',[])
        stats[metric]={'datapoints':[{'timestamp':str(x['Timestamp']),'sum':x['Sum']} for x in points],
            'sum':sum(x['Sum'] for x in points) if points else None,
            'scope':'Topic and window; supporting provider telemetry, not proof of inbox receipt'}
    result={'status':'ALARM_NOTIFICATION_ACTION_VERIFIED','captured_at_utc':end.isoformat(),
        'alarm_action_success':True,'matching_actions':matching,
        'test_request_id':request['request_id'],'test_requested_at_utc':request['requested_at_utc'],
        'subscription_confirmed':True,'provider_metrics':stats,
        'current_alarm_state':alarm['StateValue'],'current_state_reason':alarm.get('StateReason'),
        'recipient_receipt_confirmed_by_this_collector':False,
        'source_history_sha256':hashlib.sha256((home/'notification_alarm_history_20261027_01.json').read_bytes()).hexdigest(),
        'collector_mutations':[],'new_test_notifications_sent':0,'jobs_started':[]}
    out=Path(a.output)
    with out.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps({'output':str(out),'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),
                     'status':result['status'],'current_alarm_state':result['current_alarm_state']}))

if __name__=='__main__':main()
