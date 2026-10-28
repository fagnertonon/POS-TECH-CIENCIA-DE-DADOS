"""Send exactly one approved, identified CloudWatch alarm test after confirmation."""
import argparse,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
import configure_notifications as n

def main():
    p=argparse.ArgumentParser();p.add_argument('--output-dir',required=True);a=p.parse_args()
    folder=Path(a.output_dir);folder.mkdir(parents=True,exist_ok=False)
    import boto3
    from botocore.config import Config
    cfg=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=15)
    clients={s:boto3.client(s,region_name=n.REGION,config=cfg) for s in ['sts','sns','cloudwatch']}
    assert clients['sts'].get_caller_identity()['Account']==n.ACCOUNT
    intended=json.loads(Path('/home/cloudshell-user/observability_notifications_apply_20261027/plan.json').read_bytes())
    subscriptions=clients['sns'].list_subscriptions_by_topic(TopicArn=n.TOPIC)
    assert not subscriptions.get('NextToken')
    subs=subscriptions.get('Subscriptions',[])
    assert len(subs)==1 and subs[0]['Protocol']=='email'
    assert hashlib.sha256(subs[0]['Endpoint'].encode()).hexdigest()==intended['recipient_sha256']
    assert subs[0]['SubscriptionArn'].startswith(n.TOPIC+':'),'Subscription is not confirmed'
    name='fiap-inep-streaming-rejected'
    alarm=clients['cloudwatch'].describe_alarms(AlarmNames=[name])['MetricAlarms'][0]
    assert alarm['ActionsEnabled'] and alarm['AlarmActions']==[n.TOPIC]
    assert not alarm.get('OKActions') and not alarm.get('InsufficientDataActions')
    started=datetime.now(timezone.utc).isoformat()
    reason='TESTE AUTORIZADO FIAP observabilidade 20261027. Somente rota de notificacao; nao representa nova falha de dados.'
    intent={'kind':'IDENTIFIED_NOTIFICATION_ROUTE_TEST','alarm_name':name,'started_at_utc':started,
        'previous_state':alarm['StateValue'],'topic_arn':n.TOPIC,'recipient_sha256':intended['recipient_sha256'],
        'reason':reason,'jobs_started':[],'events_replayed':0}
    (folder/'intent.json').write_text(json.dumps(intent,indent=2)+'\n',encoding='utf-8')
    if alarm['StateValue']=='ALARM':
        clients['cloudwatch'].set_alarm_state(AlarmName=name,StateValue='OK',
            StateReason='Preparacao do teste autorizado da rota; metricas continuam sendo avaliadas')
    response=clients['cloudwatch'].set_alarm_state(AlarmName=name,StateValue='ALARM',StateReason=reason)
    intent.update(status='TEST_REQUEST_ACCEPTED_AWAITING_RECEIPT',
        request_id=response.get('ResponseMetadata',{}).get('RequestId'),
        requested_at_utc=datetime.now(timezone.utc).isoformat())
    (folder/'request.json').write_text(json.dumps(intent,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(intent))

if __name__=='__main__':main()
