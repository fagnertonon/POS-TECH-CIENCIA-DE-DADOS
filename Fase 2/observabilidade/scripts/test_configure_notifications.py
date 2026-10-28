import json,tempfile,unittest
from pathlib import Path
import configure_notifications as n

class Missing(Exception):
    response={'Error':{'Code':'ResourceNotFoundException'}}

class Client:
    def __init__(self,service,calls,values):self.service,self.calls,self.values=service,calls,values
    def __getattr__(self,op):
        def run(**kw):
            self.calls.append((self.service,op,kw))
            value=self.values[op]
            if isinstance(value,Exception):raise value
            return value
        return run

def setup():
    calls=[]
    alarms=[{'AlarmName':x,'Namespace':'TEST','MetricName':'Failed','Dimensions':[],
      'Statistic':'Sum','Period':60,'EvaluationPeriods':1,'Threshold':1,
      'ComparisonOperator':'GreaterThanOrEqualToThreshold','TreatMissingData':'notBreaching',
      'ActionsEnabled':False,'AlarmActions':[],'OKActions':[],'InsufficientDataActions':[],
      'AlarmArn':'output-only','StateValue':'OK'} for x in n.ALARM_NAMES]
    values={'sts':{'get_caller_identity':{'Account':n.ACCOUNT}},
      'cloudwatch':{'describe_alarms':{'MetricAlarms':alarms},'put_metric_alarm':{}},
      'events':{'describe_rule':Missing(),'put_rule':{'RuleArn':n.RULE},'put_targets':{'FailedEntryCount':0}},
      'sns':{'list_topics':{'Topics':[]},'create_topic':{'TopicArn':n.TOPIC},
        'get_topic_attributes':{'Attributes':{'Policy':json.dumps({'Version':'2012-10-17','Statement':[]})}},
        'set_topic_attributes':{},'list_subscriptions_by_topic':{'Subscriptions':[]},
        'subscribe':{'SubscriptionArn':'pending confirmation'}}}
    return {s:Client(s,calls,v) for s,v in values.items()},calls,values

class Tests(unittest.TestCase):
    def test_alarm_update_preserves_metric_and_refuses_other_actions(self):
        clients,calls,values=setup()
        alarm=values['cloudwatch']['describe_alarms']['MetricAlarms'][0]
        updated=n.update_alarm(alarm)
        self.assertEqual(updated['Threshold'],alarm['Threshold'])
        self.assertEqual(updated['Dimensions'],alarm['Dimensions'])
        self.assertTrue(updated['ActionsEnabled'])
        self.assertEqual(updated['AlarmActions'],[n.TOPIC])
        self.assertNotIn('StateValue',updated)
        alarm['AlarmActions']=['arn:aws:autoscaling:unexpected']
        with self.assertRaises(ValueError):n.update_alarm(alarm)

    def test_policy_has_no_s3_iam_or_cross_account_publish(self):
        p=n.plan('owner@example.org')
        self.assertNotIn('owner@example.org',json.dumps(p))
        for statement in p['topic_policy_additions']:
            self.assertEqual(statement['Action'],'sns:Publish')
            self.assertEqual(statement['Resource'],n.TOPIC)
            self.assertEqual(statement['Condition']['StringEquals']['aws:SourceAccount'],n.ACCOUNT)
        cw=p['topic_policy_additions'][0]
        self.assertEqual(len(cw['Condition']['ArnEquals']['aws:SourceArn']),5)
        self.assertNotIn('ScheduleExpression',p)

    def test_configuration_subscribes_only_after_validated_alarm_updates(self):
        clients,calls,values=setup()
        with tempfile.TemporaryDirectory() as tmp:
            result=n.run(clients,'owner@example.org',Path(tmp)/'new')
        self.assertEqual(result['status'],'CONFIGURED_AWAITING_SUBSCRIPTION_AND_RECEIPT')
        self.assertEqual(calls[-1][1],'subscribe')
        self.assertEqual(len([c for c in calls if c[1]=='put_metric_alarm']),5)
        self.assertFalse(any(c[1] in {'start_job_run','publish','set_alarm_state'} for c in calls))

    def test_wrong_account_or_missing_alarm_stops_before_mutations(self):
        for condition in ['wrong-account','missing-alarm']:
            clients,calls,values=setup()
            if condition=='wrong-account':values['sts']['get_caller_identity']['Account']='wrong'
            else:values['cloudwatch']['describe_alarms']['MetricAlarms'].pop()
            with tempfile.TemporaryDirectory() as tmp,self.assertRaises(ValueError):
                n.run(clients,'owner@example.org',Path(tmp)/'new')
            self.assertFalse(any(op.startswith(('put_','create_','set_')) or op=='subscribe' for _,op,_ in calls))

    def test_existing_topic_cannot_expose_new_alarm_messages_to_unknown_subscribers(self):
        clients,calls,values=setup()
        values['sns']['list_topics']['Topics']=[{'TopicArn':n.TOPIC}]
        with tempfile.TemporaryDirectory() as tmp,self.assertRaisesRegex(ValueError,'already exists'):
            n.run(clients,'owner@example.org',Path(tmp)/'new')
        self.assertFalse(any(op.startswith(('put_','create_','set_')) or op=='subscribe' for _,op,_ in calls))

if __name__=='__main__':unittest.main()
