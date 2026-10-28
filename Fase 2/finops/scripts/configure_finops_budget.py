"""Configure only the explicitly authorized monitoring budget and existing email."""
import json,hashlib
from pathlib import Path
from datetime import datetime,timezone
from zipfile import ZipFile,ZIP_DEFLATED
import boto3
from botocore.exceptions import ClientError

out=Path('/home/cloudshell-user/finops_budget_20261027')
out.mkdir(exist_ok=False)
account='637246457028'
assert boto3.client('sts').get_caller_identity()['Account']==account
budgets=boto3.client('budgets',region_name='us-east-1')
source='My Zero-Spend Budget'
notes=budgets.describe_notifications_for_budget(AccountId=account,BudgetName=source)['Notifications']
addresses=set()
for note in notes:
    rows=budgets.describe_subscribers_for_notification(AccountId=account,BudgetName=source,Notification=note)['Subscribers']
    addresses.update(r['Address'] for r in rows if r['SubscriptionType']=='EMAIL')
assert len(addresses)==1,'Expected exactly one existing email; refusing another recipient'
address=next(iter(addresses))
name='FIAP-Acompanhamento-Bruto-30USD'
desired={'BudgetName':name,'BudgetLimit':{'Amount':'30','Unit':'USD'},'TimeUnit':'MONTHLY','BudgetType':'COST',
         'CostTypes':{'IncludeCredit':False,'IncludeRefund':False,'IncludeTax':True,'UseBlended':False,'UseAmortized':False}}
notifications=[{'Notification':{'NotificationType':'ACTUAL','ComparisonOperator':'GREATER_THAN','Threshold':v,
                              'ThresholdType':'ABSOLUTE_VALUE'},
                'Subscribers':[{'SubscriptionType':'EMAIL','Address':address}]} for v in [15,24,30]]
try:
    old=budgets.describe_budget(AccountId=account,BudgetName=name)['Budget']
except ClientError as error:
    if error.response['Error']['Code']!='NotFoundException':raise
    budgets.create_budget(AccountId=account,Budget=desired,NotificationsWithSubscribers=notifications)
    operation='created'
else:
    assert old['BudgetLimit']['Amount'] in ['30','30.0'] and old['TimeUnit']=='MONTHLY' and old['CostTypes']['IncludeCredit'] is False
    operation='already_matches'
after=budgets.describe_budget(AccountId=account,BudgetName=name)['Budget']
assert float(after['BudgetLimit']['Amount'])==30 and after['CostTypes']['IncludeCredit'] is False and after['CostTypes']['IncludeRefund'] is False
ns=budgets.describe_notifications_for_budget(AccountId=account,BudgetName=name)['Notifications']
assert sorted(float(n['Threshold']) for n in ns)==[15,24,30]
for n in ns:
    ss=budgets.describe_subscribers_for_notification(AccountId=account,BudgetName=name,Notification=n)['Subscribers']
    assert ss==[{'SubscriptionType':'EMAIL','Address':address}]
    assert n['NotificationType']=='ACTUAL' and n['ThresholdType']=='ABSOLUTE_VALUE' and n['ComparisonOperator']=='GREATER_THAN'
report={'observed_at_utc':str(datetime.now(timezone.utc)),'operation':operation,'budget_name':name,
        'account':account,'budget_limit_usd':30,'time_unit':'MONTHLY','total_project_ceiling_usd':140,
        'credits_included':False,'refunds_included':False,'notifications_usd':[15,24,30],
        'subscriber_type':'EMAIL','subscriber_count':1,'same_recipient_as_source_budget':True,
        'recipient_verification_status':'Check console; API does not expose verification status',
        'recipient_address_exported':False,'automatic_resource_actions':[],
        'authorization':'Human explicitly approved alerts using the already registered budget email on2026-10-27.',
        'note':'USD30 is a monthly monitoring reserve, not the project estimate or a renewable USD140 budget. No hard cap, upgrade, IAM change or new Glue execution.'}
(out/'budget_applied_redacted.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
safe={k:v for k,v in after.items() if k!='NotificationSubscribers'}
(out/'budget_after.json').write_text(json.dumps(safe,ensure_ascii=False,indent=2,default=str)+'\n')
(out/'notifications_redacted.json').write_text(json.dumps(ns,ensure_ascii=False,indent=2)+'\n')
files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir()}
(out/'SHA256.json').write_text(json.dumps(files,indent=2)+'\n')
archive=out.parent/(out.name+'.zip')
with ZipFile(archive,'w',ZIP_DEFLATED) as z:
    for p in out.iterdir():z.write(p,p.name)
print(json.dumps({'report':report,'zip':str(archive),'sha256':hashlib.sha256(archive.read_bytes()).hexdigest()},ensure_ascii=False))
