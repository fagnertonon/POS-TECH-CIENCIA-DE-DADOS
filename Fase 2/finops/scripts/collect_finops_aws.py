"""Read-only FIAP FinOps inventory. No secrets, jobs, queries or mutations."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import urllib.request
from zipfile import ZipFile, ZIP_DEFLATED
import boto3
from botocore.config import Config

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output-dir', type=Path, required=True)
parser.add_argument('--include-cost-explorer', action='store_true', help='One paginated read of MTD Usage costs/quantities; CE API can cost USD0.01 per page')
args = parser.parse_args()
OUT = args.output_dir.expanduser().resolve()
OUT.mkdir(parents=True, exist_ok=False)
REGION, ACCOUNT = 'us-east-2', '637246457028'
session = boto3.Session(region_name=REGION)
config = Config(retries={'max_attempts':2,'mode':'standard'}, connect_timeout=10, read_timeout=25)
def client(name, region=REGION): return session.client(name, region_name=region, config=config)
assert client('sts').get_caller_identity()['Account'] == ACCOUNT
now = datetime.now(timezone.utc)
end = now.replace(hour=0, minute=0, second=0, microsecond=0)
start = end-timedelta(days=7)
errors = []
def save(name, obj):
    (OUT/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
def attempt(label, fn, default=None):
    try: return fn()
    except Exception as error:
        errors.append({'operation':label,'error_type':type(error).__name__,'message':str(error)[:600]})
        return default
def pages(c, op, key, **kw):
    if c.can_paginate(op):
        return [row for page in c.get_paginator(op).paginate(**kw) for row in page.get(key,[])]
    result=[]
    while True:
        response=getattr(c,op)(**kw)
        result.extend(response.get(key,[]))
        token=response.get('NextToken')
        if not token: return result
        kw['NextToken']=token
def trim(obj, fields): return {k:obj[k] for k in fields if k in obj}
save('collection_scope.json',{'observed_at_utc':now,'account':ACCOUNT,'region':REGION,
     'metric_start_utc':start,'metric_end_exclusive_utc':end,'aws_mutations':[],
     'starts_glue_jobs':False,'reads_secrets':False,'starts_bigquery_queries':False,
     'includes_cost_explorer':args.include_cost_explorer})

glue = client('glue')
jobs = attempt('glue.get_jobs',lambda:pages(glue,'get_jobs','Jobs'),[])
job_rows = []
all_runs = []
for job in jobs:
    if not job['Name'].startswith('fiap'): continue
    row = trim(job,['Name','WorkerType','NumberOfWorkers','MaxCapacity','MaxRetries','Timeout','ExecutionProperty','GlueVersion','ExecutionClass','CreatedOn','LastModifiedOn'])
    row['tags'] = attempt('glue.get_tags:'+job['Name'],lambda:glue.get_tags(ResourceArn=f"arn:aws:glue:{REGION}:{ACCOUNT}:job/{job['Name']}").get('Tags',{}),{})
    row['source_connection_names'] = job.get('Connections',{}).get('Connections',[])
    row['default_argument_keys'] = sorted(job.get('DefaultArguments',{}))
    row['selected_default_arguments'] = {k:v for k,v in job.get('DefaultArguments',{}).items() if k in ['--job-bookmark-option','--enable-auto-scaling','--enable-metrics','--VERIFY_COVERAGE_FIXTURE','--SILVER_MANIFEST_URI','--DATABASE_GOLD','--DATABASE_SILVER','--S3_BUCKET']}
    runs = attempt('glue.get_job_runs:'+job['Name'],lambda:pages(glue,'get_job_runs','JobRuns',JobName=job['Name']),[])
    for run in runs:
        all_runs.append(dict(job_name=job['Name'],**trim(run,['Id','JobRunState','StartedOn','CompletedOn','ExecutionTime','DPUSeconds','WorkerType','NumberOfWorkers','MaxCapacity','ExecutionClass','GlueVersion','ErrorMessage'])))
    job_rows.append(row)
save('glue_jobs.json',job_rows)
save('glue_runs.json',all_runs)
save('glue_sessions.json',attempt('glue.list_sessions',lambda:[trim(x,['Id','Status','CreatedOn','CompletedOn','IdleTimeout','Timeout','NumberOfWorkers','WorkerType','ExecutionTime','DPUSeconds']) for x in pages(glue,'list_sessions','Sessions')],[]))
save('glue_triggers.json',attempt('glue.get_triggers',lambda:[trim(x,['Name','Type','State','Schedule','Actions']) for x in pages(glue,'get_triggers','Triggers')],[]))
catalog_tables = []
for db in ['tech_challenge_fase2_silver','tech_challenge_fase2_gold']:
    tabs = attempt('glue.get_tables:'+db,lambda:pages(glue,'get_tables','TableList',DatabaseName=db),[])
    catalog_tables.extend({'database':db,'name':t['Name'],'create_time':t.get('CreateTime'),'update_time':t.get('UpdateTime')} for t in tabs)
save('catalog_tables.json',catalog_tables)
print('COLLECTED_GLUE',len(job_rows),len(all_runs),flush=True)

s3 = client('s3')
buckets = []
for bucket in ['fiap-tc-fase2-2026','aws-glue-assets-637246457028-us-east-2']:
    objects = attempt('s3.list_objects_v2:'+bucket,lambda:pages(s3,'list_objects_v2','Contents',Bucket=bucket),[])
    prefix_rows = defaultdict(lambda:Counter(objects=0,bytes=0,parquet_objects=0,parquet_bytes=0))
    storage = Counter()
    for obj in objects:
        key = obj['Key']
        prefix = '/'.join(key.split('/')[:2]) if '/' in key else key
        prefix_rows[prefix]['objects'] += 1; prefix_rows[prefix]['bytes'] += obj['Size']
        storage[obj.get('StorageClass','STANDARD')] += obj['Size']
        if key.endswith('.parquet'):
            prefix_rows[prefix]['parquet_objects'] += 1; prefix_rows[prefix]['parquet_bytes'] += obj['Size']
    versions = attempt('s3.get_bucket_versioning:'+bucket,lambda:s3.get_bucket_versioning(Bucket=bucket),{})
    lifecycle = attempt('s3.get_bucket_lifecycle_configuration:'+bucket,lambda:s3.get_bucket_lifecycle_configuration(Bucket=bucket),{})
    version_list = attempt('s3.list_object_versions:'+bucket,lambda:pages(s3,'list_object_versions','Versions',Bucket=bucket),[])
    multipart = attempt('s3.list_multipart_uploads:'+bucket,lambda:pages(s3,'list_multipart_uploads','Uploads',Bucket=bucket),[])
    buckets.append({'bucket':bucket,'objects':len(objects),'bytes':sum(o['Size'] for o in objects),
        'storage_class_bytes':dict(storage),'prefixes':dict(prefix_rows),
        'versioning':trim(versions,['Status','MFADelete']), 'lifecycle_rules':lifecycle.get('Rules',[]),
        'noncurrent_version_objects':sum(not o.get('IsLatest',True) for o in version_list),
        'noncurrent_version_bytes':sum(o['Size'] for o in version_list if not o.get('IsLatest',True)),
        'multipart_uploads':len(multipart),
        'object_inventory':[trim(o,['Key','Size','LastModified','StorageClass']) for o in objects]})
save('s3_inventory.json',buckets)
print('COLLECTED_S3',sum(b['bytes'] for b in buckets),flush=True)

logs = client('logs')
groups = attempt('logs.describe_log_groups',lambda:pages(logs,'describe_log_groups','logGroups'),[])
groups = [trim(g,['logGroupName','storedBytes','retentionInDays','creationTime','logGroupClass']) for g in groups if any(x in g['logGroupName'] for x in ['fiap','aws-glue'])]
save('log_groups.json',groups)
lam = client('lambda')
functions = attempt('lambda.list_functions',lambda:pages(lam,'list_functions','Functions'),[])
functions = [trim(f,['FunctionName','Runtime','MemorySize','Timeout','Architectures','LastModified','CodeSize']) for f in functions if f['FunctionName'].startswith('fiap')]
for f in functions:
    name = f['FunctionName']
    f['concurrency'] = attempt('lambda.get_function_concurrency:'+name,lambda:lam.get_function_concurrency(FunctionName=name).get('ReservedConcurrentExecutions'))
    f['event_source_mappings'] = attempt('lambda.list_event_source_mappings:'+name,lambda:[trim(m,['UUID','EventSourceArn','State','StateTransitionReason','BatchSize','MaximumBatchingWindowInSeconds','ScalingConfig','LastModified']) for m in pages(lam,'list_event_source_mappings','EventSourceMappings',FunctionName=name)],[])
save('lambda_config.json',functions)
sqs = client('sqs')
queue_urls = attempt('sqs.list_queues',lambda:pages(sqs,'list_queues','QueueUrls',QueueNamePrefix='fiap'),[])
queues = []
for url in queue_urls:
    attrs = attempt('sqs.get_queue_attributes',lambda:sqs.get_queue_attributes(QueueUrl=url,AttributeNames=['All']).get('Attributes',{}),{})
    queues.append({'queue_name':url.rsplit('/',1)[-1],'queue_url':url,
        'attributes':{k:v for k,v in attrs.items() if k in ['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible','MessageRetentionPeriod','VisibilityTimeout','ReceiveMessageWaitTimeSeconds','RedrivePolicy','CreatedTimestamp']}})
save('sqs_config.json',queues)
secrets = attempt('secretsmanager.list_secrets',lambda:pages(client('secretsmanager'),'list_secrets','SecretList'),[])
save('secret_metadata.json',[trim(s,['Name','CreatedDate','LastChangedDate','LastAccessedDate','DeletedDate','Tags']) for s in secrets])

cw = client('cloudwatch')
alarms = attempt('cloudwatch.describe_alarms',lambda:pages(cw,'describe_alarms','MetricAlarms'),[])
alarms = [trim(a,['AlarmName','Namespace','MetricName','Dimensions','Period','EvaluationPeriods','StateValue','ActionsEnabled','AlarmActions','OKActions','InsufficientDataActions','Metrics']) for a in alarms if a['AlarmName'].startswith('fiap')]
save('cloudwatch_alarms.json',alarms)
metrics = []
def metric(namespace,name,dimensions,stat='Sum',period=86400):
    data = attempt('cloudwatch.get_metric_statistics:'+namespace+'/'+name,lambda:cw.get_metric_statistics(Namespace=namespace,MetricName=name,Dimensions=dimensions,StartTime=start,EndTime=end,Period=period,Statistics=[stat]).get('Datapoints',[]),[])
    metrics.append({'namespace':namespace,'metric_name':name,'dimensions':dimensions,'statistic':stat,'period_seconds':period,'start_utc':start,'end_exclusive_utc':end,'datapoints':sorted(data,key=lambda x:x['Timestamp'])})
for f in functions:
    dim=[{'Name':'FunctionName','Value':f['FunctionName']}]
    for name in ['Invocations','Duration','Errors','Throttles']: metric('AWS/Lambda',name,dim)
    group='/aws/lambda/'+f['FunctionName']
    streams = attempt('logs.describe_log_streams:'+group,lambda:pages(logs,'describe_log_streams','logStreams',logGroupName=group),[])
    reports=[]
    for stream in streams:
        if stream.get('lastEventTimestamp',0) < int(start.timestamp()*1000): continue
        events = attempt('logs.filter_log_events:'+group,lambda:pages(logs,'filter_log_events','events',logGroupName=group,logStreamNames=[stream['logStreamName']],startTime=int(start.timestamp()*1000),endTime=int(end.timestamp()*1000),filterPattern='"REPORT RequestId"'),[])
        for e in events:
            msg=e['message']
            billed=re.search(r'Billed Duration:\s*([\d.]+)\s*ms',msg)
            memory=re.search(r'Memory Size:\s*(\d+)\s*MB',msg)
            request=re.search(r'REPORT RequestId:\s*([\w-]+)',msg)
            if billed and memory: reports.append({'timestamp':e['timestamp'],'request_id':request.group(1) if request else None,'billed_duration_ms':float(billed.group(1)),'memory_mb':int(memory.group(1))})
    save('lambda_billing_reports_'+f['FunctionName']+'.json',reports)
for q in queues:
    for name in ['NumberOfEmptyReceives','NumberOfMessagesSent','NumberOfMessagesReceived','NumberOfMessagesDeleted','SentMessageSize']:
        metric('AWS/SQS',name,[{'Name':'QueueName','Value':q['queue_name']}])
for g in groups:
    metric('AWS/Logs','IncomingBytes',[{'Name':'LogGroupName','Value':g['logGroupName']}])
for pipeline in [('FIAP/Streaming','Pipeline','fiap-inep-streaming'),('FIAP/Medallion','Pipeline','silver'),('FIAP/Medallion','Pipeline','gold')]:
    found=attempt('cloudwatch.list_metrics:'+pipeline[0]+'/'+pipeline[2],lambda:pages(cw,'list_metrics','Metrics',Namespace=pipeline[0],Dimensions=[{'Name':pipeline[1],'Value':pipeline[2]}]),[])
    save('custom_metrics_'+pipeline[2]+'.json',found)
save('cloudwatch_daily_metrics.json',metrics)
print('COLLECTED_METRICS',len(metrics),flush=True)

budgets = client('budgets','us-east-1')
bs = attempt('budgets.describe_budgets',lambda:pages(budgets,'describe_budgets','Budgets',AccountId=ACCOUNT),[])
budget_rows=[]
for b in bs:
    row = trim(b,['BudgetName','BudgetLimit','CalculatedSpend','TimeUnit','TimePeriod','BudgetType','CostTypes','CostFilters','LastUpdatedTime'])
    notes = attempt('budgets.describe_notifications_for_budget',lambda:pages(budgets,'describe_notifications_for_budget','Notifications',AccountId=ACCOUNT,BudgetName=b['BudgetName']),[])
    row['notifications']=[]
    for n in notes:
        subscribers=attempt('budgets.describe_subscribers_for_notification',lambda:pages(budgets,'describe_subscribers_for_notification','Subscribers',AccountId=ACCOUNT,BudgetName=b['BudgetName'],Notification=n),[])
        row['notifications'].append({'notification':n,'subscriber_types':[x.get('SubscriptionType') for x in subscribers],'subscriber_count':len(subscribers)})
    budget_rows.append(row)
save('budgets_redacted.json',budget_rows)

if args.include_cost_explorer:
    ce=client('ce','us-east-1')
    request={'TimePeriod':{'Start':now.date().replace(day=1).isoformat(),'End':(now.date()+timedelta(days=1)).isoformat()},'Granularity':'MONTHLY','Metrics':['UnblendedCost','UsageQuantity'],'GroupBy':[{'Type':'DIMENSION','Key':'SERVICE'},{'Type':'DIMENSION','Key':'USAGE_TYPE'}],'Filter':{'Dimensions':{'Key':'RECORD_TYPE','Values':['Usage']}}}
    responses=[]
    for n in range(5):
        response=attempt('ce.get_cost_and_usage',lambda:ce.get_cost_and_usage(**request))
        if response is None: break
        responses.append(response)
        token=response.get('NextPageToken')
        if not token: break
        request['NextPageToken']=token
    save('cost_explorer_usage_types.json',{'request':{k:v for k,v in request.items() if k!='NextPageToken'},'pages':responses,'api_pages':len(responses),'estimated_ce_query_usd':0.01*len(responses)})
    print('COLLECTED_COST_EXPLORER',len(responses),flush=True)

price_rows=[]
for service in ['AWSGlue','AmazonS3','AmazonCloudWatch','AWSLambda','AWSQueueService','AWSSecretsManager']:
    def collect_prices():
        url=f'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/{REGION}/index.json'
        with urllib.request.urlopen(url,timeout=25) as response:
            data=json.load(response)
        records=[]
        for sku,product in data.get('products',{}).items():
            attrs=product.get('attributes',{})
            for offer in data.get('terms',{}).get('OnDemand',{}).get(sku,{}).values():
                for dim in offer.get('priceDimensions',{}).values():
                    records.append({'service':service,'sku':sku,'family':product.get('productFamily'),'attributes':attrs,
                        'unit':dim.get('unit'),'begin_range':dim.get('beginRange'),'end_range':dim.get('endRange'),
                        'usd':dim.get('pricePerUnit',{}).get('USD'),'description':dim.get('description'),
                        'source_url':url,'publication_date':data.get('publicationDate'),'effective_date':offer.get('effectiveDate')})
        return records
    found=attempt('public_price_list:'+service,collect_prices,[])
    price_rows.extend(found)
    print('COLLECTED_PRICE_LIST',service,len(found),flush=True)
save('regional_price_list.json',price_rows)
save('errors.json',errors)
files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in OUT.iterdir() if p.is_file()}
save('SHA256.json',files)
archive=OUT.parent/(OUT.name+'.zip')
with ZipFile(archive,'w',ZIP_DEFLATED) as z:
    for p in sorted(OUT.iterdir()): z.write(p,p.name)
print('FINOPS_COMPLETE',json.dumps({'folder':str(OUT),'zip':str(archive),'zip_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'errors':len(errors),'runs_collected':len(all_runs),'bytes_s3':sum(x['bytes'] for x in buckets)}),flush=True)
