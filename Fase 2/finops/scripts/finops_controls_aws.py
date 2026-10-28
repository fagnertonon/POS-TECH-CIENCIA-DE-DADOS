"""Apply only a reversible legacy-job resource limit and collect missing metadata."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import argparse, hashlib, json
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import boto3

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--output-dir',type=Path,required=True)
p.add_argument('--apply-legacy-limit',action='store_true')
a=p.parse_args()
out=a.output_dir.expanduser().resolve();out.mkdir(parents=True,exist_ok=False)
s=boto3.Session(region_name='us-east-2')
assert s.client('sts').get_caller_identity()['Account']=='637246457028'
glue=s.client('glue');cw=s.client('cloudwatch');s3=s.client('s3')
def save(name,obj): (out/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
job_name='fiap-ingestao-bigquery-uf'
job=glue.get_job(JobName=job_name)['Job']
save('legacy_job_before.json',job)
runs=glue.get_job_runs(JobName=job_name,MaxResults=25)['JobRuns']
assert not any(x['JobRunState'] in ['STARTING','RUNNING','WAITING','STOPPING'] for x in runs),'Active legacy run'
report={'observed_at_utc':datetime.now(timezone.utc),'job_name':job_name,'applied':False,
        'before':{k:job.get(k) for k in ['NumberOfWorkers','WorkerType','Timeout','MaxRetries','ExecutionProperty']},
        'starts_job':False,'iam_changes':False,'deletes_data':False,'sends_notifications':False}
if a.apply_legacy_limit:
    assert (job['WorkerType'],job['NumberOfWorkers'],job['Timeout'],job['MaxRetries'])==('G.1X',10,480,0),'Configuration changed since review'
    allowed=glue.meta.service_model.operation_model('UpdateJob').input_shape.members['JobUpdate'].members
    update={k:deepcopy(v) for k,v in job.items() if k in allowed}
    update.pop('AllocatedCapacity',None);update.pop('MaxCapacity',None)
    update['NumberOfWorkers']=2;update['Timeout']=30
    glue.update_job(JobName=job_name,JobUpdate=update)
    after=glue.get_job(JobName=job_name)['Job']
    for k in update: assert after.get(k)==update[k],k
    save('legacy_job_after.json',after)
    report.update(applied=True,after={k:after.get(k) for k in ['NumberOfWorkers','WorkerType','Timeout','MaxRetries','ExecutionProperty']},
                  max_compute_at_timeout_before_usd=10*8*.44,max_compute_at_timeout_after_usd=2*.5*.44)
save('controls_report.json',report)

metrics=[]
for ns in ['FIAP/Medallion','FIAP/Streaming','Glue']:
    for page in cw.get_paginator('list_metrics').paginate(Namespace=ns):
        metrics.extend(page.get('Metrics',[]))
save('custom_metric_inventory_corrected.json',metrics)
end=datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
hours=[]
for m in metrics:
    if not m['Namespace'].startswith('FIAP/'):continue
    data=cw.get_metric_statistics(**m,StartTime=end-timedelta(days=7),EndTime=end,Period=3600,Statistics=['SampleCount'])['Datapoints']
    hours.append({'metric':m,'start_utc':end-timedelta(days=7),'end_exclusive_utc':end,
                  'hours_with_samples':len(data),'datapoints':sorted(data,key=lambda x:x['Timestamp'])})
save('fiap_metric_active_hours.json',hours)
gold_run='jr_1ae29e48e526f050de2d2d637a5474387a37398466e7af56ed0584132f58e7fe'
run=glue.get_job_run(JobName='fiap-gold-inep-batch',RunId=gold_run)['JobRun']
save('latest_gold_run.json',{k:v for k,v in run.items() if k!='Arguments'}|{'selected_arguments':{k:v for k,v in run.get('Arguments',{}).items() if k in ['--VERIFY_COVERAGE_FIXTURE','--DEMO_FAILURE','--SILVER_MANIFEST_URI']}})
key=f'auditoria/gold/ingestion_date=2026-10-27/run_id={gold_run}/manifest.json'
body=s3.get_object(Bucket='fiap-tc-fase2-2026',Key=key)['Body'].read()
manifest=json.loads(body)
assert manifest['status']=='SUCCEEDED' and manifest['all_five_tables_validated']
(out/'latest_gold_manifest.json').write_bytes(body)
save('latest_gold_manifest_source.json',{'uri':'s3://fiap-tc-fase2-2026/'+key,'sha256':hashlib.sha256(body).hexdigest()})
files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir()}
save('SHA256.json',files)
zpath=out.parent/(out.name+'.zip')
with ZipFile(zpath,'w',ZIP_DEFLATED) as z:
    for f in out.iterdir():z.write(f,f.name)
print(json.dumps({'controls':report,'metric_count':len(metrics),'latest_gold_fixture':manifest.get('coverage_spark_fixture'),'zip':str(zpath),'zip_sha256':hashlib.sha256(zpath.read_bytes()).hexdigest()},ensure_ascii=False,default=str))
