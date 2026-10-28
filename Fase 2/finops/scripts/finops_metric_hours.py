"""Measure active custom metric-hours of the three reference ETL runs. Read-only."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib,json
from pathlib import Path
from zipfile import ZipFile,ZIP_DEFLATED
import boto3
root=Path('/home/cloudshell-user/finops_controls_20261027_01')
metrics=json.loads((root/'custom_metric_inventory_corrected.json').read_text())
cases=[('bronze','fiap-bronze-inep-batch','jr_cdfd2933220ec75b45c04518c6e5afe93eb98915f3b88328f8bde8d7183d3700','2026-10-20T18:00:00+00:00','2026-10-20T19:00:00+00:00'),
('silver','fiap-silver-inep-batch','jr_9c58c50a3e452ab6fff7f8560524b5235aa0d0c92036feccc6768ce2499b0288','2026-10-22T13:00:00+00:00','2026-10-22T15:00:00+00:00'),
('gold','fiap-gold-inep-batch','jr_1ae29e48e526f050de2d2d637a5474387a37398466e7af56ed0584132f58e7fe','2026-10-27T00:00:00+00:00','2026-10-27T01:00:00+00:00')]
cw=boto3.client('cloudwatch',region_name='us-east-2')
assert boto3.client('sts').get_caller_identity()['Account']=='637246457028'
work=[]
for layer,job,run,start,end in cases:
    for m in metrics:
        dims={d['Name']:d['Value'] for d in m['Dimensions']}
        if m['Namespace']=='Glue' and dims.get('JobName')==job and dims.get('JobRunId') in [run,'ALL']:
            work.append((layer,run,start,end,m))
def collect(row):
    layer,run,start,end,m=row
    data=cw.get_metric_statistics(**m,StartTime=datetime.fromisoformat(start),EndTime=datetime.fromisoformat(end),Period=3600,Statistics=['SampleCount'])['Datapoints']
    return {'layer':layer,'reference_run_id':run,'start_utc':start,'end_exclusive_utc':end,'metric':m,'hours_with_samples':len(data),'sample_count':sum(x['SampleCount'] for x in data)}
with ThreadPoolExecutor(max_workers=6) as pool: rows=list(pool.map(collect,work))
report={'observed_at_utc':str(datetime.now(timezone.utc)),'method':'Count UTC hourly buckets with samples for matching JobRunId and ALL series; do not sum sample values as metric-hours.',
        'rows':rows,'sum_active_hours':sum(x['hours_with_samples'] for x in rows),'reference_runs':cases,
        'limitations':['ALL-series first Bronze reference hour also had another short Bronze run; this does not add another active hour to this identity.',
                       'Metric names/dimensions can grow if worker count, run count or observability settings change.',
                       'Excludes FIAP namespaces, separately measured from their emitted metric-hours.']}
out=Path('/home/cloudshell-user/finops_glue_metric_hours.json')
out.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'file':str(out),'queries':len(rows),'sum_hours':report['sum_active_hours'],'by_layer':{c[0]:sum(x['hours_with_samples'] for x in rows if x['layer']==c[0]) for c in cases},'sha256':hashlib.sha256(out.read_bytes()).hexdigest()}))
