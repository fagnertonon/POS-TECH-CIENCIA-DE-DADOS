"""Read GCP metadata and historical telemetry. Never query tables or enable billing."""
import base64,gzip,hashlib,json,subprocess,textwrap
from pathlib import Path
from datetime import datetime,timezone
import google.auth
from google.auth.transport.requests import AuthorizedSession

PROJECT='fiap1-512214'
out=Path.home()/'finops_gcp_20261027'
out.mkdir(exist_ok=False)
now=datetime.now(timezone.utc)
errors=[]
def save(n,obj): (out/n).write_text(json.dumps(obj,ensure_ascii=False,indent=2)+'\n')
def cli(args):
    p=subprocess.run(args,text=True,capture_output=True,timeout=100)
    if p.returncode:
        errors.append({'command':args[:5],'returncode':p.returncode,'error':p.stderr[-1500:]})
        return None
    try:return json.loads(p.stdout)
    except Exception:return {'text':p.stdout[:4000]}
actual=subprocess.check_output(['gcloud','config','get-value','project'],text=True).strip()
assert actual==PROJECT,actual
dataset=cli(['bq','--quiet','--project_id='+PROJECT,'show','--format=prettyjson','basedosdados:br_inep_avaliacao_alfabetizacao'])
dataset={k:dataset[k] for k in ['id','location','creationTime','lastModifiedTime'] if dataset and k in dataset}
save('source_dataset_metadata.json',dataset)
billing=cli(['gcloud','billing','projects','describe',PROJECT,'--format=json(projectId,billingEnabled)'])
save('project_billing.json',billing)
audit_filter='timestamp >= "2026-10-20T00:00:00Z" timestamp < "2026-10-28T00:00:00Z" (protoPayload.methodName:"ReadRows" OR protoPayload.methodName:"CreateReadSession" OR protoPayload.metadata.tableDataRead.reason="READ_ROWS")'
audit=cli(['gcloud','logging','read',audit_filter,'--project='+PROJECT,'--order=asc','--limit=10000','--format=json(timestamp,insertId,logName,resource,protoPayload.methodName,protoPayload.resourceName,protoPayload.metadata.tableDataRead)'])
save('read_audit_logs.json',{'filter':audit_filter,'limit':10000,'entries':audit})
creds,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform.read-only'])
http=AuthorizedSession(creds)
telemetry={}
for metric in ['serviceruntime.googleapis.com/api/response_sizes','serviceruntime.googleapis.com/api/request_count']:
    params={'filter':f'metric.type="{metric}" AND resource.type="consumed_api" AND resource.labels.service="bigquerystorage.googleapis.com"',
            'interval.startTime':'2026-10-20T00:00:00Z','interval.endTime':now.isoformat().replace('+00:00','Z'),
            'view':'FULL','pageSize':1000}
    series=[];pages=0
    while True:
        response=http.get(f'https://monitoring.googleapis.com/v3/projects/{PROJECT}/timeSeries',params=params,timeout=30)
        if response.status_code!=200:
            errors.append({'operation':'monitoring.timeSeries.list','metric':metric,'http_status':response.status_code,'body':response.text[:1200]})
            break
        data=response.json();series.extend(data.get('timeSeries',[]));pages+=1
        token=data.get('nextPageToken')
        if not token:break
        if pages>=10:raise RuntimeError('Monitoring output requires more pages; do not truncate silently')
        params['pageToken']=token
    telemetry[metric]={'filter':params['filter'],'pages':pages,'series':series}
save('monitoring_storage_api.json',telemetry)
summary={'project':PROJECT,'observed_at_utc':now.isoformat(),'dataset':dataset,'billing':billing,
         'table_queries_started':0,'read_sessions_started':0,'billing_changes':False,
         'audit_entries_available':len(audit) if isinstance(audit,list) else None,
         'audit_limit_reached':isinstance(audit,list) and len(audit)>=10000,
         'response_size_bytes_by_method':{},'requests_by_method':{},'scanned_bytes':None,'serialized_bytes':None,
         'limits':['API response_sizes is observed telemetry, not proof of a billed egress SKU.',
                   'Empty audit logs or unavailable byte fields are not zero consumption.',
                   'No SQL query, table download, storage read session, trial or billing linkage was created.']}
for metric,record in telemetry.items():
    dest=summary['response_size_bytes_by_method'] if metric.endswith('response_sizes') else summary['requests_by_method']
    for series in record['series']:
        labels=series.get('resource',{}).get('labels',{})
        method=labels.get('method','unknown')
        total=0
        for point in series.get('points',[]):
            value=point.get('value',{})
            if 'distributionValue' in value:
                v=value['distributionValue'];total+=float(v.get('mean',0))*int(v.get('count',0))
            else:total+=int(value.get('int64Value',0))
        dest[method]=dest.get(method,0)+total
if isinstance(audit,list):
    seen=set();scanned=[];serialized=[]
    for e in audit:
        identity=(e.get('logName'),e.get('insertId'),e.get('timestamp'))
        if identity in seen:continue
        seen.add(identity)
        detail=e.get('protoPayload',{}).get('metadata',{}).get('tableDataRead',{})
        if 'scannedBytes' in detail:scanned.append(int(detail['scannedBytes']))
        if 'serializedResponseBytes' in detail:serialized.append(int(detail['serializedResponseBytes']))
    if scanned:summary['scanned_bytes']=sum(scanned)
    if serialized:summary['serialized_bytes']=sum(serialized)
save('summary.json',summary);save('errors.json',errors)
files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir()}
save('SHA256.json',files)
package={p.name:json.loads(p.read_text()) for p in out.iterdir()}
body=json.dumps(package,ensure_ascii=False,separators=(',',':')).encode()
packed=gzip.compress(body)
print('FINOPS_GCP_SUMMARY',json.dumps(summary,ensure_ascii=False))
print('FINOPS_GCP_EXPORT_BEGIN')
print('\n'.join(textwrap.wrap(base64.b64encode(packed).decode(),100)))
print('FINOPS_GCP_EXPORT_END')
print('FINOPS_GCP_GZIP_SHA256',hashlib.sha256(packed).hexdigest())
