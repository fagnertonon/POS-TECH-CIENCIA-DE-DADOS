import json, hashlib, io, subprocess, sys
from pathlib import Path
from urllib.parse import urlsplit
from datetime import datetime, timezone
import boto3
import pyarrow.parquet as pq

import argparse
parser = argparse.ArgumentParser(description='Conferência AWS somente leitura desta publicação Gold. Grava relatórios locais; não inicia Glue.')
parser.add_argument('--output-dir', type=Path, required=True)
args = parser.parse_args()
ROOT = Path(__file__).resolve().parent
RUN = 'jr_2f49f1ef178c48d81fd1f61569da095c16d1e58d8f7a15ce11ae9c4f6b84cc29'
ATTEMPT = 'jr_8544c209aa38c4721b3669728ad5c701a96e60e604c8e33ed536798061470c29'
OLD = 's3://fiap-tc-fase2-2026/auditoria/gold/ingestion_date=2026-10-22/run_id=jr_74dca58f19d0bd28dd27bba71b3c1a0bc420e55ec2dab74fb047e23b1901d4cd/manifest.json'
NEW = 's3://fiap-tc-fase2-2026/auditoria/gold/ingestion_date=2026-10-26/run_id=' + RUN + '/manifest.json'
session = boto3.Session(region_name='us-east-2')
assert session.client('sts').get_caller_identity()['Account'] == '637246457028'
s3, glue = session.client('s3'), session.client('glue')
run = glue.get_job_run(JobName='fiap-gold-inep-batch', RunId=RUN)['JobRun']
if run['JobRunState'] != 'SUCCEEDED':
    print(json.dumps({'state':run['JobRunState'],'error':run.get('ErrorMessage'),'seconds':run.get('ExecutionTime')},ensure_ascii=False))
    raise SystemExit(2)
OUT = args.output_dir.resolve()
OUT.mkdir(parents=True, exist_ok=False)
def save(name,obj):
    (OUT/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')
def read(uri):
    u=urlsplit(uri)
    return s3.get_object(Bucket=u.netloc,Key=u.path.lstrip('/'))['Body'].read()
new_bytes, old_bytes = read(NEW), read(OLD)
new, old = json.loads(new_bytes), json.loads(old_bytes)
assert new['status']=='SUCCEEDED' and new['script_version']=='1.0.2' and new['all_five_tables_validated']
assert hashlib.sha256(old_bytes).hexdigest()=='9091d56dc044b1459cd839dcc2f075045c4633b62b4373f9bb1ac7353e9974aa'
(OUT/'manifest_final.json').write_bytes(new_bytes)
(OUT/'manifest_anterior.json').write_bytes(old_bytes)
save('run_final.json',run)
attempt = glue.get_job_run(JobName='fiap-gold-inep-batch',RunId=ATTEMPT)['JobRun']
save('run_attempt1.json',attempt)
failed_uri=NEW.rsplit('/run_id=',1)[0]+'/run_id='+ATTEMPT+'/failed.json'
failed=json.loads(read(failed_uri))
save('failed_attempt1.json',failed)
assert attempt['JobRunState']=='FAILED' and failed.get('published') is False
u=urlsplit(NEW.rsplit('/run_id=',1)[0]+'/run_id='+ATTEMPT+'/manifest.json')
try:
    s3.head_object(Bucket=u.netloc,Key=u.path.lstrip('/'))
except s3.exceptions.ClientError as e:
    assert e.response['Error']['Code'] in ['404','NoSuchKey','NotFound']
else:
    raise AssertionError('Failed attempt has a publication manifest')
subprocess.run([sys.executable,str(ROOT/'verify_continuacao.py'),'--layer','gold','--run-id',RUN,'--verify-parquet','--output',str(OUT/'verificacao_parquet.json')],check=True)
observer=json.loads((OUT/'verificacao_parquet.json').read_text())
assert observer['published'] and observer['status']=='SUCCEEDED' and not observer['errors']
counts=lambda manifest:{r['table']:r['s3_count'] for r in manifest['table_metrics']}
assert counts(new)==counts(old)
assert sum(counts(new).values())==46342

def national(manifest):
    metric=next(x for x in manifest['table_metrics'] if x['table']=='validacao_cruzada_brasil')
    u=urlsplit(metric['s3_uri'])
    keys=[o['Key'] for page in s3.get_paginator('list_objects_v2').paginate(Bucket=u.netloc,Prefix=u.path.lstrip('/')) for o in page.get('Contents',[]) if o['Key'].endswith('.parquet')]
    rows=[]
    for key in sorted(keys):
        body=s3.get_object(Bucket=u.netloc,Key=key)['Body'].read()
        rows.extend(pq.ParquetFile(io.BytesIO(body)).read().to_pylist())
    return sorted(rows,key=lambda r:(r['ano'],r['serie'],r['rede']))
previous,current=national(old),national(new)
legacy=['ano','serie','rede','alunos_avaliados','taxa_alfabetizacao_calculada','taxa_oficial_publicada','diferenca_pp','dentro_tolerancia']
assert [{k:r[k] for k in legacy} for r in current]==[{k:r[k] for k in legacy} for r in previous]
assert len(current)==2
rows={r['ano']:r for r in current}
assert rows[2023]['quantidade_ufs_microdados']==23 and rows[2023]['ufs_resultados_sem_microdados']==['SP']
assert rows[2023]['ufs_sem_microdados']==['AC','DF','RR','SP']
assert rows[2024]['quantidade_ufs_microdados']==26 and rows[2024]['ufs_microdados_sem_resultado_uf']==['DF']
assert rows[2024]['ufs_sem_microdados']==['RR']
assert rows[2023]['dentro_tolerancia'] is False and rows[2024]['dentro_tolerancia'] is True
fixture=new['quality']['coverage_spark_fixture']
assert len(fixture)>=10 and all(r['passou'] for r in fixture if r['tipo']=='bloqueante')
assert new['coverage_spark_fixture']['enabled'] and new['coverage_spark_fixture']['writes_analytical_data'] is False
joins=new['quality']['all_construction_joins']
for label in ['taxa_correspondencia:validacao_brasil_oficial','taxa_correspondencia:validacao_brasil_cobertura']:
    assert sum(r['regra']==label for r in joins)==1
save('validacao_brasil_anterior.json',previous)
save('validacao_brasil_final.json',current)
units=[]
for r in [attempt,run]:
    dpu=r.get('DPUSeconds')
    estimated=dpu is None
    if estimated:dpu=r['ExecutionTime']*r['NumberOfWorkers']
    units.append({'run_id':r['Id'],'state':r['JobRunState'],'execution_seconds':r['ExecutionTime'],'dpu_seconds':dpu,'dpu_seconds_estimated':estimated,'glue_usd_at_0_44_per_dpu_hour':dpu/3600*0.44})
summary={'observed_at_utc':datetime.now(timezone.utc).isoformat(),'status':'SUCCEEDED','run_id':RUN,'script_version':'1.0.2','manifest_uri':NEW,'previous_manifest_unchanged_sha256':hashlib.sha256(old_bytes).hexdigest(),'five_counts_unchanged':counts(new),'total_gold_rows':46342,'all_parquet_footers_and_schemas_verified':True,'legacy_national_fields_unchanged':True,'coverage_spark_fixture_checks':len(fixture),'synthetic_joins_excluded_from_production_metrics':True,'national_rows':current,'runs_this_update':units,'estimated_glue_only_usd':sum(r['glue_usd_at_0_44_per_dpu_hour'] for r in units),'price_basis':'USD0.44 per DPU-hour reference rate checked for Ohio on2026-10-26; model, not the invoice or account balance','bigquery_queries_this_update':0,'new_infrastructure_resources':[],'iam_changes':False}
save('resumo_atualizacao.json',summary)
print(json.dumps({'status':summary['status'],'run_id':RUN,'counts':counts(new),'glue_only_estimated_usd':summary['estimated_glue_only_usd'],'output_dir':str(OUT)},ensure_ascii=False,indent=2))
