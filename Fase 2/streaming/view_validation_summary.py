"""Display only facts from the completed replay's saved verification reports."""
import argparse
import json
from pathlib import Path

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--project-dir',type=Path,default=Path(__file__).resolve().parent)
a=p.parse_args()
root=a.project_dir
e=root/'exercise_01'
load=lambda path:json.loads(path.read_text(encoding='utf-8'))
v=load(e/'verification_final.json')
n=load(e/'verification_negative.json')
f=load(e/'finalized/finalization_report.json')
o=load(root/'official_preservation.json')
r=load(e/'runtime_evidence.json')
assert v['validation_status']==n['validation_status']==f['status']==o['status']=='SUCCEEDED'
print('FIAP - STREAMING ATE GOLD DE SIMULACAO - VALIDADO NA AWS')
print('Execucao:',v['run_id'])
print('Bronze / Silver / contribuicoes Gold:',*[v['inventory'][k]['actual'] for k in ('bronze','silver','gold')])
print('Parquet Snappy: Silver',f['datasets']['silver']['rows'],'linhas; Gold',f['datasets']['gold']['rows'],'grupos')
print('Eventos aceitos e duplicatas:',r['logs']['positive_run']['status_counts'])
print('Peso invalido:',n['logs']['observed_invalid_payload_failures'],'falhas; DLQ preservada:',n['dlq']['matched']['matches_original_event'])
print('Prefixos oficiais identicos:',sum(x['unchanged'] for x in o['official_prefixes'].values()),'de 6')
l=v['end_to_end_latency_seconds']
print('Latencia ate Gold (s): media %.3f / p95 %.3f / maxima %.3f'%(l['mean'],l['p95_nearest_rank'],l['max']))
print('Lambda: memoria',r['lambda_configuration']['MemorySize'],'MB; timeout',r['lambda_configuration']['Timeout'],'s; concorrencia',r['lambda_concurrency']['ReservedConcurrentExecutions'])
print('Fila principal:',n['source_queue']['attributes'])
t=r['logs']['exclusive_target_invocation_totals']
print('Medidos:',t['invocations_with_report'],'invocacoes; soma billed',t['billed_duration_ms_sum']/1000,'s; memoria maxima',t['max_memory_used_mb'],'MB')
print('Sem novas execucoes Glue/BigQuery ou produtor recorrente. FinOps: proxima etapa.')
if (e/'audit_publication.json').exists():
    print('Auditoria S3:',load(e/'audit_publication.json')['status'])
