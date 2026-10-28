"""Create explicit measured drivers for the FIAP 30-day FinOps projection."""
from pathlib import Path
from decimal import Decimal as D
from collections import defaultdict
import hashlib,json

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'outputs/finops_2026_10_27'; RAW=OUT/'aws'; CTRL=OUT/'controls'
load=lambda p:json.loads(p.read_text(encoding='utf-8'))
rates=load(OUT/'tarifas_oficiais.json'); price=rates['unit_prices_usd']
prices={r['id']:r for r in rates['prices']}
scope=load(RAW/'collection_scope.json')
ce=load(RAW/'cost_explorer_usage_types.json')
usage={}; services=defaultdict(D)
for page in ce['pages']:
    for period in page['ResultsByTime']:
        for g in period['Groups']:
            service,kind=g['Keys'];q=g['Metrics']['UsageQuantity'];c=g['Metrics']['UnblendedCost']
            usage[(service,kind)]={'quantity':D(q['Amount']),'unit':q['Unit'],'cost':D(c['Amount'])}
            services[service]+=D(c['Amount'])
total=sum(services.values())
assert total==D('3.4142741034')
GB=D(2)**30
buckets=load(RAW/'s3_inventory.json');logs=load(RAW/'log_groups.json')
inventory=[o for b in buckets for o in b['object_inventory']]
stock=sum(b['bytes']+b['noncurrent_version_bytes'] for b in buckets)
source_runs=['jr_cdfd2933220ec75b45c04518c6e5afe93eb98915f3b88328f8bde8d7183d3700',
             'jr_9c58c50a3e452ab6fff7f8560524b5235aa0d0c92036feccc6768ce2499b0288',
             'jr_1ae29e48e526f050de2d2d637a5474387a37398466e7af56ed0584132f58e7fe']
cycle_objects=[o for o in inventory if any(r in o['Key'] for r in source_runs)
               and (o['Key'].startswith(('bronze/','silver/','gold/','auditoria/')))]
cycle_bytes=sum(o['Size'] for o in cycle_objects)
reports=load(RAW/'lambda_billing_reports_fiap-inep-streaming-consumer.json')
unique={x['request_id']:x for x in reports};assert len(unique)==len(reports)==39
lambda_gbs=sum(D(str(x['billed_duration_ms']))/1000*D(x['memory_mb'])/1024 for x in reports)
assert lambda_gbs==D('2.41575')
hours=load(CTRL/'finops_glue_metric_hours.json');assert hours['sum_active_hours']==307
metric_hours=D(hours['sum_active_hours']+7)
metrics=load(RAW/'cloudwatch_daily_metrics.json')
incoming=sum(D(str(p['Sum'])) for m in metrics if m['namespace']=='AWS/Logs' and m['metric_name']=='IncomingBytes' for p in m['datapoints'])
stored=sum(g['storedBytes'] for g in logs)
CE_SOURCE='aws/cost_explorer_usage_types.json; filtro RECORD_TYPE=Usage; quantidade e unidade da API'
rows=[]
def row(id,service,item,unit,q,rate_id,basis,source,limits='',**extra):
    pr=prices.get(rate_id,{})
    tariff=D(price[rate_id]) if rate_id else None
    record={'id':id,'service':service,'item':item,'unit':unit,'quantity':float(q) if q is not None else None,
            'tariff_usd':float(tariff) if tariff is not None else None,'basis':basis,
            'period':scope['metric_start_utc']+' até '+scope['metric_end_exclusive_utc']+' (métricas); billing MTD conforme fonte',
            'source':source,'tariff_source':'; '.join(pr.get('source_urls',[])),
            'limits':limits,'short_note':limits,'rate_id':rate_id,**extra}
    rows.append(record)

for layer,seconds in [('Bronze',293),('Silver',517),('Gold',306)]:
    row('glue_'+layer.lower(),'AWS Glue',layer+' batch','DPU-hora',D(seconds)/3600,'glue_etl_dpu_hour','per_cycle',
        'aws/glue_runs.json; controls/latest_gold_manifest.json; referência '+source_runs[['Bronze','Silver','Gold'].index(layer)],
        'DPUSeconds / 3.600. Gold recente com fixture desativada. Mesmos volumes e workers como hipótese.')
row('s3_storage','Amazon S3','Armazenamento Standard','GB-mês',D(stock)/GB,'s3_standard_storage_gb_month','storage_growth',
    'aws/s3_inventory.json; soma Size por objeto, incluindo assets e histórico',
    'Estoque atual + metade do crescimento de quatro snapshots distribuídos uniformemente no mês. Inclui auditorias dos runs de referência; sem apagar histórico.',
    growth_per_cycle=float(D(cycle_bytes)/GB),growth_source='Objetos bronze/silver/gold/auditoria dos três runs de referência, por Size')
for ident,tier,rateid in [('s3_put','Tier1','s3_standard_put_request'),('s3_get','Tier2','s3_standard_get_request')]:
    amount=sum(v['quantity'] for (s,t),v in usage.items() if s=='Amazon Simple Storage Service' and t.endswith('Requests-'+tier))
    row(ident,'Amazon S3','PUT/COPY/POST/LIST' if tier=='Tier1' else 'GET e outras Tier2','requests',amount,rateid,'observed_period',CE_SOURCE,
        'Requests de toda a conta. Extrapola o volume de apoio observado em cerca de sete dias; não é medição isolada por ciclo.',observed_days=7)
row('logs_ingestion','CloudWatch Logs','Ingestão Standard','GB',incoming/GB,'cloudwatch_standard_log_ingestion_gb','observed_period',
    'aws/cloudwatch_daily_metrics.json; AWS/Logs IncomingBytes; sete dias UTC completos',
    'Mantém o ritmo observado de logs de jobs, sessões e streaming; não altera retenção nem repete sessões de prévia.',observed_days=7)
row('logs_archive','CloudWatch Logs','Arquivo de logs atual','GB-mês',D(stored)/GB,'cloudwatch_log_archive_gb_month','monthly',
    'aws/log_groups.json; storedBytes de sete grupos',
    'Custo do estoque atual mantido. Crescimento futuro do arquivo depende de compressão/retenção e fica pendente em linha separada.')
row('logs_archive_growth','CloudWatch Logs','Crescimento futuro do arquivo','GB-mês',None,'cloudwatch_log_archive_gb_month','pending',
    'IncomingBytes é não comprimido e storedBytes é comprimido; não substituir um pelo outro',
    'Falta medir o acréscimo de bytes arquivados por ciclo na mesma janela; não foi inventada taxa de compressão.')
row('custom_metrics','Amazon CloudWatch','Métricas customizadas','métrica-hora',metric_hours,'cloudwatch_custom_metric_month','per_cycle',
    'controls/finops_glue_metric_hours.json e controls/fiap_metric_active_hours.json',
    '307 horas Glue + 2 FIAP batch + 5 FIAP streaming por ensaio. Preço mensal dividido pelas 720h dos30dias; mesmas séries e amostras como hipótese.',
    tariff_usd=float(D(price['cloudwatch_custom_metric_month'])/720))
row('alarms','Amazon CloudWatch','Cinco alarmes Standard','métrica-alarme-mês',D(len(load(RAW/'cloudwatch_alarms.json'))),
    'cloudwatch_standard_alarm_metric_month','monthly','aws/cloudwatch_alarms.json',
    'Cada alarme avalia uma métrica; recursos mantidos durante o horizonte. Sem assinatura externa operacional comprovada.')
row('lambda_compute','AWS Lambda','Consumidor x86,256MB','GB-segundo',lambda_gbs,'lambda_x86_gb_second','per_cycle',
    'aws/lambda_billing_reports_fiap-inep-streaming-consumer.json; 39 IDs únicos',
    'BilledDuration9.663ms ×0,25GB. Um replay com30eventos novos+5duplicatas+1inválido por ciclo de validação.')
row('lambda_requests','AWS Lambda','Invocações do replay','requests',D(len(unique)),'lambda_request','per_cycle',
    '39 REPORTs e CE USE2-Request=39', 'Volume medido do ensaio, não fluxo ao vivo do Inep.')
sqs_qty=usage[('Amazon Simple Queue Service','USE2-Requests-Tier1')]['quantity']
row('sqs_requests','Amazon SQS','Requests Standard, incluindo polling','requests',sqs_qty,'sqs_standard_request','observed_period',CE_SOURCE,
    'Mesmo ritmo de requests da observação7dias; inclui polling sem produtor. CW confirma8.636,5empty/dia nos seis dias ociosos.',observed_days=7)
row('secrets','Secrets Manager','Um segredo mantido','segredo-mês',D(len(load(RAW/'secret_metadata.json'))),
    'secretsmanager_secret_month','monthly','aws/secret_metadata.json; somente metadados, sem ler valor secreto','Mantido no mês; não duplicado por cada camada.')
row('secret_api','Secrets Manager','Chamadas de API','requests',usage[('AWS Secrets Manager','USE2-AWSSecretsManagerAPIRequest')]['quantity'],
    'secretsmanager_api_request','observed_period',CE_SOURCE,'Ritmo observado em sete dias como hipótese; sem novas credenciais e sem leitura do segredo nesta auditoria.',observed_days=7)
row('catalog_storage','Glue Data Catalog','Armazenamento de metadados','objeto-mês',usage[('AWS Glue','USE2-Catalog-Storage')]['quantity'],
    'glue_catalog_object_month_paid','observed_period',CE_SOURCE,
    'Equivalente ao preço pago acima da franquia. Na faixa atual, a cobrança é zero; 22 tabelas não são todos os objetos faturáveis.',observed_days=7)
row('catalog_requests','Glue Data Catalog','Requests de catálogo','requests',usage[('AWS Glue','USE2-Catalog-Request')]['quantity'],
    'glue_catalog_request_paid','observed_period',CE_SOURCE,
    'Equivalente ao preço pago acima da franquia de1milhão. Na faixa atual, a cobrança é zero.',observed_days=7)
row('s3_inbound','Amazon S3','Entrada comum de dados','GB',usage[('Amazon Simple Storage Service','USE2-DataTransfer-In-Bytes')]['quantity'],
    's3_transfer_in_internet_gb','observed_period',CE_SOURCE,
    'Ingresso comum S3US$0; não inclui egressGoogle nem roteamentoMRAP.',observed_days=7)
for id,item,unit,rate in [('gcp_read','BigQuery Storage Read API','TiB','bigquery_storage_read_tib'),
                          ('gcp_egress','Saída GCP para AWS','GiB','bigquery_storage_read_egress_gib_to_aws_reference')]:
    row(id,'Google Cloud',item,unit,None,rate,'pending','Conector sourceType=table; parentProject fiap1-512214',
        'Região e bytes cobrados ainda não confirmados; não deduzir do ParquetS3 e não lançar custo zero.')

measurements=[
 {'id':'s3_stock','service':'Amazon S3','metric':'Estoque de dois buckets','value':stock,'unit':'bytes','source':'aws/s3_inventory.json','period':scope['observed_at_utc'],'limits':'348objetos; sem versões antigas nem multipart; STANDARD.'},
 {'id':'snapshot_increment','service':'Amazon S3','metric':'Dados e auditorias por ciclo completo','value':cycle_bytes,'unit':'bytes/ciclo','source':'aws/s3_inventory.json; três runs aprovados','period':'20,22,27/10/2026','limits':'Soma dos prefixos com run_id das referências. Assets fixos já no estoque; falhas futuras não são presumidas.'},
 {'id':'lambda_billed','service':'AWS Lambda','metric':'Duração faturada','value':9663,'unit':'ms','source':'39 REPORTs únicos','period':'20->27/10UTC','limits':'Diferente de Duration sem arredondamento faturável.'},
 {'id':'sqs_idle','service':'Amazon SQS','metric':'Polling vazio por dia ocioso','value':8636.5,'unit':'empty receives/dia','source':'AWS/SQS NumberOfEmptyReceives','period':'21-26/10UTC,6dias completos','limits':'Não é toda a quantidade de requests faturáveis.'},
 {'id':'preview_compute','service':'AWS Glue','metric':'Sessões interativas já registradas','value':float(usage[('AWS Glue','USE2-GlueInteractiveSession-DPU-Hour')]['quantity']),'unit':'DPU-hora','source':CE_SOURCE,'period':'MTD','limits':'Seis sessões. Não previstas no cenário; não abrirDataPreviewpara reproduzir relatórios.'},
 {'id':'metric_hours','service':'Amazon CloudWatch','metric':'Métricas Glue com amostras nas referências','value':307,'unit':'métrica-hora/ciclo','source':'controls/finops_glue_metric_hours.json','period':'Janelas horárias dos3runs','limits':'255 consultas de séries; incluiJobRunId eALL; métricas FIAP tratadas separadamente.'},
]
model={'as_of':'2026-10-27','region':'us-east-2','budget_total_usd':140,
       'actual':{'gross_usage_usd':float(total),'credits_remaining_usd':136.59,'plan_days':172,
                 'period':'Billing MTD:01/10 a28/10exclusive; observação27/10/2026',
                 'source':CE_SOURCE,'credits_source':'Console Free Tier27/10; creditos_2026_10_27.jpg',
                 'scope':'Conta AWS inteira, não exclusivamente o projeto. Custos estimados sujeitos a atraso.'},
       'cadence':{'cycles_30d':4,'days':30,'description':'Batch por nova publicação. Quatro ciclos completos de validação e quatro replays em30dias são hipótese ajustável do projeto, não frequência imposta pelo enunciado ou agenda instalada.'},
       'projection_rows':rows,'measurements':measurements,
       'unresolved':['Região e bytes cobrados do BigQuery Storage Read/egress no projeto fiap1-512214.',
                     'Acréscimo comprimido de arquivo de logs por ciclo, sem confundir com IncomingBytes.',
                     'Validação do destinatário e implantação dos alertas financeiros dependem da autorização solicitada.'],
       'controls':load(CTRL/'controls_report.json'),
       'account_cost_by_service_usd':{k:float(v) for k,v in services.items()},
       'method_notes':['Preços AWS regionais validados por SKUOhio em27/10/2026.',
                       'Estimativa paga equivalente antes de créditos e benefícios gratuitos; DataCatalog na faixa atual tem cobrança zero.',
                       'Requests de apoio e logs usam o ritmo médio observado e não medição isolada por ciclo. Janela7dias é hipótese explicitada para billingMTD.',
                       'S3cresce com histórico; média do mês assume execuções distribuídas uniformemente.',
                       'Nenhuma sessão interativa nova faz parte do cenário; sessões históricas explicam consumo passado.',
                       'Sem rateio de custos por tag: CostExplorer é da conta inteira.',
                       'Custo completo não será apresentado enquanto existirem parcelas pendentes.']}

def project(r,cycles=4):
    if r['quantity'] is None or r['tariff_usd'] is None or r['basis']=='pending':return None
    q=D(str(r['quantity']));rate=D(str(r['tariff_usd']))
    if r['basis']=='per_cycle':q*=cycles
    elif r['basis']=='per_day':q*=30
    elif r['basis']=='observed_period':q*=D(30)/D(str(r['observed_days']))
    elif r['basis']=='storage_growth':q+=D(str(r['growth_per_cycle']))*cycles/2
    return q*rate
model['calculated_reference']={'cycles_30d':4,'available_subtotal_usd':str(sum((project(r) for r in rows if project(r) is not None),D(0))),
                               'complete_total_usd':None,'rows':{r['id']:str(project(r)) if project(r) is not None else None for r in rows}}
(OUT/'modelo_finops.json').write_text(json.dumps(model,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'rows':len(rows),'bytes_current':stock,'bytes_per_cycle':cycle_bytes,'lambda_gbseconds':str(lambda_gbs),'subtotal':model['calculated_reference']['available_subtotal_usd'],'pending':len([r for r in rows if r['quantity'] is None])},ensure_ascii=False))
