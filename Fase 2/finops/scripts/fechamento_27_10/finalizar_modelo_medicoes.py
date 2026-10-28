"""Create a new partial forecast from measured table metadata; no cloud calls."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
ROOT = BASE.parents[1]
read = lambda p: json.loads(p.read_text(encoding='utf-8-sig'))
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
previous_path = ROOT/'outputs/finops_atualizado_20261027/modelo_finops.json'
previous = read(previous_path)
gcp = read(BASE/'fechamento_gcp.json')
raw_gcp = read(BASE/'raw/gcp_existing_metrics.json')
logs = read(BASE/'fechamento_logs.json')
assert gcp['status'] == 'STORAGE_READ_PROSPECTIVE_LINE_CLOSABLE'
assert gcp['reconciliation']['all_seven_compatible'] is True
assert gcp['reconciliation']['logical_bytes'] == 271547681
assert gcp['reconciliation']['total_source_rows'] == 3902954
assert gcp['inputs']['gcp_sha256'] == sha(BASE/'raw/gcp_existing_metrics.json')
assert raw_gcp['billing_metadata']['billingEnabled'] is False

def rebase(value):
    if isinstance(value,dict):return {k:rebase(v) for k,v in value.items()}
    if isinstance(value,list):return [rebase(v) for v in value]
    if isinstance(value,str):
        return value.replace('raw/','../finops_atualizado_20261027/raw/').replace('creditos_atuais.png','../finops_atualizado_20261027/creditos_atuais.png')
    return value

model = rebase(deepcopy(previous))
model['as_of'] = '2026-10-27'
model['projection_status'] = 'PARTIAL_TWO_UNMEASURED_COST_COMPONENTS'
model['candidate_status'] = 'APPROVED_PARTIAL_FORECAST'
model['reviewed_at_utc'] = datetime.now(timezone.utc).isoformat()
model['final_document_ready'] = True
model['predecessor'] = {'model':'../finops_atualizado_20261027/modelo_finops.json',
                        'sha256':sha(previous_path),'preserved_unchanged':True}
quantity = Decimal(271547681)/Decimal(2**40)
row = next(r for r in model['projection_rows'] if r['id'] == 'gcp_read')
row.update(item='Leitura integral prospectiva das sete tabelas',unit='TiB',
           quantity=float(quantity),quantity_exact_decimal=str(quantity),
           measured_source_logical_bytes=271547681,tariff_usd=1.10,
           basis='per_cycle',cycle_driver='batch',
           source='raw/gcp_existing_metrics.json; fechamento_gcp.json; ../manifesto_bronze.json; ../../outputs/bronze_glue.py',
           period=raw_gcp['observed_at_utc']+'; metadados atuais conciliados com a referência Bronze',
           limits='Projeção prospectiva: uma leitura integral por batch das mesmas sete tabelas, todas as colunas e linhas. Não é histórico faturado nem garantia de teto para retries, leituras adicionais ou previews. Atualização da fonte exige nova medição de metadados. Franquia de 300 TiB por conta não aplicada.',
           short_note='Metadados medidos; uma leitura integral por batch; antes de franquias.',
           measurement_type='measured_logical_capacity_for_prospective_full_read',
           historical_billed_bytes=None)
row.pop('projected_quantity_30d',None)
egress = next(r for r in model['projection_rows'] if r['id']=='gcp_egress')
egress.update(source='raw/gcp_existing_metrics.json; fechamento_gcp.json',
              period='20/10/2026 18:20–18:30 UTC; coleta '+raw_gcp['observed_at_utc'],
              limits='ReadRows response_sizes observou 1.860.997 bytes na janela do projeto, que contém batch e previews. Não foi estabelecida equivalência entre a distribuição de respostas e todos os blocos de streaming, rota ou SKU de egress. Auditoria do consumidor não retornou registros com bytes. Tamanho lógico e Parquet não substituem rede.',
              short_note='Telemetria de respostas medida; quantidade faturável de egress indisponível.')
stock = next(r for r in model['projection_rows'] if r['id']=='logs_archive')
stock.update(source='raw/aws_logs/log_growth_readonly.json; fechamento_logs.json',
             period='2026-10-27T18:02:12.430737Z',
             limits='7.417.226 bytes reportados; hipótese de manter o estoque por 30 dias normalizados. Não inclui crescimento futuro não medido. Retenção pode reduzir estoque. Não somar de novo histórico de arquivo CE.')
growth = next(r for r in model['projection_rows'] if r['id']=='logs_archive_growth')
growth.update(source='raw/aws_logs/log_growth_readonly.json; fechamento_logs.json; logs_T4.json',
              period='T3 17:04 UTC, T4 17:57 UTC e coleta detalhada 18:02 UTC de 27/10/2026',
              limits='6.216.753 bytes de IncomingBytes não comprimidos e 27.113 eventos observados hoje, com estoque reportado estável. Política STANDARD, nenhuma identidade diária StoredBytes. Janela mista e buckets sobrepostos; incremento comprimido por ciclo permanece indisponível.',
              short_note='Há ingestão medida; delta líquido de estoque zero não fecha crescimento.')
ref = model['calculated_reference']
ref['rows']['gcp_read'] = str(quantity*Decimal(4)*Decimal('1.10'))
ref['available_subtotal_usd'] = str(sum((Decimal(v) for v in ref['rows'].values() if v is not None),Decimal(0)))
ref['complete_total_usd'] = None
model['unresolved'] = [
    'Egress GCP: telemetria response_sizes medida, sem equivalência comprovada ao volume transferido faturável; custo permanece n.a.',
    'Crescimento comprimido do arquivo de logs por ciclo: ingresso foi medido, mas as capturas de estoque estável não permitem atribuição; custo permanece n.a.',
    'Requests de apoio: baseline de seis dias sem Bronze/replay; reconciliar cobertura da cadência pretendida antes de apresentar custo completo.',
    'E-mail Budget: último estado observado 0/1 verificado e Inactive; entrega não comprovada. Nenhum novo envio foi tentado.'
]
model['gcp_evidence']['latest_collection'] = {'source':'raw/gcp_existing_metrics.json',
    'sha256':sha(BASE/'raw/gcp_existing_metrics.json'),'analysis':'fechamento_gcp.json',
    'observed_at_utc':raw_gcp['observed_at_utc'],'monitoring_now_available':True,
    'billing_enabled_current':False,'cloud_calls':19,
    'storage_read_prospective_line_quantified':True,'historical_billed_read_bytes':None,
    'billed_egress_bytes':None,'prior_403_errors_are_historical':True,
    'credential_renewal_authorized_by_human':True}
model['gcp_evidence']['billed_bytes_still_unavailable'] = True
model['method_notes'][-1] = 'Storage Read agora tem projeção medida de uma leitura integral por batch. Egress e crescimento comprimido dos logs permanecem fora do subtotal; nenhum custo indisponível foi convertido em zero.'
model['method_notes'] += [
    '271.547.681 bytes lógicos medidos em sete tabelas TABLE/US; linhas e contagem de colunas conciliam a referência Bronze. O hash de schema atual é baseline e não prova igualdade de hashes históricos Spark/BigQuery.',
    'Preço de leitura pré-franquias. Não aplicar 300 TiB gratuitos sem confirmar elegibilidade/saldo da conta; a franquia de leitura não cobre egress.',
    '19 APIs GCP de metadados/históricos e seis APIs AWS de leitura foram executadas. Billing continua desativado; nenhum SQL, ReadRows novo, ReadSession nova, API ou IAM foi ativado.',
    'Consulta CloudWatch GetMetricData: 14 métricas, referência pontual US$0,00014 antes de franquias. Valor não afirmado como já faturado e não recorrente nesta projeção.',
    'Créditos e Cost Explorer não foram reconferidos nesta investigação. Não descontar o preço da consulta novamente do saldo observado.'
]
model['actual']['credits_observation_note'] += ' Esta investigação de pendências não consultou novamente créditos ou Cost Explorer.'
model['actual']['one_off_measurement_api_note'] += ' Nesta etapa adicional, GetMetricData solicitou 14 métricas: referência pontual US$0,00014; sem nova consulta CE.'
model['measurement_attempts'].update(gcp_existing_metadata={'source':'raw/gcp_existing_metrics.json',
    'status':'MEASURED_LOGICAL_CAPACITY_SUPPORTS_PROSPECTIVE_READ_ESTIMATE','new_extractions':False},
    logs_latest={'source':'raw/aws_logs/log_growth_readonly.json','status':'MEASURED_INGESTION_AND_STOCK_NOT_FUTURE_COMPRESSED_INCREMENT'})
for note in model['evidence_notes']:
    if note['service']=='Google Cloud':
        note.update(description='Coleta de 18:16 UTC: 19 chamadas de metadados e históricos, sem erros; Monitoring respondeu, billingEnabled permaneceu false.',
                    source='raw/gcp_existing_metrics.json; fechamento_gcp.json',
                    limits='Erros 403 da coleta de 12:15 UTC são históricos. A nova sessão foi autorizada pelo usuário; não houve ativação de faturamento. Auditoria do consumidor vazia não prova consumo zero nem cobre o dono da tabela.')
    elif note['service']=='CloudWatch Logs':
        note.update(description='Estoque de 7.417.226 bytes confirmado na coleta ampliada; 6.216.753 bytes não comprimidos e 27.113 eventos de log observados hoje.',
                    source='raw/aws_logs/log_growth_readonly.json; fechamento_logs.json',
                    limits='14 séries Complete e duas sem datapoints, preservadas como null. Política STANDARD, sem identidade diária StoredBytes. Estoque estável não fecha incremento comprimido por ciclo.')
    elif note['service']=='Créditos e orçamento':
        note['description']='Teto total US$140. Fotografia de créditos às 17:18 UTC: US$136,59; uso bruto CE anterior estimado US$3,4142741034.'
model['evidence_notes'] += [
    {'service':'Leitura prospectiva BigQuery','description':'271.547.681 bytes lógicos medidos; uma leitura integral por batch, quatro ciclos hipotéticos em 30 dias.',
     'source':'raw/gcp_existing_metrics.json; fechamento_gcp.json; ../manifesto_bronze.json',
     'limits':'Projeção bruta pré-franquias, não histórico faturado. Leituras extras e retries ficam fora do fator. Egress e crescimento comprimido dos logs seguem n.a.'},
    {'service':'Coleta GCP atual','description':'Monitoring respondeu na sessão autorizada; foram feitas 19 chamadas de leitura de metadados/históricos.',
     'source':'raw/gcp_existing_metrics.json; gcp_acesso_renovado.png; gcp_coleta_executada.png',
     'limits':'A recusa automática inicial de renovação foi resolvida com aprovação explícita do usuário. Não ativou billing, APIs ou IAM; nenhum SQL, ReadRows ou ReadSession novo foi iniciado.'},
    {'service':'Telemetria GCP','description':'ReadRows: 1.860.997 bytes de response_sizes e 13 requests na janela de dez minutos.',
     'source':'raw/gcp_existing_metrics.json; fechamento_gcp.json',
     'limits':'A janela do consumidor contém batch e previews. A distribuição de respostas não estabelece equivalência aos bytes de rede faturáveis; não usá-la como egress.'}
]
model['candidate_audit']['prior_revision_audit'] = deepcopy(previous.get('candidate_audit',{}))
model['candidate_audit']['old_available_subtotal_usd'] = previous['calculated_reference']['available_subtotal_usd']
model['candidate_audit']['delta_from_old_available_subtotal_usd'] = str(Decimal(ref['available_subtotal_usd'])-Decimal(previous['calculated_reference']['available_subtotal_usd']))
model['candidate_audit']['predecessor_model_sha256'] = sha(previous_path)
model['candidate_audit']['delta_interpretation'] = 'Incremento da linha prospectiva BigQuery Storage Read, com referência à revisão imediatamente anterior; auditoria mais antiga preservada separadamente.'
old_stock = next(m for m in model['measurements'] if m['id']=='logs_T3_stock')
old_stock.update(metric='Estoque reportado na coleta ampliada',source='raw/aws_logs/log_growth_readonly.json',
                 period='2026-10-27T18:02:12.430737Z',limits='7.417.226 bytes; estoque estável por grupo; não demonstra geração comprimida zero.')
model['measurements'] += [
    {'id':'gcp_logical_capacity','service':'BigQuery','metric':'Capacidade lógica medida das sete tabelas','value':271547681,'unit':'bytes','source':'raw/gcp_existing_metrics.json; fechamento_gcp.json','period':raw_gcp['observed_at_utc'],'limits':'Metadados conciliados com 3.902.954 linhas Bronze; base prospectiva de uma leitura integral, não histórico faturado.'},
    {'id':'gcp_readrows_response_bytes','service':'BigQuery Storage API','metric':'Soma de response_sizes de ReadRows','value':1860997,'unit':'bytes de telemetria','source':'raw/gcp_existing_metrics.json','period':'20/10/2026 18:20–18:30 UTC','limits':'13 requests. Distribuição da API não comprova quantidade de egress nem isolamento do run.'},
    {'id':'logs_ingestion_today','service':'CloudWatch Logs','metric':'IncomingBytes nos pontos reportados hoje','value':6216753,'unit':'bytes não comprimidos','source':'raw/aws_logs/log_growth_readonly.json; fechamento_logs.json','period':'27/10/2026 00:00–18:00 UTC exclusivo','limits':'14 séries Complete; logs-v2 sem datapoints preservado como null. Não substitui arquivo comprimido.'},
    {'id':'logs_events_today','service':'CloudWatch Logs','metric':'IncomingLogEvents nos pontos reportados hoje','value':27113,'unit':'eventos de log','source':'raw/aws_logs/log_growth_readonly.json; fechamento_logs.json','period':'27/10/2026 00:00–18:00 UTC exclusivo','limits':'Eventos de log não são invocações Lambda ou eventos de negócio.'}
]
assert len(model['projection_rows'])==21
assert sum(r['quantity'] is None for r in model['projection_rows'])==2
assert sorted(k for k,v in ref['rows'].items() if v is None)==['gcp_egress','logs_archive_growth']
assert len(model['measurements'])==12
target=BASE/'modelo_finops.json'
target.write_text(json.dumps(model,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
assert sha(previous_path)==model['predecessor']['sha256']
print(json.dumps({'status':'FINAL_PARTIAL_MODEL_READY','path':str(target),'sha256':sha(target),
    'numeric_rows':19,'pending_rows':2,'storage_read_usd_30d':ref['rows']['gcp_read'],
    'partial_subtotal_usd':ref['available_subtotal_usd'],'complete_total_usd':None},ensure_ascii=False))
