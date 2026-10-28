"""Build the reviewed four-page FinOps PDF from a consolidated report input.

Authoring marker ran once before this builder was first created. Repairs/reruns
of this operation must not run that marker again. This script makes no AWS calls.
"""
from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, PageBreak,
)
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'outputs/finops_2026_10_27'
OUTPUT = BASE / 'FinOps_FIAP.pdf'
PAGE = landscape(A4)
MARGIN = 38
WIDTH = PAGE[0] - MARGIN * 2
NAVY, TEAL, INK, MUTED, PALE, LINE = map(colors.HexColor,
    ['#17324D','#087E8B','#253748','#5B6D7D','#EDF4F7','#CCDCE5'])

for name, filename in [('Arial','arial.ttf'),('Arial-Bold','arialbd.ttf'),('Consolas','consola.ttf')]:
    pdfmetrics.registerFont(TTFont(name, str(Path('C:/Windows/Fonts') / filename)))

STYLES = {
    'title': ParagraphStyle('title',fontName='Arial-Bold',fontSize=21,leading=25,textColor=NAVY,spaceAfter=10),
    'eyebrow': ParagraphStyle('eyebrow',fontName='Arial-Bold',fontSize=8.5,leading=12,textColor=TEAL,spaceAfter=6),
    'h': ParagraphStyle('h',fontName='Arial-Bold',fontSize=12,leading=16,textColor=TEAL,spaceBefore=8,spaceAfter=5),
    'body': ParagraphStyle('body',fontName='Arial',fontSize=10.2,leading=14,textColor=INK,spaceAfter=7),
    'small': ParagraphStyle('small',fontName='Arial',fontSize=8.3,leading=11,textColor=MUTED,spaceAfter=5),
    'cell': ParagraphStyle('cell',fontName='Arial',fontSize=8.8,leading=11.4,textColor=INK),
    'head': ParagraphStyle('head',fontName='Arial-Bold',fontSize=8.7,leading=11.5,textColor=colors.white),
    'card_label': ParagraphStyle('card_label',fontName='Arial',fontSize=9,leading=12,textColor=MUTED,spaceAfter=7),
    'card_value': ParagraphStyle('card_value',fontName='Arial-Bold',fontSize=21,leading=25,textColor=NAVY),
}


def decimal(value):
    return Decimal(str(value))


def number(value, digits=2):
    s=f'{decimal(value):,.{digits}f}'
    return s.replace(',','_').replace('.',',').replace('_','.')


def usd(value, digits=2):
    return 'US$ '+number(value,digits)


def para(text, style='body'):
    return Paragraph(str(text), STYLES[style])


class Report:
    def __init__(self):
        self.story=[]

    def text(self,text,style='body'):
        self.story.append(para(text,style))

    def page(self,title,section):
        if self.story:self.story.append(PageBreak())
        self.text('FIAP TECH CHALLENGE - FASE 2 | FINOPS | 27/10/2026','eyebrow')
        self.text(title,'title')
        self.text(section,'small')

    def heading(self,text):self.text(text,'h')

    def table(self,headers,rows,widths=None):
        data=[[para(escape(str(x)),'head') for x in headers]]
        data += [[para(escape(str(x)),'cell') for x in row] for row in rows]
        t=Table(data,colWidths=widths,hAlign='LEFT',repeatRows=1)
        t.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),NAVY),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,PALE]),
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),
            ('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),
            ('LINEBELOW',(0,-1),(-1,-1),.5,LINE)
        ]))
        self.story.extend([t,Spacer(1,7)])

    def cards(self,items):
        data=[]
        for label,value,note in items:
            data.append([para(label,'card_label'),para(value,'card_value'),para(note,'small')])
        t=Table([[cell for cell in data]],colWidths=[WIDTH/len(data)]*len(data))
        t.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,-1),PALE),('VALIGN',(0,0),(-1,-1),'TOP'),
            ('LEFTPADDING',(0,0),(-1,-1),12),('RIGHTPADDING',(0,0),(-1,-1),12),
            ('TOPPADDING',(0,0),(-1,-1),11),('BOTTOMPADDING',(0,0),(-1,-1),10),
            ('LINEAFTER',(0,0),(-2,-1),5,colors.white)
        ]))
        self.story.extend([t,Spacer(1,8)])


def adapt(raw):
    """Independently calculate the consolidated model, then prepare readable rows."""
    if 'projection_rows' not in raw:
        raise ValueError('Expected the consolidated modelo_finops.json schema')
    cycles=decimal(raw['cadence']['cycles_30d'])
    days=decimal(raw['cadence']['days'])
    gcp=raw.get('gcp_evidence')
    if gcp:
        assert gcp['summary']['dataset']['location']=='US'
        assert gcp['summary']['billing']['billingEnabled'] is False
        assert gcp['summary']['table_queries_started']==0 and gcp['summary']['read_sessions_started']==0
    results={}
    display=[]
    for row in raw['projection_rows']:
        basis=row['basis']
        quantity=None if row['quantity'] is None else decimal(row['quantity'])
        if basis=='pending':
            projected=None
        elif basis=='per_cycle':
            projected=quantity*cycles
        elif basis=='observed_period':
            projected=quantity*days/decimal(row['observed_days'])
        elif basis=='storage_growth':
            projected=quantity+decimal(row['growth_per_cycle'])*cycles/2
        elif basis=='monthly':
            projected=quantity
        else:raise ValueError('Unexpected basis: '+basis)
        cost=None if projected is None else projected*decimal(row['tariff_usd'])
        reference=raw['calculated_reference']['rows'][row['id']]
        assert (cost is None)==(reference is None),row['id']
        if cost is not None:
            assert abs(cost-decimal(reference))<Decimal('0.00000001'),row['id']
        results[row['id']]=cost
        if quantity is None:
            measured=('Pendente: compressão e retenção' if row['id']=='logs_archive_growth' else
                      ('Fonte US confirmada; bytes indisponíveis' if gcp else 'Pendente: acesso/localização em conferência'))
        else:
            digits=0 if row['unit']=='requests' else (0 if row['id'] in ['custom_metrics','alarms','secrets'] else 6)
            qty=number(quantity,digits)
            if basis=='per_cycle':measured=qty+' por ciclo/ensaio'
            elif basis=='observed_period':
                measured=qty+(' em 7 dias UTC' if row['id']=='logs_ingestion' else ' no billing MTD; base de apoio: 7 dias')
            elif basis=='storage_growth':measured=qty+' GB atuais + '+number(row['growth_per_cycle'],6)+' GB por ciclo'
            else:measured=qty+' mantidos no mês'
        rate=decimal(row['tariff_usd'])
        if row['id']=='custom_metrics':rate_display='US$ 0,30/métrica-mês / 720h'
        elif row['unit']=='requests':
            if rate==0:rate_display='US$ 0'
            else:rate_display=usd(rate*Decimal('1000000'),4)+' / milhão'
        else:
            digits=10 if rate<Decimal('.001') else (6 if rate<Decimal('.01') else 3)
            rate_display=usd(rate,digits)+' / '+row['unit']
        labels={
            'glue_bronze':'Glue - Bronze batch','glue_silver':'Glue - Silver batch','glue_gold':'Glue - Gold batch',
            's3_storage':'S3 - Standard','s3_put':'S3 - PUT/COPY/POST/LIST','s3_get':'S3 - GET / Tier2',
            'logs_ingestion':'Logs - ingestão Standard','logs_archive':'Logs - arquivo atual',
            'logs_archive_growth':'Logs - crescimento de arquivo','custom_metrics':'CloudWatch - métricas',
            'alarms':'CloudWatch - alarmes Standard','lambda_compute':'Lambda - duração x86, 256MB',
            'lambda_requests':'Lambda - invocações do replay','sqs_requests':'SQS - requests com polling',
            'secrets':'Secrets Manager - segredo','secret_api':'Secrets Manager - API',
            'catalog_storage':'Glue Catalog - armazenamento','catalog_requests':'Glue Catalog - requests',
            's3_inbound':'S3 - entrada comum','gcp_read':'BigQuery - Storage Read API','gcp_egress':'GCP - saída para AWS'
        }
        display.append({'id':row['id'],'service':labels[row['id']],
            'unit':row['unit'],'quantity_measured':measured,'unit_price_display':rate_display,
            'monthly_display':'Pendente' if cost is None else usd(cost,6)})
    subtotal=sum((x for x in results.values() if x is not None),Decimal('0'))
    assert abs(subtotal-decimal(raw['calculated_reference']['available_subtotal_usd']))<Decimal('0.00000001')
    assert sum(x is None for x in results.values())==3
    controls=raw['controls']
    assert controls['applied'] is True and controls['starts_job'] is False
    assert controls['before']['NumberOfWorkers']==10 and controls['before']['Timeout']==480
    assert controls['after']['NumberOfWorkers']==2 and controls['after']['Timeout']==30
    budget=raw.get('budget_applied')
    if budget:
        assert budget['operation']=='created'
        assert budget['budget_limit_usd']==30 and budget['time_unit']=='MONTHLY'
        assert budget['total_project_ceiling_usd']==140
        assert budget['credits_included'] is False and budget['refunds_included'] is False
        assert budget['notifications_usd']==[15,24,30]
        assert budget['same_recipient_as_source_budget'] is True
        assert budget['automatic_resource_actions']==[]
        verification=budget.get('recipient_verification_status','')
        if 'Inactive' in verification and 'denied' in verification.lower():
            verification_text=('Console: 0/1 destinatário verificado, status Inactive. A tentativa de enviar a verificação '
                               'foi negada por IAM; nenhum envio foi confirmado. Um administrador precisa concluir essa etapa.')
        else:
            verification_text='A verificação do destinatário ainda depende de conferência no console; a API não comprova essa etapa.'
        budget_status=('Criado e conferido: FIAP-Acompanhamento-Bruto-30USD, mensal de US$ 30, '
                       'sem créditos/reembolsos, com avisos de US$ 15 / US$ 24 / US$ 30 no e-mail já cadastrado e autorizado. '
                       +verification_text)
        budget_scope=('US$ 30 é reserva de monitoramento, não estimativa de custo nem novo limite autorizado. '
                      'US$ 140 continua sendo o teto TOTAL do projeto. Não foram criadas ações automáticas para parar recursos; '
                      'budget e alertas não são travas de gasto. O novo orçamento acompanha valores brutos sem abater créditos.')
    else:
        budget_status=('Proposta preparada, ainda não implantada: orçamento mensal de acompanhamento bruto de US$ 30, '
                       'excluindo créditos, com limiares de US$ 15 / US$ 24 / US$ 30. '
                       'Uso de e-mail e verificação do destinatário aguardam autorização.')
        budget_scope=('US$ 30 é reserva de monitoramento, não estimativa de custo nem novo limite autorizado. '
                      'US$ 140 continua sendo o teto TOTAL do projeto. Budget e alertas não são travas automáticas. '
                      'A configuração existente inclui créditos e não acompanha o consumo bruto de modo equivalente.')
    return {
        'current':{
            'as_of':raw['as_of'],'user_total_ceiling_usd':raw['budget_total_usd'],
            'usage_mtd_usd':raw['actual']['gross_usage_usd'],
            'credits_remaining_usd':raw['actual']['credits_remaining_usd'],
            'plan_days_remaining':raw['actual']['plan_days'],
            'observation_windows':'Janelas: billing MTD de 01/10 até 28/10 exclusivo; CloudWatch/REPORTs de 20/10 até 27/10 exclusivo (a referência Gold é um run separado de 27/10, 00:41 UTC). Inventário S3 e créditos são fotografias da consulta de 27/10.'
        },
        'scenario':{
            'description':'Batch sob demanda, por nova publicação. O cenário principal adota quatro ciclos completos de validação e quatro replays em 30 dias como hipótese ajustável. O enunciado não fixa essa cadência e nenhuma agenda foi criada.',
            'short_label':'4 CICLOS E 4 REPLAYS EM 30 DIAS',
            'comparisons':[
                {'name':'Cenário principal de validação','cycles':4,'monthly_aws_display':usd(subtotal,4)+' - subtotal disponível','gcp_display':'Fonte US confirmada; bytes pendentes' if gcp else 'Acesso/localização em conferência'},
            ],
            'total_scope_note':'A referência é bruta, a tarifas pagas, antes de créditos e franquias. Três parcelas não foram somadas: crescimento do arquivo de logs, leitura BigQuery e saída GCP. Não é o custo total da arquitetura.',
            'main_total_display':'Subtotal disponível de referência: '+usd(subtotal,6)+' por 30 dias. Custo completo: PENDENTE.'
        },
        'rows':display,
        'table_split_after':10,
        'table_notes_first':[
            'Glue: DPUSeconds / 3.600 x 4 ciclos x US$ 0,44. Bases Bronze 293, Silver 517 e Gold 306 DPU-segundos. Gold: run de 27/10, 00:41 UTC, com fixture desativada, fora da janela CloudWatch.',
            'S3: GB = bytes / 2^30. Estoque médio = atual + crescimento por ciclo x 4 / 2, assumindo execuções distribuídas uniformemente. Histórico preservado.',
            'Logs: IncomingBytes mede ingresso não comprimido; storedBytes mede arquivo comprimido. O crescimento de arquivo é pendente, sem taxa de compressão inventada.',
            'Ingestão de logs: mantém o ritmo histórico de apoio de sete dias, inclusive prévias já feitas. O cenário não abre novas sessões; essa hipótese não é um fator de logs isolado por novo ciclo.'
        ],
        'table_notes_second':[
            'Observado em período: quantidade x 30 / 7; essa base de sete dias é hipótese de apoio para quantidades vindas do billing MTD. Não é uma medição isolada por ciclo.',
            'Métricas customizadas: 314 métricas-hora por ciclo/ensaio x 4 x US$ 0,30 / 720h. São horas com dados publicados; não se cobram todas as séries por um mês inteiro.',
            'Lambda: 9.663ms faturados x 0,25GB / 1.000 = 2,41575 GB-s por replay, com 39 invocações. SQS inclui polling vazio.',
            'Catalog: parcelas lineares são equivalentes antes da franquia; nas quantidades atuais elegíveis, a cobrança efetiva é zero. Franquias e créditos não são descontados deste subtotal de referência.'
        ],
        'controls':{
            'summary':'O job antigo fiap-ingestao-bigquery-uf foi reduzido de 10 para 2 workers G.1X e de 480 para 30 minutos de timeout, com configuração anterior preservada. Esse ajuste não iniciou execução, não mudou IAM e não apagou dados.',
            'rows':[
                {'name':'Job antigo: capacidade e timeout','state':'Aplicado: 10 G.1X / 480min para 2 G.1X / 30min. Backup preservado.','meaning':'Exposição teórica até o timeout: US$ 35,20 para US$ 0,44 por execução. Redução de risco, não economia já realizada.'},
                {'name':'Execução e recorrência','state':'Batch sob demanda; quatro ciclos e replays são hipótese de 30 dias.','meaning':'Não criar agenda contínua nem abrir sessões de prévia para apenas consultar relatórios.'},
                {'name':'Preservação e acesso','state':'Nenhum dado apagado e nenhuma permissão IAM ampliada pelo ajuste.','meaning':'Manter históricos/auditorias. Reduzir timeout não constitui um bloqueio de gasto da conta.'}
            ]
        },
        'budget':{
            'status_text':budget_status,
            'scope_note':budget_scope
        },
        'limitations':[
            ('GCP: fonte US confirmada por metadados; billingEnabled=false. A auditoria de 20 a 27/10 não retornou registros e o Monitoring negou a consulta de bytes por faturamento desabilitado. Nesta conferência, não se ativou billing nem se iniciou leitura de dados.' if gcp else 'GCP: acesso e localização do projeto estão em conferência. Região e bytes cobrados de leitura/egress ainda não estão disponíveis.'),
            ('O dashboard de 30 dias mostrou 24 CreateReadSession e 16 ReadRows. São contagens de requests, não bytes lidos ou egress faturável; registros vazios e erro 403 não comprovam custo zero. As duas parcelas GCP continuam pendentes.' if gcp else 'As parcelas GCP não podem ser substituídas por custo zero nem pelos bytes Parquet do S3.'),
            'Logs: falta medir o acréscimo comprimido arquivado por ciclo. Até fechar essas parcelas, não garantir custo total nem aderência multicloud integral ao teto de US$ 140.',
            'O subtotal não inclui novos serviços, modelos de IA, impostos nem falhas futuras presumidas. Mantém volume, séries e duração das referências; o consumo futuro pode variar.'
        ],
        'sources':[
            'Medições e controles: aws/ (runs, inventário S3, métricas, REPORTs e billing); controls/controls_report.json; créditos no console de 27/10. Métodos e referências completos em modelo_finops.json.',
            'GCP: gcp/summary.json, source_dataset_metadata.json, project_billing.json, read_audit_logs.json e errors.json. Orçamento: budget/budget_applied_redacted.json, budget_after.json e verification_console.json, sem endereço nos entregáveis.',
            'Preços: aws/regional_price_list.json e tarifas_oficiais.json, com SKUs, datas e URLs oficiais de cada serviço. https://aws.amazon.com/glue/pricing/ ; https://aws.amazon.com/cloudwatch/pricing/ ; https://cloud.google.com/bigquery/pricing'
        ]
    }


def build(model):
    r=Report()
    current=model['current']
    scenario=model['scenario']
    rows=model['rows']
    assert current['as_of']=='2026-10-27'
    assert decimal(current['user_total_ceiling_usd'])==Decimal('140')

    r.page('FinOps do projeto: consumo, projeção e controles','1 DE 4 - POSIÇÃO ATUAL E CENÁRIO DE PLANEJAMENTO')
    r.cards([
        ('Teto total autorizado',usd(current['user_total_ceiling_usd']),'Não é um limite mensal e não se renova.'),
        ('Uso AWS acumulado',usd(current['usage_mtd_usd']),'Cost Explorer Usage, créditos excluídos; estimado.'),
        ('Créditos exibidos',usd(current['credits_remaining_usd']),str(current['plan_days_remaining'])+' dias restantes no plano gratuito.')
    ])
    r.text('As métricas operacionais, o consumo faturado e o saldo de créditos têm escopos diferentes. O consumo observado é da conta AWS; não foi atribuído integralmente a uma tag do projeto. O planejamento adota US$ 140 como teto total.')
    r.heading('Cadência usada na projeção')
    r.text(escape(scenario['description']))
    r.table(['Cenário de 30 dias','Ciclos batch','Subtotal AWS projetado','Google Cloud'],[
        [x['name'],x['cycles'],x['monthly_aws_display'],x.get('gcp_display','Pendente de medição')]
        for x in scenario['comparisons']
    ],[WIDTH*.32,WIDTH*.12,WIDTH*.26,WIDTH*.30])
    r.text(escape(scenario['total_scope_note']),'small')
    r.heading('O que foi feito nesta etapa')
    r.text(escape(model['controls']['summary']))
    r.text(escape(current['observation_windows']),'small')

    headers=['Serviço / componente','Unidade','Quantidade medida / base','Preço unitário','Custo mensal projetado']
    col_widths=[WIDTH*.19,WIDTH*.15,WIDTH*.31,WIDTH*.18,WIDTH*.17]
    # The consolidated input chooses a logical split to preserve legibility.
    split=model.get('table_split_after',9)
    for part,subset in enumerate([rows[:split],rows[split:]],2):
        r.page('Custos por serviço - '+('computação e dados' if part==2 else 'streaming e observabilidade'),
               f'{part} DE 4 - CENÁRIO PRINCIPAL: '+escape(scenario['short_label']))
        r.text('Valores brutos de referência antes de créditos. Quantidades observadas e hipóteses de repetição são identificadas; itens sem medição permanecem pendentes.','small')
        r.table(headers,[[x['service'],x['unit'],x['quantity_measured'],x['unit_price_display'],x['monthly_display']] for x in subset],col_widths)
        if part==2:
            r.heading('Como ler a quantidade e o custo mensal')
            for note in model['table_notes_first']:
                r.text(escape(note),'small')
        else:
            r.text(escape(scenario['main_total_display']),'body')
            for note in model['table_notes_second']:
                r.text(escape(note),'small')

    r.page('Controles, limites e evidências','4 DE 4 - CONTROLES PARA O TETO TOTAL DE US$ 140')
    r.table(['Controle','Situação comprovada','Efeito / limite'],[
        [x['name'],x['state'],x['meaning']] for x in model['controls']['rows']
    ],[WIDTH*.22,WIDTH*.39,WIDTH*.39])
    r.heading('Orçamento e alertas financeiros')
    r.text(escape(model['budget']['status_text']))
    r.text(escape(model['budget']['scope_note']),'small')
    r.heading('Limitações que permanecem')
    for note in model['limitations']:
        r.text(escape(note),'small')
    r.heading('Fontes e rastreabilidade')
    for source in model['sources']:
        r.text(escape(source),'small')

    def footer(canvas,doc):
        canvas.saveState()
        canvas.setStrokeColor(LINE)
        canvas.line(MARGIN,33,PAGE[0]-MARGIN,33)
        canvas.setFont('Arial',7.6)
        canvas.setFillColor(MUTED)
        canvas.drawString(MARGIN,21,'FIAP Fase 2 | FinOps | Consulta e premissas de 27/10/2026 | USD')
        canvas.drawRightString(PAGE[0]-MARGIN,21,str(doc.page))
        canvas.restoreState()

    OUTPUT.parent.mkdir(parents=True,exist_ok=True)
    doc=SimpleDocTemplate(str(OUTPUT),pagesize=PAGE,leftMargin=MARGIN,rightMargin=MARGIN,
        topMargin=30,bottomMargin=42,title='FinOps - FIAP Tech Challenge Fase 2',author='Equipe Try Catch Finally')
    doc.build(r.story,onFirstPage=footer,onLaterPages=footer)
    reader=PdfReader(OUTPUT)
    assert len(reader.pages)==4,('Unexpected pagination',len(reader.pages))
    assert all((page.extract_text() or '').strip() for page in reader.pages)
    digest=hashlib.sha256(OUTPUT.read_bytes()).hexdigest()
    OUTPUT.with_suffix('.sha256').write_text(digest+'  '+OUTPUT.name+'\n',encoding='utf-8')
    return {'pdf':str(OUTPUT),'pages':len(reader.pages),'bytes':OUTPUT.stat().st_size,
            'sha256':digest}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--final-budget-status-confirmed',action='store_true',
        help='Only generate after the task owner confirms the consolidated model and budget status.')
    args=parser.parse_args()
    if not args.final_budget_status_confirmed:
        parser.error('Await the consolidated model and current budget state before rendering the final report.')
    model=adapt(json.loads(args.input.read_text(encoding='utf-8')))
    print(json.dumps(build(model),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
