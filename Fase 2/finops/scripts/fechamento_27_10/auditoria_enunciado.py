"""Local-only audit of the assignment against the approved partial cost model."""
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, getcontext
import hashlib
import json
from pathlib import Path

from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[3]
PDF = Path('C:/Users/USER/Downloads/POSTECH - IAST - Tech Challenge - Fase 2 (2).pdf')
MODEL = ROOT / 'outputs/finops_atualizado_20261027/modelo_finops.json'
TEXT = ROOT / 'outputs/finops_atualizado_20261027/FinOps_FIAP.txt'
OUTPUT = ROOT / 'outputs/finops_pendencias_20261027/auditoria_enunciado.json'


def source(path):
    body = path.read_bytes()
    return {'path': str(path), 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}


def main():
    getcontext().prec = 50
    model = json.loads(MODEL.read_text(encoding='utf-8'))
    reader = PdfReader(PDF)
    pages = {str(number): reader.pages[number-1].extract_text() for number in (4, 8, 9)}
    rows = model['projection_rows']
    costs = model['calculated_reference']['rows']
    missing = [row['id'] for row in rows if costs[row['id']] is None]
    numeric_costs = [Decimal(value) for value in costs.values() if value is not None]
    total = sum(numeric_costs, Decimal(0))
    reference = Decimal(model['calculated_reference']['available_subtotal_usd'])
    result = {
        'audited_at_utc': datetime.now(timezone.utc).isoformat(),
        'scope': 'Somente arquivos locais; sem chamadas cloud/browser, consultas ou execução de pipeline. Nenhum XLSX, PDF ou modelo alterado.',
        'sources': {'assignment_pdf': source(PDF), 'model': source(MODEL), 'finops_text': source(TEXT)},
        'assignment_pdf_pages': [4, 8, 9], 'assignment_pdf_total_pages': len(reader.pages),
        'source_page_checks': {
            'page4_access_and_update': 'Forma de acesso' in pages['4'] and 'atualizada' in pages['4'],
            'page8_measured_estimate': 'números medidos' in pages['8'],
            'page9_production_projection': 'mesmo desenho em produção' in pages['9'],
        },
        'five_required_fields': [
            {'field': 'Serviço', 'assignment_page': 9, 'finding': 'PRESENTE', 'evidence': '21 parcelas com serviço identificado; tabelas do FinOps nas seções2/3.'},
            {'field': 'Unidade', 'assignment_page': 9, 'finding': 'PRESENTE', 'evidence': 'Unidades por parcela, incluindo DPU-hora, GB-mês, requests, GB-segundo, TiB/GiB.'},
            {'field': 'Quantidade medida', 'assignment_page': 9, 'finding': 'PARCIAL',
             'evidence': f'{len(rows)-len(missing)} quantidades numéricas; {len(missing)} sem quantidade. O documento distingue medição direta e proxy histórico.',
             'limitation': 'Uma contagem observada em6dias pode ser real e, ainda assim, não representar as solicitações marginais de4batches completos.'},
            {'field': 'Preço unitário', 'assignment_page': 9, 'finding': 'PRESENTE_COM_CONDIÇÕES',
             'evidence': 'Tarifas e fontes por parcela; valores antes de franquias/créditos. Tarifas GCP continuam condicionadas à aplicabilidade de região/rota/faixa.'},
            {'field': 'Custo mensal projetado', 'assignment_page': 9, 'finding': 'PARCIAL',
             'evidence': f'{len(numeric_costs)} parcelas numéricas e {len(missing)} n.a.; horizonte fixo de30dias; subtotal explicitamente parcial.'},
        ],
        'local_reconciliation': {
            'projection_row_count': len(rows), 'numeric_cost_rows': len(numeric_costs), 'pending_cost_rows': missing,
            'calculated_rows_sum_decimal': str(total), 'available_subtotal_reference_usd': str(reference),
            'matches_reference_within_1e_8': abs(total-reference) <= Decimal('0.00000001'),
            'complete_total_usd': model['calculated_reference']['complete_total_usd'],
            'basis_counts': dict(Counter(row['basis'] for row in rows)),
        },
        'free_sandbox_production': {
            'assignment_pages': [4, 9],
            'free_or_sandbox_accepted': True,
            'cloud_execution_evidence_required': True,
            'paid_production_deployment_required_by_these_pages': False,
            'same_design_production_cost_projection_required': True,
            'interpretation': 'O enunciado aceita ambiente gratuito/sandbox, mas exige informar essa condição e projetar o mesmo desenho em produção com a cadência pretendida. Não impõe upgrade ou ativação de billing para fins de entrega.',
            'current_document': 'Tarifas brutas antes de franquias/créditos e limitações são declaradas. O cálculo de produção permanece parcial, especialmente na parcela multicloud.',
            'measurement_examples': 'Dry run, dados escaneados, duração e tamanho de objetos são exemplos de ferramentas/evidências; a página9 não ordena iniciar nova extração ou novo job para esta revisão.',
            'not_zero_evidence': ['Créditos disponíveis', 'billingEnabled=false', 'Monitoring403', 'Auditoria sem resultados', '5.128 eventos sem contador de bytes', 'T2 eT3 com mesmo estoque líquido'],
        },
        'cadence': {
            'assignment_pages': [4, 9], 'mandatory_four_executions_found': False,
            'required_by_assignment': 'Explicar a atualização da fonte e usar a cadência pretendida na projeção.',
            'declared_scenario': model['cadence'],
            'finding': '4ciclos batch +4replays em30dias são hipótese do grupo, corretamente identificada; não atribuir essa frequência ao enunciado.',
            'normalization': '720horas e30dias são uma convenção de planejamento declarada, não uma reprodução exata da fatura de um mês civil.',
        },
        'production_monitoring_page8': {
            'required': 'Persistência histórica de duração/volume/status, latência real e alguma forma de notificação. README deve indicar responsável e prazo para descobrir falha.',
            'boundary': 'Budget com contatoInactive não prova entrega de alerta. Esse estado financeiro não substitui a verificação separada da política/canal de falha operacional.',
        },
        'total_estimate_decision': {
            'closed_total_justified_now': False,
            'appropriate_label': 'Estimativa parcial de30dias ancorada em medições, com18parcelas calculadas e3não fechadas.',
            'why': 'A presença dos cinco cabeçalhos não torna medidas ausentes em zero. O valor2.1496597505703168 é subtotal, não custo completo nem prova de aderência multicloud aosUS$140 totais.',
            'necessary_conditions': [
                'BigQueryStorageRead: quantidade elegível à cobrança medida/recuperada no projeto e período corretos e tarifa aplicável confirmada.',
                'SaídaGCP: rota/região e bytes transferidos elegíveis à cobrança confirmados; não usar tamanho do Parquet como substituto.',
                'Arquivo de logs: crescimento comprimido por janela/ciclo sustentado por medição, distinguindo retenção, outras cargas e atraso.',
                'Reconciliar unidades, períodos, faixas e cadência de todas as parcelas; proxies de requests precisam cobrir ou delimitar a carga pretendida, pois a janela atual exclui Bronze e o replay.',
                'Aplicar zero apenas com evidência de quantidade zero no escopo adequado, tarifa zero aplicável ou não aplicabilidade comprovada. Remover uma parcela do escopo não fecha o custo do desenho original.',
            ],
            'budget_condition': 'US$140 é o teto total do projeto, não mensal. Comparar o custo completo e o histórico atribuível sem descontar o mesmo uso novamente do saldo de créditos.',
            'cost_vs_alert_pending': 'Três linhas de custo n.a. são crescimento de logs e duas parcelasGCP. Verificação de e-mail é uma pendência de controle, não uma quarta linha de custo ausente.',
        },
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'AUDITED_LOCAL_ONLY', 'output': str(OUTPUT), 'pending_cost_rows': missing,
                      'closed_total_justified_now': False, 'assignment_sha256': result['sources']['assignment_pdf']['sha256']}))


if __name__ == '__main__':
    main()
