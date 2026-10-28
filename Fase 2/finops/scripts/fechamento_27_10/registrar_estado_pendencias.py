"""Record evidence closure status locally. No cloud connections or file replacement."""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
ROOT = BASE.parents[1]
read = lambda p: json.loads(p.read_text(encoding='utf-8-sig'))
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
prior = ROOT/'outputs/finops_atualizado_20261027/modelo_finops.json'
model = read(prior)
logs = read(BASE/'fechamento_logs.json')
raw = read(BASE/'raw/aws_logs/log_growth_readonly.json')
assert len(raw['ingestion']['queries']) == 14
assert len(raw['api_calls']) == 6
assert not raw['errors']
rates = read(ROOT/'outputs/finops_atualizado_20261027/raw/aws/regional_price_list.json')
price = next(p for p in rates if p['sku'] == 'Y7EVPXWVUXVP9MUM')
assert price['unit'] == 'Metrics' and Decimal(price['usd']) == Decimal('0.00001')
report = {
    'schema':'fiap.finops-pending-evidence.v1',
    'observed_at_utc':datetime.now(timezone.utc).isoformat(),
    'project_ceiling_total_usd':140,
    'approved_scope':{'historical_metadata_only':True,'new_data_extractions':False,
                      'enable_billing':False,'enable_apis':False,'change_iam':False},
    'requirements':{'source_pdf_sha256':'e599dae420e7b72d60f66586a032b7b87ba3fec9dfae58a18025caaeedbde6c0',
                    'pages_reviewed':[4,8,9],
                    'required_columns':['Serviço','Unidade','Quantidade medida','Preço unitário','Custo mensal projetado'],
                    'production_equivalent_projection_required_in_sandbox':True,
                    'four_cycles_is_a_hypothesis_not_a_requirement':True,
                    'audit':'auditoria_enunciado.json'},
    'prior_forecast':{'model':str(prior.relative_to(ROOT)),'sha256':sha(prior),
                      'unchanged':True,'days':30,'batches':4,'replays':4,
                      'available_partial_subtotal_usd':model['calculated_reference']['available_subtotal_usd'],
                      'complete_total_usd':None,'numeric_cost_rows':18,'unmeasured_cost_rows':3,
                      'credits_remaining_usd':136.59,
                      'credits_observed_at_utc':model['actual']['credits_observed_at_utc'],
                      'credits_refreshed_this_investigation':False,
                      'caveat':'The 6-day support-request proxy did not contain a Bronze/replay cycle. It needs representative coverage before claiming a complete production cost.'},
    'new_log_measurement':{'summary':'fechamento_logs.json','sha256':sha(BASE/'fechamento_logs.json'),
                           'raw':'raw/aws_logs/log_growth_readonly.json',
                           'raw_sha256':sha(BASE/'raw/aws_logs/log_growth_readonly.json'),
                           'stock_bytes':7417226,
                           'reported_ingestion_bytes_uncompressed':6216753,
                           'reported_ingestion_log_events':27113,
                           'window_start_utc':'2026-10-27T00:00:00Z',
                           'window_end_exclusive_utc':'2026-10-27T18:00:00Z',
                           'missing_datapoints_not_zero':True,
                           'policy':'STANDARD','existing_daily_storedbytes_identities':0,
                           'compressed_archive_growth_per_cycle_bytes':None,
                           'stock_cost_normalized_month_usd':str(Decimal(7417226)/Decimal(2**30)*Decimal('0.03')),
                           'stock_cost_already_in_prior_forecast':True,
                           'why_pending':'Positive uncompressed ingestion with unchanged reported compressed stock; mixed preview/replay workload and bucket overlap prevent a measured per-cycle archive increment.'},
    'gcp':{'project':'fiap1-512214','new_script_executed':False,
           'credential_grant_performed':False,
           'status':'WAITING_FOR_EXPLICIT_CLOUD_SHELL_CREDENTIAL_RENEWAL_APPROVAL',
           'auto_review_rejected_action':'Click Autorizar in Google Cloud Shell credential-use dialog',
           'auto_review_reason':'The dialog permits credential use for this session and future API calls; the broad persistent grant was not explicitly approved.',
           'permission_question_pending':True,
           'screenshot':'gcp_autorizacao_pendente.png',
           'existing_console_observation':{'scope':'BigQuery Storage API, 30-day visible console window; not correlated billable bytes',
                                           'create_read_session_requests':24,'read_rows_requests':16,
                                           'request_counts_are_bytes':False},
           'prior_billing_snapshot':'../finops_2026_10_27/gcp/project_billing.json',
           'prior_billing_snapshot_refreshed':False,
           'table_metadata_search_measured_capacity_bytes':None,
           'storage_read_bytes':None,'egress_bytes':None,
           'collector':'scripts/collect_gcp_existing_metrics.py',
           'scope_note':'Only the authenticated existing console was browsed after the rejection. No CLI token, ADC, credential export or indirect execution was used to bypass the refusal.'},
    'measurement_reference_cost':{'service':'Amazon CloudWatch GetMetricData',
                                  'quantity_metrics_requested':14,'unit_price_usd':price['usd'],
                                  'one_off_reference_usd':str(Decimal(14)*Decimal(price['usd'])),
                                  'sku':price['sku'],'source_url':price['source_url'],
                                  'price_effective_date':price['effective_date'],
                                  'not_a_posted_bill_or_recurring_projection':True,
                                  'cost_explorer_called_this_investigation':False},
    'completion':{'closed_total_estimate':False,
                  'remaining_cost_components':['gcp_read','gcp_egress','logs_archive_growth'],
                  'unavailable_quantities_assigned_zero':False,
                  'budget_guaranteed':False,
                  'next_steps':['Human response to the already pending Google Cloud Shell renewal question.',
                                'If approved, execute the bounded metadata collector once. A repeated billing/permisson refusal remains a documented blocker; do not enable billing.',
                                'Reconcile archive stock after an existing workload settles, with attribution/retention checked; do not launch a new job to generate a measurement.',
                                'Retain explicit gaps if the source never recorded the necessary counters.']}
}
gcp_path=BASE/'raw/gcp_existing_metrics.json'
updated_model_path=BASE/'modelo_finops.json'
if gcp_path.is_file() and updated_model_path.is_file():
    observed_gcp=read(gcp_path)
    closure=read(BASE/'fechamento_gcp.json')
    updated=read(updated_model_path)
    assert sha(gcp_path)==closure['inputs']['gcp_sha256']
    assert updated['predecessor']['sha256']==sha(prior)
    assert observed_gcp['changes_billing_apis_iam'] is False
    report['gcp'].update(new_script_executed=True,credential_grant_performed=True,
        status='COLLECTED_EXISTING_EVIDENCE_STORAGE_READ_PROJECTED_EGRESS_PENDING',
        explicit_human_approval={'answer':'Sim, renovar o acesso para essas leituras',
                                'scope':'Existing Cloud Shell credential renewal for read-only metrics/metadata'},
        initial_auto_review_rejection_resolved=True,permission_question_pending=False,
        credential_renewal_proof='gcp_acesso_renovado.png',
        collection_proof='gcp_coleta_executada.png',
        collection_source='raw/gcp_existing_metrics.json',collection_sha256=sha(gcp_path),
        observed_at_utc=observed_gcp['observed_at_utc'],api_calls=observed_gcp['api_calls'],
        collection_top_level_errors=observed_gcp['errors'],
        billing_snapshot_refreshed=True,billing_enabled_current=False,
        monitoring_now_available=True,
        effective_permissions_confirmed=observed_gcp['effective_permissions'],
        measured_logical_capacity_bytes=closure['reconciliation']['logical_bytes'],
        source_rows_reconciled=closure['reconciliation']['total_source_rows'],
        current_storage_read_prospective_quantity_TiB_per_cycle=closure['storage_read_projection']['modeled_TiB_per_cycle'],
        current_storage_read_prospective_usd_30d=closure['storage_read_projection']['gross_usd_30d'],
        source_metadata_is_historical_billed_bytes=False,
        current_readrows_response_sizes_telemetry_bytes='1860997.0',
        current_readrows_request_count=13,current_create_read_session_request_count=16,
        audit_entries=observed_gcp['audit']['entries'],
        semantic_review='fechamento_gcp.json',
        scope_note='Human explicitly approved the credential renewal. It succeeded in the official standalone Cloud Shell. The bounded collector then read metadata/historical telemetry once, without SQL, new Storage Read sessions, billing/API/IAM changes.')
    report['forecast_update']={'model':'modelo_finops.json','sha256':sha(updated_model_path),
        'available_partial_subtotal_usd':updated['calculated_reference']['available_subtotal_usd'],
        'complete_total_usd':None,'numeric_cost_rows':19,'unmeasured_cost_rows':2,
        'read_cost_basis':'Measured logical capacity; one prospective complete read per batch; no free tier applied, not historical billed usage.'}
    report['completion'].update(remaining_cost_components=['gcp_egress','logs_archive_growth'],
        resolved_cost_components={'gcp_read':'Measured prospective full-read projection, not historical billed bytes'},
        next_steps=['Recover audited serializedResponseBytes correlated to the original extraction, or historical billed network SKU quantities, if those records exist.',
                    'Do not repeat the same response-size metric query to invent billing equivalence; it does not resolve the streaming/SKU semantics.',
                    'Reconcile compressed log archive after existing workload settles, with attribution/retention checked; do not start a new extraction merely to measure it.',
                    'Maintain both unavailable costs as n.a. and the total as partial until evidence exists; preserve the support-request coverage caveat.'])
target=BASE/'estado_pendencias.json'
target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'status':'RECORDED_PARTIAL_EVIDENCE','output':str(target),
                  'sha256':sha(target),'complete_total_usd':None,'cost_reference_usd':report['measurement_reference_cost']['one_off_reference_usd']},ensure_ascii=False))
