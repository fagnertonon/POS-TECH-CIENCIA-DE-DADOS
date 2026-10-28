"""Verify one persisted attempt and unchanged analytical bytes; test routing only."""
import argparse,hashlib,json
from datetime import datetime,timezone
from pathlib import Path

def sha(b):return hashlib.sha256(b).hexdigest()
def main():
    p=argparse.ArgumentParser();p.add_argument('--reference-hashes',required=True)
    p.add_argument('--output',required=True);a=p.parse_args()
    hashes=json.loads(a.reference_hashes)
    import boto3
    from botocore.config import Config
    cfg=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=15)
    clients={s:boto3.client(s,region_name='us-east-2',config=cfg) for s in ['sts','s3','events']}
    assert clients['sts'].get_caller_identity()['Account']=='637246457028'
    folder=Path('/home/cloudshell-user/observabilidade_20261027_positive')
    report=json.loads((folder/'finalization_report.json').read_bytes())
    metrics=report['execution_metrics']
    bucket='fiap-tc-fase2-2026'
    assert metrics['audit_key'].startswith('auditoria/streaming/analitico/execucoes/')
    def get(key):
        response=clients['s3'].get_object(Bucket=bucket,Key=key)
        assert response.get('ContentLength',262145)<=262144
        value=response['Body'].read(262145)
        assert len(value)<=262144
        return value
    audit=get(metrics['audit_key'])
    assert json.loads(audit)==metrics
    assert sha(audit)==sha((folder/'execution_metrics.json').read_bytes())
    comparisons={}
    for layer,data in report['datasets'].items():
        assert data['key'].startswith(layer+'/inep_streaming/')
        actual=sha(get(data['key']))
        comparisons[layer]={'expected_sha256':hashes[layer],'actual_sha256':actual,'unchanged':actual==hashes[layer]}
    assert report['manifest_key'].startswith('auditoria/streaming/analitico/runs/')
    actual=sha(get(report['manifest_key']))
    comparisons['manifest']={'expected_sha256':hashes['manifest'],'actual_sha256':actual,'unchanged':actual==hashes['manifest']}
    assert all(x['unchanged'] for x in comparisons.values())
    jobs=['fiap-bronze-inep-batch','fiap-silver-inep-batch','fiap-gold-inep-batch']
    pattern={'source':['aws.glue'],'detail-type':['Glue Job State Change'],
       'detail':{'jobName':jobs,'state':['FAILED','TIMEOUT','STOPPED','ERROR','EXPIRED']}}
    matches=[]
    for job,state,expected in [(jobs[0],'FAILED',True),(jobs[2],'TIMEOUT',True),
                                (jobs[1],'SUCCEEDED',False),('other-job','FAILED',False)]:
        event={'version':'0','id':'00000000-0000-0000-0000-000000000000','detail-type':'Glue Job State Change',
         'source':'aws.glue','account':'637246457028','time':'2026-10-27T19:00:00Z','region':'us-east-2',
         'resources':[],'detail':{'jobName':job,'state':state,'jobRunId':'TEST_PATTERN_ONLY'}}
        observed=clients['events'].test_event_pattern(EventPattern=json.dumps(pattern),Event=json.dumps(event))['Result']
        matches.append({'job':job,'state':state,'expected':expected,'matched':observed})
        assert observed==expected
    result={'status':'SUCCEEDED','verified_at_utc':datetime.now(timezone.utc).isoformat(),
     'execution_metrics':metrics,'execution_metrics_sha256':sha(audit),'s3_audit_matches_local':True,
     'analytical_bytes_unchanged':comparisons,'event_pattern_tests':matches,
     'pattern_test_does_not_deliver_notifications':True,'jobs_started':[],'messages_sent':[],
     'script_sha256':sha(Path('/home/cloudshell-user/observabilidade_20261027_code/finalize_simulation.py').read_bytes()),
     'api_calls':{'sts':1,'s3_get':4,'events_test_pattern':4},'verification_mutations':[]}
    out=Path(a.output)
    with out.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps({'output':str(out),'sha256':sha(out.read_bytes()),'status':result['status']}))

if __name__=='__main__':main()
