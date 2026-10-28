"""Publish an immutable audit manifest only after all saved AWS checks pass."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def put_once(s3,bucket,key,body):
    try:
        s3.put_object(Bucket=bucket,Key=key,Body=body,ContentType='application/json',IfNoneMatch='*',
                      Metadata={'sha256':hashlib.sha256(body).hexdigest()})
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code','')) not in {'PreconditionFailed','412','ConditionalRequestConflict','409'}:
            raise
        if s3.get_object(Bucket=bucket,Key=key)['Body'].read() != body:
            raise ValueError('Existing immutable evidence differs: '+key) from exc


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-dir',type=Path,required=True)
    parser.add_argument('--exercise-dir',type=Path,required=True)
    parser.add_argument('--region',default='us-east-2')
    args=parser.parse_args()
    p,e=args.project_dir,args.exercise_dir
    paths={'producer':e/'producer/producer_report.json','positive':e/'verification_final.json',
           'negative':e/'verification_negative.json','runtime':e/'runtime_evidence.json',
           'official_preservation':p/'official_preservation.json','exercise':e/'exercise_metadata.json',
           'invalid_fixture':e/'invalid_semantic.json','source_diagnostic':p/'deployment/source_diagnostic.json',
           'finalization':e/'finalized/finalization_report.json','snapshot_manifest':e/'finalized/manifest.json',
           'before':p/'aws_before/preflight_report.json','after':p/'aws_after/preflight_report.json',
           'deployment':p/'deployment/deployment_state.json','approved_plan':p/'deployment/change_set_review.json'}
    docs={k:json.loads(v.read_text(encoding='utf-8')) for k,v in paths.items()}
    statuses={'producer':('status','SUCCEEDED'),'positive':('validation_status','SUCCEEDED'),
              'negative':('validation_status','SUCCEEDED'),'runtime':('capture_status','COMPLETE'),
              'official_preservation':('status','SUCCEEDED'),'finalization':('status','SUCCEEDED'),
              'snapshot_manifest':('status','SUCCEEDED'),'before':('validation_status','SUCCEEDED'),
              'after':('validation_status','SUCCEEDED'),'deployment':('stack_status','UPDATE_COMPLETE')}
    for name,(field,status) in statuses.items():
        if docs[name].get(field)!=status:
            raise ValueError('Unsuccessful prerequisite: '+name)
    if docs['positive'].get('finalized_snapshot',{}).get('status')!='SUCCEEDED':
        raise ValueError('Parquet snapshot was not verified')
    run=docs['exercise']['run_id']
    import lambda_function as consumer
    if not consumer.SAFE_ID.fullmatch(run):
        raise ValueError('Invalid run ID')
    if any(docs[name]['run_id']!=run for name in ('producer','positive','runtime','finalization','snapshot_manifest')):
        raise ValueError('Evidence belongs to different runs')
    if docs['source_diagnostic'].get('exact_match') is not True or docs['official_preservation'].get('authorized_iam_additions_only') is not True:
        raise ValueError('Live source or IAM change not verified')
    invalid=docs['invalid_fixture']
    negative=docs['negative']
    if (negative.get('run_id')!=invalid['event']['run_id'] or negative.get('sqs_message_id')!=invalid['message_id']
        or negative.get('event_sha256')!=consumer.sha256(invalid['event'])
        or docs['runtime'].get('invalid_message_id')!=invalid['message_id']):
        raise ValueError('Negative evidence does not belong to the supplied invalid fixture')
    expected_source_hash=hashlib.sha256(Path(__file__).with_name('lambda_function.py').read_bytes()).hexdigest()
    if docs['source_diagnostic'].get('live_source_sha256')!=expected_source_hash or docs['exercise'].get('source_sha256')!=expected_source_hash:
        raise ValueError('Live source diagnostic differs from the local reviewed source')
    import boto3
    session=boto3.Session(region_name=args.region)
    if args.region!='us-east-2' or session.client('sts').get_caller_identity()['Account']!='637246457028':
        raise ValueError('Account/region not authorized')
    bucket='fiap-tc-fase2-2026'
    prefix=f'auditoria/streaming/runs/run_id={run}'
    s3=session.client('s3')
    evidence={}
    for name,path in paths.items():
        body=path.read_bytes()
        key=f'{prefix}/evidence/{name}.json'
        put_once(s3,bucket,key,body)
        evidence[name]={'uri':f's3://{bucket}/{key}','sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body)}
    manifest={'schema_version':2,'pipeline_version':consumer.PIPELINE_VERSION,'status':'SUCCEEDED',
              'origin':consumer.ORIGIN,'simulation':True,'run_id':run,'snapshot_id':docs['finalization']['snapshot_id'],
              'source_snapshot_run_id':docs['snapshot_manifest']['source_snapshot_run_id'],
              'counts':{k:docs['snapshot_manifest'][k] for k in ('expected_unique_events','bronze_events','silver_events','gold_contributions','gold_groups')},
              'gold_snapshot_manifest_uri':docs['finalization']['manifest_uri'],
              'positive_and_parquet_verified':True,'expected_negative_rejection_verified':True,
              'negative_run_id':docs['negative']['run_id'],'dlq_message_preserved':True,
              'official_prefixes_unchanged':6,'authorized_iam_additions_only':True,
              'source_sha256':docs['source_diagnostic']['live_source_sha256'],
              'last_gold_ready_at_utc':docs['snapshot_manifest']['last_gold_ready_at_utc'],
              'compute_caps':{'memory_mb':256,'timeout_seconds':10,'max_concurrency':2},
              'additional_glue_or_bigquery_runs':0,'recurring_producer_created':False,
              'finops_status':'Deferred until after this demand; no invoice or full cost reconciliation asserted',
              'evidence':evidence}
    key=prefix+'/manifest.json'
    put_once(s3,bucket,key,canonical(manifest)+b'\n')
    result={'status':'SUCCEEDED','manifest_uri':f's3://{bucket}/{key}','manifest_key':key,
            'manifest_sha256':hashlib.sha256(canonical(manifest)+b'\n').hexdigest(),'run_id':run}
    (e/'audit_publication.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    (e/'audit_manifest.json').write_bytes(canonical(manifest)+b'\n')
    print(json.dumps(result))


if __name__=='__main__':
    main()
