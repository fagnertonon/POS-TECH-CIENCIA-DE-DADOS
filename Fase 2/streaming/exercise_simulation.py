"""One bounded AWS replay: 30 arrivals, 5 duplicates and one semantic rejection.

Run only after the local suite and the existing stack UPDATE have succeeded.
No purge, redrive, BigQuery read, schedule or Glue job is performed.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import time
import uuid
from urllib.request import urlopen
import zipfile


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output-dir', required=True)
    p.add_argument('--region', default='us-east-2')
    args = p.parse_args(argv)
    if args.region != 'us-east-2':
        raise ValueError('Controlled replay must use Ohio')
    root = Path(__file__).resolve().parent
    out = Path(args.output_dir)
    if out.exists():
        raise FileExistsError('Choose a new evidence directory')
    import boto3
    import producer
    import lambda_function as consumer
    session = boto3.Session(region_name=args.region)
    if session.client('sts').get_caller_identity()['Account'] != '637246457028':
        raise ValueError('Unexpected AWS account')
    cfn = session.client('cloudformation')
    stack = cfn.describe_stacks(StackName='fiap-inep-streaming')['Stacks'][0]
    if stack['StackStatus'] != 'UPDATE_COMPLETE':
        raise ValueError('Reviewed UPDATE is not complete')
    deployed = cfn.get_template(StackName='fiap-inep-streaming', TemplateStage='Original')['TemplateBody']
    if isinstance(deployed, str):
        deployed = json.loads(deployed)
    local_source = (root/'lambda_function.py').read_text(encoding='utf-8')
    template_source = deployed['Resources']['ConsumerFunction']['Properties']['Code']['ZipFile']
    # The console's GetTemplate export can replace non-ASCII comments with '?'.
    # Recognize only this exact, observed export conversion; the actual Lambda
    # ZIP below MUST still match every UTF-8 source byte before the first send.
    if template_source not in (local_source, local_source.encode('ascii','replace').decode()):
        raise ValueError('Deployed template differs beyond the known export encoding')
    outputs = {o['OutputKey']:o['OutputValue'] for o in stack['Outputs']}
    lamb = session.client('lambda')
    cfg = lamb.get_function_configuration(FunctionName=outputs['FunctionName'])
    mapping = lamb.get_event_source_mapping(UUID=outputs['EventSourceMappingUuid'])
    if (cfg['State'], cfg['LastUpdateStatus'], cfg['MemorySize'], cfg['Timeout'], mapping['State']) != ('Active','Successful',256,10,'Enabled'):
        raise ValueError('Consumer is not ready or compute caps drifted')
    concurrency = lamb.get_function_concurrency(FunctionName=outputs['FunctionName'])
    if (concurrency.get('ReservedConcurrentExecutions'), mapping.get('BatchSize'),
        mapping.get('ScalingConfig',{}).get('MaximumConcurrency'),
        mapping.get('FunctionResponseTypes')) != (2,5,2,['ReportBatchItemFailures']):
        raise ValueError('Concurrency, batch size or partial failures configuration drifted')
    # Verify deployed bytes as well as the template; never print/persist the
    # temporary signed URL returned by AWS for this authorized source read.
    location = lamb.get_function(FunctionName=outputs['FunctionName'])['Code']['Location']
    with urlopen(location,timeout=20) as response:
        deployed_zip = response.read(1024*1024+1)
    if len(deployed_zip) > 1024*1024:
        raise ValueError('Unexpected deployed package size')
    import base64
    if base64.b64encode(hashlib.sha256(deployed_zip).digest()).decode() != cfg['CodeSha256']:
        raise ValueError('Deployed ZIP checksum differs from Lambda configuration')
    with zipfile.ZipFile(io.BytesIO(deployed_zip)) as archive:
        if archive.read('index.py').decode('utf-8') != (root/'lambda_function.py').read_text(encoding='utf-8'):
            raise ValueError('Running Lambda source differs from locally tested source')
    rows, source = producer.load_sample(session.client('s3'), producer.DEFAULT_MANIFEST, 30)
    if len(rows) != 30:
        raise ValueError('Expected exactly thirty sampled rows')
    for row in rows:
        consumer.transform_payload(row['payload'])
    now = datetime.now(timezone.utc)
    run_id = 'sim_gold_' + now.strftime('%Y%m%dT%H%M%SZ') + '_' + uuid.uuid4().hex[:8]
    bad = producer.make_event(rows[0], 'invalid_weight_' + uuid.uuid4().hex[:16], source['snapshot_run_id'])
    bad['payload'] = dict(bad['payload'], presenca='1', preenchimento_caderno='1', alfabetizado='1', peso_aluno=0)
    try:
        consumer.transform_payload(bad['payload'])
    except consumer.InvalidPayload:
        pass
    else:
        raise ValueError('Negative fixture unexpectedly valid')
    out.mkdir(parents=True)
    sqs = session.client('sqs')
    invalid_response = sqs.send_message(QueueUrl=outputs['QueueUrl'],MessageBody=producer.strict_json(bad))
    invalid = {'event':bad,'message_id':invalid_response['MessageId'],'expected_status':'QUALITY_REJECTED',
               'expected_bronze_objects':1,'expected_silver_objects':0,'expected_gold_objects':0,
               'expected_dlq_after_max_receive_count':5}
    (out/'invalid_semantic.json').write_text(json.dumps(invalid,indent=2)+'\n',encoding='utf-8')
    # Reuse the rows already sampled: do not download the same shard twice.
    folder = out/'producer'
    folder.mkdir()
    sends, bodies = [], []
    report = {'schema_version':1,'run_id':run_id,'source':source,'unique_event_count':30,
              'duplicate_send_count':5,'interval_seconds':1,'started_at_utc':producer.iso_utc(datetime.now(timezone.utc)),
              'send_results':sends,'simulation':True,'status':'STARTED'}
    try:
        with (folder/'events.jsonl').open('x',encoding='utf-8',newline='\n') as stream:
            for index, row in enumerate(rows):
                if index:
                    time.sleep(1)
                body = producer.strict_json(producer.make_event(row,run_id,source['snapshot_run_id']))
                bodies.append(body)
                stream.write(body+'\n')
                stream.flush()
                sends.append(producer.send_body(sqs,outputs['QueueUrl'],body,'NEW',index+1))
            for index in range(5):
                time.sleep(1)
                sends.append(producer.send_body(sqs,outputs['QueueUrl'],bodies[index],'DUPLICATE',31+index))
        report['status'] = 'SUCCEEDED'
    except Exception as exc:
        report['status'], report['error_type'] = 'FAILED', type(exc).__name__
        raise
    finally:
        report['confirmed_send_count'] = len(sends)
        report['finished_at_utc'] = producer.iso_utc(datetime.now(timezone.utc))
        (folder/'producer_report.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    (out/'exercise_metadata.json').write_text(json.dumps({'run_id':run_id,'source':source,
        'new_events':30,'duplicate_sends':5,'semantic_rejections':1,'glue_runs_started':0,
        'bigquery_queries_started':0,'schedulers_created':0,'pipeline_version':consumer.PIPELINE_VERSION,
        'source_sha256':hashlib.sha256((root/'lambda_function.py').read_bytes()).hexdigest(),
        'template_export_exact_match':template_source==local_source,
        'template_export_ascii_replacement_match':template_source==local_source.encode('ascii','replace').decode(),
        'running_lambda_source_exact_match':True,
        'started_at_utc':producer.iso_utc(now),'completed_at_utc':producer.iso_utc(datetime.now(timezone.utc))},indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'status':'REPLAY_SENT','run_id':run_id,'invalid_message_id':invalid['message_id'],
                      'evidence_directory':str(out)}))


if __name__ == '__main__':
    main()
