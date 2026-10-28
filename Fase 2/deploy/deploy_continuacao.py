"""Publish reviewed Silver/Gold code and create a stopped Glue job. No run is started."""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ACCOUNT = '637246457028'
REGION = 'us-east-2'
ASSETS = 'aws-glue-assets-637246457028-us-east-2'
BUCKET = 'fiap-tc-fase2-2026'
BRONZE_MANIFEST = ('s3://fiap-tc-fase2-2026/auditoria/bronze/ingestion_date=2026-10-20/'
                   'run_id=jr_cdfd2933220ec75b45c04518c6e5afe93eb98915f3b88328f8bde8d7183d3700/manifest.json')


def immutable_upload(s3, path, key):
    content = Path(path).read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    try:
        s3.put_object(Bucket=ASSETS, Key=key, Body=content,
                      IfNoneMatch='*', ServerSideEncryption='AES256',
                      Metadata={'sha256': digest})
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('PreconditionFailed', '412'):
            raise
        existing = s3.get_object(Bucket=ASSETS, Key=key)['Body'].read()
        if hashlib.sha256(existing).hexdigest() != digest:
            raise RuntimeError('An immutable asset exists with different content: ' + key)
    head = s3.head_object(Bucket=ASSETS, Key=key)
    if head['ContentLength'] != len(content) or head.get('Metadata', {}).get('sha256') != digest:
        raise RuntimeError('Asset verification failed: ' + key)
    return {'uri': 's3://' + ASSETS + '/' + key, 'sha256': digest, 'bytes': len(content)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--layer', choices=('silver', 'gold'), required=True)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--defaults-json', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--update-existing', action='store_true')
    args = parser.parse_args()
    session = boto3.Session(region_name=REGION)
    if session.client('sts').get_caller_identity()['Account'] != ACCOUNT:
        raise RuntimeError('Wrong AWS account')
    glue, s3 = session.client('glue'), session.client('s3')
    job_name = 'fiap-' + args.layer + '-inep-batch'
    root = Path(args.directory)
    script = root / (args.layer + '_glue.py')
    number = '02' if args.layer == 'silver' else '03'
    assets = {}
    for label, path in (
        ('script', script),
        ('original', root / (number + '_camada_' + args.layer + '_original.ipynb')),
        ('notebook', root / (number + '_camada_' + args.layer + '_glue.ipynb')),
    ):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        prefix = 'scripts' if label == 'script' else 'notebooks'
        assets[label] = immutable_upload(s3, path, prefix + '/' + job_name + '/' + digest + '/' + path.name)
    role = glue.get_job(JobName='fiap-bronze-inep-batch')['Job']['Role']
    defaults = json.loads(Path(args.defaults_json).read_text(encoding='utf-8-sig'))
    defaults.update({'--job-bookmark-option': 'job-bookmark-disable',
                     '--enable-glue-datacatalog': '', '--enable-metrics': '',
                     '--enable-continuous-cloudwatch-log': 'true',
                     '--enable-spark-ui': 'false',
                     '--TempDir': 's3://' + ASSETS + '/temporary/' + job_name + '/'})
    config = dict(
        Description='FIAP Fase 2: ' + args.layer + ' adaptada para Glue, historico e validacoes por execucao.',
        Role=role, Command={'Name': 'glueetl', 'ScriptLocation': assets['script']['uri'], 'PythonVersion': '3'},
        DefaultArguments=defaults, ExecutionProperty={'MaxConcurrentRuns': 1},
        MaxRetries=0, Timeout=15, GlueVersion='5.1', WorkerType='G.1X',
        NumberOfWorkers=2, ExecutionClass='STANDARD',
    )
    try:
        existing = glue.get_job(JobName=job_name)['Job']
    except glue.exceptions.EntityNotFoundException:
        existing = None
    if existing:
        if not args.update_existing:
            raise RuntimeError('Job exists; review before using --update-existing: ' + job_name)
        tags = glue.get_tags(ResourceArn='arn:aws:glue:' + REGION + ':' + ACCOUNT + ':job/' + job_name)['Tags']
        if tags.get('Project') != 'FIAP-TC-Fase2' or tags.get('Component') != args.layer + '-inep':
            raise RuntimeError('Refusing to change a job without the expected project tags')
        active = [r for r in glue.get_job_runs(JobName=job_name, MaxResults=10)['JobRuns']
                  if r['JobRunState'] in ('STARTING', 'RUNNING', 'STOPPING', 'WAITING')]
        if active:
            raise RuntimeError('An execution is active; refusing to change job')
        glue.update_job(JobName=job_name, JobUpdate=config)
        operation = 'updated'
    else:
        glue.create_job(Name=job_name, Tags={'Project': 'FIAP-TC-Fase2', 'Component': args.layer + '-inep'}, **config)
        operation = 'created'
    saved = glue.get_job(JobName=job_name)['Job']
    for key in ('Role', 'Command', 'MaxRetries', 'Timeout', 'GlueVersion', 'WorkerType', 'NumberOfWorkers'):
        if saved[key] != config[key]:
            raise RuntimeError('Saved configuration mismatch: ' + key)
    report = {'verified_at_utc': datetime.now(timezone.utc).isoformat(),
              'layer': args.layer, 'job_name': job_name, 'operation': operation,
              'account': ACCOUNT, 'region': REGION, 'assets': assets,
              'script_uri': assets['script']['uri'], 'original_uri': assets['original']['uri'],
              'notebook_uri': assets['notebook']['uri'],
              'workers': 2, 'worker_type': 'G.1X', 'timeout_minutes': 15,
              'max_retries': 0, 'run_started': False, 'default_arguments': defaults}
    Path(args.output).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
