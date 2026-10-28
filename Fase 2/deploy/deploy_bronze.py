"""Deploy the Bronze batch job from an authenticated AWS shell (no credentials in code)."""
import argparse
import hashlib
import json
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--region', default='us-east-2')
    parser.add_argument('--source-job', default='fiap-ingestao-bigquery-uf')
    parser.add_argument('--job-name', default='fiap-bronze-inep-batch')
    parser.add_argument('--script-file', default='bronze_glue.py')
    args = parser.parse_args()
    script = Path(args.script_file)
    digest = hashlib.sha256(script.read_bytes()).hexdigest()
    glue = boto3.client('glue', region_name=args.region)
    source = glue.get_job(JobName=args.source_job)['Job']
    try:
        glue.get_job(JobName=args.job_name)
    except glue.exceptions.EntityNotFoundException:
        pass
    else:
        raise RuntimeError(f'Job already exists; review it before changing: {args.job_name}')
    asset_bucket = source['Command']['ScriptLocation'].split('/')[2]
    script_key = f'scripts/{args.job_name}/{digest}/bronze_glue.py'
    boto3.client('s3', region_name=args.region).upload_file(str(script), asset_bucket, script_key)
    defaults = dict(source.get('DefaultArguments', {}))
    defaults.pop('--enable-auto-scaling', None)
    defaults.update({
        '--BUCKET': 'fiap-tc-fase2-2026',
        '--PREFIX': 'bronze/inep',
        '--AUDIT_PREFIX': 'auditoria/bronze',
        '--CONNECTION_NAME': 'Bigquery connection',
        '--PARENT_PROJECT': 'fiap1-512214',
        '--SOURCE_DATASET': 'basedosdados.br_inep_avaliacao_alfabetizacao',
        '--TABLES': 'alunos,municipio,uf,meta_alfabetizacao_brasil,meta_alfabetizacao_uf,meta_alfabetizacao_municipio,dicionario',
        '--job-bookmark-option': 'job-bookmark-disable',
    })
    config = dict(
        Name=args.job_name,
        Description='Bronze INEP: sete tabelas BigQuery, historico imutavel por execucao e validacao de contagens.',
        Role=source['Role'],
        Command={'Name': 'glueetl', 'ScriptLocation': f's3://{asset_bucket}/{script_key}', 'PythonVersion': '3'},
        Connections=source['Connections'],
        DefaultArguments=defaults,
        ExecutionProperty={'MaxConcurrentRuns': 1},
        MaxRetries=0,
        Timeout=30,
        GlueVersion='5.1',
        WorkerType='G.1X',
        NumberOfWorkers=2,
        ExecutionClass='STANDARD',
        Tags={'Project': 'FIAP-TC-Fase2', 'Component': 'bronze-inep'},
    )
    result = glue.create_job(**config)
    print(json.dumps({'job_name': result['Name'], 'script_sha256': digest,
                      'script_uri': config['Command']['ScriptLocation'],
                      'region': args.region, 'workers': 2, 'timeout_minutes': 30}, indent=2))


if __name__ == '__main__':
    main()
