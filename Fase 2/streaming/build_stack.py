"""Build the CloudFormation template, embedding the reviewed Lambda source."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FUNCTION = 'fiap-inep-streaming-consumer'
QUEUE = 'fiap-inep-streaming'
ROLE = 'fiap-lambda-inep-streaming'
PIPELINE = 'fiap-inep-streaming'


def sub(value):
    return {'Fn::Sub': value}


def attr(resource, key):
    return {'Fn::GetAtt': [resource, key]}


def build():
    source = (ROOT / 'lambda_function.py').read_text(encoding='utf-8')
    bucket = {'Ref': 'BucketName'}
    s3_paths = [sub('arn:${AWS::Partition}:s3:::${BucketName}/' + prefix + '/*') for prefix in (
        'bronze/inep_streaming', 'silver/inep_streaming/entity=alunos',
        'gold/inep_streaming/contribuicoes', 'auditoria/streaming/idempotency',
        'auditoria/streaming/processed')]
    resources = {
        'DeadLetterQueue': {'Type': 'AWS::SQS::Queue', 'Properties': {
            'QueueName': QUEUE + '-dlq', 'MessageRetentionPeriod': 1209600,
            'SqsManagedSseEnabled': True,
            'RedriveAllowPolicy': {'redrivePermission': 'byQueue', 'sourceQueueArns': [sub('arn:${AWS::Partition}:sqs:${AWS::Region}:${AWS::AccountId}:' + QUEUE)]},
            'Tags': [{'Key': 'Project', 'Value': 'FIAP-TC-Fase2'}],
        }},
        'SourceQueue': {'Type': 'AWS::SQS::Queue', 'Properties': {
            'QueueName': QUEUE, 'VisibilityTimeout': 60, 'ReceiveMessageWaitTimeSeconds': 20,
            'MessageRetentionPeriod': 345600, 'SqsManagedSseEnabled': True,
            'RedrivePolicy': {'deadLetterTargetArn': attr('DeadLetterQueue', 'Arn'), 'maxReceiveCount': 5},
            'Tags': [{'Key': 'Project', 'Value': 'FIAP-TC-Fase2'}],
        }},
        'ConsumerLogs': {'Type': 'AWS::Logs::LogGroup', 'DeletionPolicy': 'Retain', 'UpdateReplacePolicy': 'Retain', 'Properties': {
            'LogGroupName': '/aws/lambda/' + FUNCTION, 'RetentionInDays': 14,
        }},
        'ConsumerRole': {'Type': 'AWS::IAM::Role', 'Properties': {
            'RoleName': ROLE,
            'AssumeRolePolicyDocument': {'Version': '2012-10-17', 'Statement': [{
                'Effect': 'Allow', 'Principal': {'Service': 'lambda.amazonaws.com'}, 'Action': 'sts:AssumeRole',
            }]},
            'Policies': [{'PolicyName': 'StreamingScopedAccess', 'PolicyDocument': {
                'Version': '2012-10-17', 'Statement': [
                    {'Sid': 'PollOnlyTheStreamingQueue', 'Effect': 'Allow', 'Action': ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'], 'Resource': attr('SourceQueue', 'Arn')},
                    {'Sid': 'PersistOnlyStreamingObjects', 'Effect': 'Allow', 'Action': ['s3:GetObject', 's3:PutObject'], 'Resource': s3_paths},
                    {'Sid': 'WriteOnlyTheConsumerLogs', 'Effect': 'Allow', 'Action': ['logs:CreateLogStream', 'logs:PutLogEvents'],
                     'Resource': sub('arn:${AWS::Partition}:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda/' + FUNCTION + ':*')},
                ],
            }}],
            'Tags': [{'Key': 'Project', 'Value': 'FIAP-TC-Fase2'}],
        }},
        'ConsumerFunction': {'Type': 'AWS::Lambda::Function', 'DependsOn': 'ConsumerLogs', 'Properties': {
            'FunctionName': FUNCTION, 'Runtime': 'python3.12', 'Handler': 'index.lambda_handler',
            'Role': attr('ConsumerRole', 'Arn'), 'Timeout': 10, 'MemorySize': 256,
            'ReservedConcurrentExecutions': 2,
            'Description': 'Simulacao INEP v2: SQS para Bronze, Silver e Gold isoladas; idempotencia e qualidade.',
            'Code': {'ZipFile': source},
            'Environment': {'Variables': {'BUCKET': bucket, 'PREFIX': 'bronze/inep_streaming',
                'IDEMPOTENCY_PREFIX': 'auditoria/streaming/idempotency', 'ALLOWED_ENTITIES': 'alunos',
                'SILVER_PREFIX': 'silver/inep_streaming', 'GOLD_PREFIX': 'gold/inep_streaming',
                'PROCESSED_PREFIX': 'auditoria/streaming/processed'}},
            'Tags': [{'Key': 'Project', 'Value': 'FIAP-TC-Fase2'}],
        }},
        'QueueConsumerMapping': {'Type': 'AWS::Lambda::EventSourceMapping', 'Properties': {
            'FunctionName': {'Ref': 'ConsumerFunction'}, 'EventSourceArn': attr('SourceQueue', 'Arn'),
            'BatchSize': 5, 'MaximumBatchingWindowInSeconds': 0,
            'FunctionResponseTypes': ['ReportBatchItemFailures'],
            'ScalingConfig': {'MaximumConcurrency': 2}, 'Enabled': True,
        }},
    }
    alarm_common = {'ComparisonOperator': 'GreaterThanOrEqualToThreshold', 'EvaluationPeriods': 1,
                    'DatapointsToAlarm': 1, 'Period': 60, 'Statistic': 'Sum', 'TreatMissingData': 'notBreaching'}
    for logical, suffix, namespace, metric, dimensions, threshold, statistic, description in [
        ('DeadLetterAlarm', 'dlq', 'AWS/SQS', 'ApproximateNumberOfMessagesVisible', [{'Name': 'QueueName', 'Value': QUEUE + '-dlq'}], 1, 'Maximum', 'Mensagem na DLQ; revisar erros no CloudWatch e corrigir antes do redrive. Alerta visivel no console, sem e-mail configurado.'),
        ('RejectedEventAlarm', 'rejected', 'FIAP/Streaming', 'Failed', [{'Name': 'Pipeline', 'Value': PIPELINE}], 1, 'Sum', 'Evento rejeitado pelo consumidor; verificar logs e DLQ. Alerta visivel no console.'),
        ('QueueAgeAlarm', 'age', 'AWS/SQS', 'ApproximateAgeOfOldestMessage', [{'Name': 'QueueName', 'Value': QUEUE}], 120, 'Maximum', 'Atraso de processamento maior ou igual a 120 segundos; revisar Lambda e fila.'),
    ]:
        resources[logical] = {'Type': 'AWS::CloudWatch::Alarm', 'Properties': {
            **alarm_common, 'AlarmName': PIPELINE + '-' + suffix, 'AlarmDescription': description,
            'Namespace': namespace, 'MetricName': metric, 'Dimensions': dimensions,
            'Threshold': threshold, 'Statistic': statistic,
        }}
    return {
        'AWSTemplateFormatVersion': '2010-09-09',
        'Description': 'FIAP Fase2 streaming simulacao Bronze-Silver-Gold: SQS/Lambda/S3, role restrita e DLQ.',
        'Parameters': {'BucketName': {'Type': 'String', 'Default': 'fiap-tc-fase2-2026'}},
        'Resources': resources,
        'Outputs': {
            'QueueUrl': {'Value': {'Ref': 'SourceQueue'}}, 'QueueArn': {'Value': attr('SourceQueue', 'Arn')},
            'DeadLetterQueueUrl': {'Value': {'Ref': 'DeadLetterQueue'}},
            'FunctionName': {'Value': {'Ref': 'ConsumerFunction'}}, 'FunctionArn': {'Value': attr('ConsumerFunction', 'Arn')},
            'ExecutionRoleArn': {'Value': attr('ConsumerRole', 'Arn')},
            'EventSourceMappingUuid': {'Value': {'Ref': 'QueueConsumerMapping'}},
            'LogGroup': {'Value': {'Ref': 'ConsumerLogs'}},
        },
    }


if __name__ == '__main__':
    template = build()
    target = ROOT / 'streaming_stack.json'
    target.write_text(json.dumps(template, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    role = template['Resources']['ConsumerRole']['Properties']
    (ROOT / 'iam_role_review.json').write_text(json.dumps(role, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(f'{target.name}: {target.stat().st_size} bytes; IAM role {ROLE}')
