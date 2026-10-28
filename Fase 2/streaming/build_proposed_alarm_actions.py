"""Generate a separate, UNDEPLOYED alarm-action proposal locally.

No boto3, AWS calls, change sets, e-mails or overwrite of the historical template.
The output file must be new. See observabilidade/cloudformation/PLANO_RECONCILIACAO.md.
"""
import argparse
import json
from pathlib import Path

from build_stack import build as build_historical


ALARM_IDS = ('DeadLetterAlarm', 'RejectedEventAlarm', 'QueueAgeAlarm')


def build():
    template = build_historical()
    # The active processing resources are taken verbatim from the original builder.
    template['Description'] += (
        ' | PROPOSTO, NAO IMPLANTADO: gerado localmente em revisao de 28/10/2026; '
        'adiciona AlertTopicArn e acoes SNS aos tres alarmes existentes. '
        'Nao usar deploy_simulation.py para esta migracao; ver PLANO_RECONCILIACAO.md.'
    )
    template['Parameters']['AlertTopicArn'] = {
        'Type': 'String',
        'Description': 'ARN do topico SNS operacional existente; conferir conta/regiao antes de uma futura migracao.',
        'AllowedPattern': r'^arn:aws:sns:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_-]+$',
    }
    alarms = {k for k, v in template['Resources'].items() if v['Type'] == 'AWS::CloudWatch::Alarm'}
    if alarms != set(ALARM_IDS):
        raise ValueError('Unexpected alarm set; review the proposal before regeneration')
    for logical in ALARM_IDS:
        props = template['Resources'][logical]['Properties']
        props.update(ActionsEnabled=True, AlarmActions=[{'Ref': 'AlertTopicArn'}], OKActions=[])
    return template


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='New local JSON file; existing files are refused')
    args = parser.parse_args(argv)
    content = json.dumps(build(), ensure_ascii=False, indent=2) + '\n'
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as output:
        output.write(content)
    print('Local proposal only; no cloud operation: ' + str(args.output))


if __name__ == '__main__':
    main()
