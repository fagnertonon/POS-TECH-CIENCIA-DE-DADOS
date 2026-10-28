"""Offline checks against saved evidence; no cloud client or deployment."""
import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'streaming'))
import build_proposed_alarm_actions as proposal


def read(relative):
    return json.loads((ROOT / relative).read_text(encoding='utf-8-sig'))


class ProposedInfrastructure(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template = read('observabilidade/cloudformation/observabilidade_stack.json')
        cls.observed = read('evidencias/observabilidade/raw/notifications_verified_04.json')
        cls.before = read('evidencias/observabilidade/raw/before.json')

    def resolve(self, value):
        if isinstance(value, list):
            return [self.resolve(item) for item in value]
        if not isinstance(value, dict):
            return value
        refs = {
            'AWS::Region': 'us-east-2', 'AWS::AccountId': self.observed['topic_owner'],
            'AlertTopic': self.observed['topic_arn'],
            'BronzeJobName': 'fiap-bronze-inep-batch',
            'SilverJobName': 'fiap-silver-inep-batch', 'GoldJobName': 'fiap-gold-inep-batch',
        }
        if set(value) == {'Ref'}:
            return refs[value['Ref']]
        if set(value) == {'Fn::Sub'}:
            result = value['Fn::Sub']
            for name, text in refs.items():
                result = result.replace('${' + name + '}', text)
            if '${' in result:
                raise AssertionError('Unresolved template variable: ' + result)
            return result
        return {key: self.resolve(item) for key, item in value.items()}

    def test_streaming_changes_are_limited_to_alarm_actions_and_parameter(self):
        baseline = read('streaming/streaming_stack.json')
        updated = proposal.build()
        self.assertEqual(set(updated['Resources']), set(baseline['Resources']))
        expected_alarms = {'DeadLetterAlarm', 'RejectedEventAlarm', 'QueueAgeAlarm'}
        changed = {name for name in baseline['Resources']
                   if updated['Resources'][name] != baseline['Resources'][name]}
        self.assertEqual(changed, expected_alarms)
        for name in expected_alarms:
            resource = copy.deepcopy(updated['Resources'][name])
            props = resource['Properties']
            self.assertTrue(props.pop('ActionsEnabled'))
            self.assertEqual(props.pop('AlarmActions'), [{'Ref': 'AlertTopicArn'}])
            self.assertEqual(props.pop('OKActions'), [])
            self.assertEqual(resource, baseline['Resources'][name])
        parameters = copy.deepcopy(updated['Parameters'])
        self.assertIn('AlertTopicArn', parameters)
        del parameters['AlertTopicArn']
        self.assertEqual(parameters, baseline['Parameters'])
        self.assertEqual(updated['Outputs'], baseline['Outputs'])
        self.assertEqual(updated['AWSTemplateFormatVersion'], baseline['AWSTemplateFormatVersion'])

    def test_saved_proposal_is_reproducible_and_cli_refuses_overwrite(self):
        self.assertEqual(read('streaming/streaming_stack_com_alarmactions_PROPOSTO.json'), proposal.build())
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'proposal.json'
            with contextlib.redirect_stdout(io.StringIO()):
                proposal.main(['--output', str(output)])
            initial = output.read_bytes()
            self.assertEqual(json.loads(initial), proposal.build())
            with self.assertRaises(FileExistsError):
                proposal.main(['--output', str(output)])
            self.assertEqual(output.read_bytes(), initial)

    def test_sns_policy_preserves_observed_scope_and_owner_statement(self):
        document = self.template['Resources']['AlertTopicPolicy']['Properties']['PolicyDocument']
        self.assertEqual(self.resolve(document), self.observed['topic_policy'])

    def test_eventbridge_preserves_filter_envelope_target_and_retry_bounds(self):
        rule = self.template['Resources']['BatchFailureRule']['Properties']
        self.assertEqual(rule['Name'], self.observed['rule']['Name'])
        self.assertEqual(rule['State'], self.observed['rule']['State'])
        self.assertEqual(self.resolve(rule['EventPattern']), json.loads(self.observed['rule']['EventPattern']))
        self.assertEqual(self.resolve(rule['Targets']), self.observed['targets'])

    def test_batch_alarm_definitions_preserve_measured_thresholds_and_dimensions(self):
        fields = ['AlarmName', 'AlarmDescription', 'Namespace', 'MetricName', 'Dimensions',
                  'Statistic', 'Period', 'EvaluationPeriods', 'DatapointsToAlarm',
                  'Threshold', 'ComparisonOperator', 'TreatMissingData',
                  'OKActions', 'InsufficientDataActions']
        for logical, name in [('SilverFailureAlarm', 'fiap-silver-inep-failure'),
                              ('GoldFailureAlarm', 'fiap-gold-inep-failure')]:
            props = self.resolve(self.template['Resources'][logical]['Properties'])
            observed = next(item for item in self.before['alarms'] if item['AlarmName'] == name)
            for key in fields:
                self.assertEqual(props[key], observed[key], (logical, key))
            self.assertTrue(props['ActionsEnabled'])
            self.assertEqual(props['AlarmActions'], [self.observed['topic_arn']])

    def test_existing_resources_have_retain_and_streaming_ownership_is_separate(self):
        expected = {
            'AlertTopic': 'AWS::SNS::Topic', 'AlertSubscription': 'AWS::SNS::Subscription',
            'AlertTopicPolicy': 'AWS::SNS::TopicPolicy', 'BatchFailureRule': 'AWS::Events::Rule',
            'SilverFailureAlarm': 'AWS::CloudWatch::Alarm', 'GoldFailureAlarm': 'AWS::CloudWatch::Alarm',
        }
        self.assertEqual({name: item['Type'] for name, item in self.template['Resources'].items()}, expected)
        for resource in self.template['Resources'].values():
            self.assertEqual(resource['DeletionPolicy'], 'Retain')
            self.assertEqual(resource['UpdateReplacePolicy'], 'Retain')
        self.assertIn('PROPOSTO, NAO IMPLANTADO', self.template['Description'])


if __name__ == '__main__':
    unittest.main()
