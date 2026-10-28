"""Offline acceptance of native migration scope and rejection of dangerous plans."""
import copy,json,shutil,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
import migrate_existing_alerts as migration


class MigrationGuards(unittest.TestCase):
    def setUp(self):
        self.plan=json.loads((migration.BASE/'migration_plan.json').read_bytes())
        self.baseline=json.loads((migration.BASE/'before.json').read_bytes())
    def native(self,phase):
        if phase=='import':
            changes=[{'Action':'Import','LogicalResourceId':x['LogicalResourceId'],'ResourceType':x['ResourceType']}
                     for x in self.plan['import_existing_resources']]
        else:changes=[{'Action':'Modify','Replacement':'False','LogicalResourceId':name,
                       'ResourceType':'AWS::CloudWatch::Alarm'} for name in sorted(migration.UPDATE_IDS)]
        return {'Status':'CREATE_COMPLETE','ExecutionStatus':'AVAILABLE',
                'Changes':[{'ResourceChange':x} for x in changes]}

    def test_only_all_six_existing_resources_are_accepted_for_import(self):
        template=json.loads((migration.BASE/'import_template.json').read_bytes())
        self.assertNotIn('Outputs',template,'IMPORT cannot add Outputs before adoption completes')
        result=migration.validate_changes(self.native('import'),'import',self.plan)
        self.assertEqual(len(result),6)
        self.assertTrue(all(x['Action']=='Import' for x in result))

    def test_import_rejects_create_replace_incomplete_or_missing_subscription(self):
        for mutation in ['Add','Remove','Modify','Replace','Missing','Paged']:
            with self.subTest(mutation=mutation):
                review=self.native('import')
                if mutation=='Missing':review['Changes']=[x for x in review['Changes'] if x['ResourceChange']['LogicalResourceId']!='AlertSubscription']
                elif mutation=='Replace':review['Changes'][0]['ResourceChange']['Replacement']='True'
                elif mutation=='Paged':review['NextToken']='more'
                else:review['Changes'][0]['ResourceChange']['Action']=mutation
                with self.assertRaises(ValueError):migration.validate_changes(review,'import',self.plan)

    def test_update_accepts_only_three_in_place_alarm_changes(self):
        self.assertEqual(len(migration.validate_changes(self.native('update'),'update',self.plan)),3)
        for mutation in ['Role','Queue','Replace','Conditional','Add','WrongType','Missing']:
            with self.subTest(mutation=mutation):
                review=self.native('update');first=review['Changes'][0]['ResourceChange']
                if mutation=='Role':first['LogicalResourceId']='ConsumerRole';first['ResourceType']='AWS::IAM::Role'
                elif mutation=='Queue':first['LogicalResourceId']='SourceQueue';first['ResourceType']='AWS::SQS::Queue'
                elif mutation=='Replace':first['Replacement']='True'
                elif mutation=='Conditional':first['Replacement']='Conditional'
                elif mutation=='Add':first['Action']='Add'
                elif mutation=='WrongType':first['ResourceType']='AWS::Lambda::Function'
                else:review['Changes'].pop()
                with self.assertRaises(ValueError):migration.validate_changes(review,'update',self.plan)

    def test_unready_or_unavailable_changesets_cannot_execute(self):
        for status,execution in [('CREATE_IN_PROGRESS','UNAVAILABLE'),('FAILED','UNAVAILABLE'),('CREATE_COMPLETE','EXECUTE_COMPLETE')]:
            review=self.native('import');review.update(Status=status,ExecutionStatus=execution)
            with self.assertRaises(ValueError):migration.validate_changes(review,'import',self.plan)

    def test_export_encoding_repair_never_authorizes_a_lambda_code_update(self):
        template=json.loads((migration.BASE/'streaming_update_template.json').read_bytes())
        proposed=template['Resources']['ConsumerFunction']['Properties']['Code']['ZipFile']
        exported=self.baseline['streaming_template']['TemplateBody']['Resources']['ConsumerFunction']['Properties']['Code']['ZipFile']
        self.assertEqual(proposed.encode('ascii',errors='replace').decode('ascii'),exported)
        proof=json.loads((migration.BASE/'lambda_source_identity.json').read_bytes())
        self.assertEqual(migration.sha(proposed.encode()),proof['source_sha256'])
        self.assertEqual(proof['installed_code_zip_sha256_base64'],self.baseline['lambda_config']['CodeSha256'])
        review=self.native('update')
        review['Changes'].append({'ResourceChange':{'Action':'Modify','Replacement':'False',
          'LogicalResourceId':'ConsumerFunction','ResourceType':'AWS::Lambda::Function'}})
        with self.assertRaises(ValueError):migration.validate_changes(review,'update',self.plan)

    def test_file_drift_invalidates_the_reviewed_plan(self):
        self.assertEqual(migration.verify_files()['account'],migration.ACCOUNT)
        with tempfile.TemporaryDirectory() as folder:
            base=Path(folder)
            for name in ['migration_plan.json','before.json',*self.plan['template_hashes']]:
                shutil.copyfile(migration.BASE/name,base/name)
            with (base/'streaming_update_template.json').open('ab') as out:out.write(b' ')
            with self.assertRaisesRegex(ValueError,'Changed migration source'):
                migration.verify_files(base)

    def test_wrong_account_or_changed_sns_policy_stops_before_mutation(self):
        with self.assertRaisesRegex(ValueError,'Wrong AWS account'):
            migration.guard_routing({'sts':SimpleNamespace(get_caller_identity=lambda:{'Account':'000000000000'})},self.baseline)
        topic=copy.deepcopy(self.baseline['topic'])
        doc=json.loads(topic['Attributes']['Policy']);doc['Statement'].append({'Effect':'Allow','Principal':'*','Action':'sns:Publish','Resource':'*'})
        topic['Attributes']['Policy']=json.dumps(doc)
        clients={'sts':SimpleNamespace(get_caller_identity=lambda:{'Account':migration.ACCOUNT}),
                 'sns':SimpleNamespace(get_topic_attributes=lambda **kwargs:topic)}
        with self.assertRaisesRegex(ValueError,'SNS owner/policy changed'):
            migration.guard_routing(clients,self.baseline)


if __name__=='__main__':unittest.main()
