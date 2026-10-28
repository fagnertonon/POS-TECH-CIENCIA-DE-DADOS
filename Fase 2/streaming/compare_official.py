"""Compare saved preflight evidence; no AWS calls or object writes."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path


def compare(before, after):
    reports = [json.loads((folder/'preflight_report.json').read_text(encoding='utf-8')) for folder in (before,after)]
    errors, results = [], {}
    expected = {'bronze/inep/','silver/inep/','gold/inep/','auditoria/bronze/', 'auditoria/silver/','auditoria/gold/'}
    for report in reports:
        if report['validation_status'] != 'SUCCEEDED' or set(report['official_inventory']) != expected:
            errors.append('Preflight is incomplete or not successful')
    for prefix in sorted(expected):
        a,b = [r['official_inventory'].get(prefix,{}) for r in reports]
        passed = a.get('complete') is True and b.get('complete') is True and all(
            a.get(k) == b.get(k) for k in ('sha256','object_count','total_bytes'))
        results[prefix] = {'unchanged':passed,'before':a,'after':b}
        if not passed:
            errors.append('Official inventory differs: '+prefix)
    policies = [json.loads((folder/'role_inline_policy.json').read_text(encoding='utf-8'))['PolicyDocument'] for folder in (before,after)]
    indexed = [{s['Sid']:s for s in policy['Statement']} for policy in policies]
    sid = 'PersistOnlyStreamingObjects'
    for statements in indexed:
        if sid not in statements:
            raise ValueError('Streaming S3 policy statement missing')
    common = lambda statement: {k:v for k,v in statement.items() if k != 'Resource'}
    s3_changes_valid = common(indexed[0][sid]) == common(indexed[1][sid]) and sorted(indexed[1][sid]['Action']) == ['s3:GetObject','s3:PutObject']
    arn = 'arn:aws:s3:::fiap-tc-fase2-2026/'
    original = {arn+p for p in ('bronze/inep_streaming/*','auditoria/streaming/idempotency/*')}
    additions = {arn+p for p in ('silver/inep_streaming/entity=alunos/*','gold/inep_streaming/contribuicoes/*','auditoria/streaming/processed/*')}
    s3_changes_valid &= set(indexed[0][sid]['Resource']) == original and set(indexed[1][sid]['Resource']) == original|additions
    other_statements_unchanged = {k:v for k,v in indexed[0].items() if k != sid} == {k:v for k,v in indexed[1].items() if k != sid}
    if not s3_changes_valid or not other_statements_unchanged:
        errors.append('Inline IAM change differs from the three authorized S3 additions')
    return {'status':'SUCCEEDED' if not errors else 'FAILED','compared_at_utc':datetime.now(timezone.utc).isoformat(),
            'scope':'Saved metadata inventories, not a reconstruction of transient writes; inline role policy only',
            'official_prefixes':results,'authorized_iam_additions_only':bool(s3_changes_valid and other_statements_unchanged),
            'errors':errors}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--before',type=Path,required=True)
    p.add_argument('--after',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    report=compare(args.before,args.after)
    args.output.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print(json.dumps({'status':report['status'],'official_prefixes_unchanged':sum(v['unchanged'] for v in report['official_prefixes'].values()),
                      'authorized_iam_additions_only':report['authorized_iam_additions_only'],'errors':report['errors']}))
    raise SystemExit(0 if report['status']=='SUCCEEDED' else 1)


if __name__=='__main__':
    main()
