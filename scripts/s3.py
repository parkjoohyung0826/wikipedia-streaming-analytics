"""Run a bounded collection and publish a verified batch to private S3; no Docker."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import secrets
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from src.s3_batch import Aws,prepare,publish,dump
CONFIG = ROOT/'config/s3.local.json'


def setup(region,profile):
    aws = Aws(region,profile)
    identity = aws.call('sts','get-caller-identity')
    if CONFIG.exists():
        config = json.loads(CONFIG.read_text())
        if config['account_id'] != identity['Account']:
            raise ValueError('Configured bucket belongs to a different AWS account')
        print('Using existing configuration: '+str(CONFIG))
        return
    bucket = 'wikipedia-batch-'+identity['Account']+'-'+secrets.token_hex(4)
    args = ['s3api','create-bucket','--bucket',bucket,'--object-ownership','BucketOwnerEnforced']
    if region != 'us-east-1':
        args += ['--create-bucket-configuration',json.dumps({'LocationConstraint':region})]
    aws.call(*args)
    # Save immediately so interrupted setup does not create another bucket on retry.
    config = dict(bucket=bucket,account_id=identity['Account'],region=region,profile=profile,prefix='wikipedia')
    CONFIG.parent.mkdir(parents=True,exist_ok=True);dump(CONFIG,config)
    configure(aws,config)
    print(json.dumps(dict(bucket=bucket,region=region,configuration=str(CONFIG)),indent=2))


def configure(aws,config):
    bucket=config['bucket']
    aws.call('s3api','put-public-access-block','--bucket',bucket,'--public-access-block-configuration',json.dumps(dict(BlockPublicAcls=True,IgnorePublicAcls=True,BlockPublicPolicy=True,RestrictPublicBuckets=True)))
    aws.call('s3api','put-bucket-encryption','--bucket',bucket,'--server-side-encryption-configuration',json.dumps({'Rules':[{'ApplyServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'}}]}))
    aws.call('s3api','put-bucket-versioning','--bucket',bucket,'--versioning-configuration','Status=Enabled')
    policy={'Version':'2012-10-17','Statement':[{'Sid':'DenyInsecureTransport','Effect':'Deny','Principal':'*','Action':'s3:*','Resource':['arn:aws:s3:::'+bucket,'arn:aws:s3:::'+bucket+'/*'],'Condition':{'Bool':{'aws:SecureTransport':'false'}}}]}
    aws.call('s3api','put-bucket-policy','--bucket',bucket,'--policy',json.dumps(policy))
    aws.call('s3api','put-bucket-tagging','--bucket',bucket,'--tagging',json.dumps({'TagSet':[{'Key':'Project','Value':'wikipedia-streaming-analytics'},{'Key':'Purpose','Value':'on-demand-batch'}]}))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['setup','run','upload','status'])
    p.add_argument('--region',default='ap-northeast-2');p.add_argument('--profile')
    p.add_argument('--seconds',type=int,default=600)
    p.add_argument('--input',type=Path);p.add_argument('--keep-local',action='store_true')
    a=p.parse_args()
    if a.seconds<1 or a.seconds>3600:p.error('seconds must be between 1 and 3600')
    if a.action=='setup':
        if CONFIG.exists():
            config=json.loads(CONFIG.read_text());aws=Aws(config['region'],config.get('profile'))
            if aws.call('sts','get-caller-identity')['Account']!=config['account_id']:raise ValueError('AWS account mismatch')
            configure(aws,config);print('Existing bucket settings verified')
        else:setup(a.region,a.profile)
        return
    if not CONFIG.exists():p.error('Run setup first')
    config=json.loads(CONFIG.read_text());aws=Aws(config['region'],config.get('profile'))
    if aws.call('sts','get-caller-identity')['Account']!=config['account_id']:raise ValueError('AWS account mismatch')
    if a.action=='status':
        print(json.dumps(aws.call('s3api','list-objects-v2','--bucket',config['bucket'],'--expected-bucket-owner',config['account_id'],'--prefix',config['prefix']+'/batches/'),indent=2));return
    if a.action=='upload' and (not a.input or not a.input.is_file()):p.error('upload requires --input JSONL')
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'_'+secrets.token_hex(3)
    work=ROOT/'artifacts/s3'/run_id;work.mkdir(parents=True)
    source=a.input.resolve() if a.input else None
    try:
        if a.action=='run':
            with (work/'collection.log').open('w') as log:
                subprocess.run([sys.executable,'-m','src.collect_recentchange','--seconds',str(a.seconds),'--output',str(work/'collection')],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True)
            source=next((work/'collection').glob('*/events.jsonl'))
        summary=prepare(source,work/'prepared')
        result=publish(aws,config,work/'prepared',summary)
        if a.action=='run':
            # Keep collection timing/coverage separately from deterministic batch metadata.
            result['collection']=json.loads(source.with_name('summary.json').read_text())
        result['verified_at']=datetime.now(timezone.utc).isoformat()
        evidence=ROOT/'evidence/s3'/f'{run_id}.json';evidence.parent.mkdir(parents=True,exist_ok=True)
        dump(evidence,result)
        print(json.dumps(dict(manifest=result['manifest'],counts=result['counts'],groups=result['groups'],evidence=str(evidence)),ensure_ascii=False,indent=2))
        if not a.keep_local:shutil.rmtree(work)
    except Exception:
        print('Failed: local recovery files retained at '+str(work),file=sys.stderr)
        raise

if __name__=='__main__':main()
