"""Archive only named legacy generated directories, verify S3 roundtrip, then optionally prune."""
import argparse
import base64
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import secrets
import shutil
import sys
import tarfile
import tempfile
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from src.s3_batch import Aws,digest,dump
TARGETS=['data','artifacts/live','artifacts/runs','artifacts/week4','artifacts/week5','artifacts/week6']


def inventory():
    result={}
    for name in TARGETS:
        base=ROOT/name
        if base.is_symlink():raise ValueError('Refusing symlink root: '+str(base))
        if not base.exists():continue
        for path in sorted(base.rglob('*')):
            if path.is_symlink():raise ValueError('Refusing symlink: '+str(path))
            if path.is_file():
                if path.name.startswith('.env') or path.name in ('credentials','config.json'):
                    raise ValueError('Review potentially sensitive file before archiving: '+str(path))
                result[str(path.relative_to(ROOT))]=dict(bytes=path.stat().st_size,sha256=digest(path))
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--prune',action='store_true');a=p.parse_args()
    config=json.loads((ROOT/'config/s3.local.json').read_text());aws=Aws(config['region'],config.get('profile'))
    if aws.call('sts','get-caller-identity')['Account']!=config['account_id']:raise ValueError('AWS account mismatch')
    files=inventory()
    if not files:print('No legacy generated files found');return
    ident=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'_'+secrets.token_hex(3)
    key='archives/legacy-generated/'+ident+'.tar.gz'
    with tempfile.TemporaryDirectory(prefix='wiki-archive-') as tmp:
        archive=Path(tmp)/'archive.tar.gz'
        with tarfile.open(archive,'w:gz') as tar:
            for name in files:tar.add(ROOT/name,arcname=name,recursive=False)
        sha=digest(archive);checksum=base64.b64encode(bytes.fromhex(sha)).decode()
        aws.call('s3api','put-object','--bucket',config['bucket'],'--key',key,'--body',str(archive),
                 '--expected-bucket-owner',config['account_id'],'--if-none-match','*','--server-side-encryption','AES256',
                 '--checksum-algorithm','SHA256','--checksum-sha256',checksum)
        downloaded=Path(tmp)/'downloaded.tar.gz'
        aws.call('s3api','get-object','--bucket',config['bucket'],'--key',key,'--expected-bucket-owner',config['account_id'],str(downloaded))
        if digest(downloaded)!=sha:raise ValueError('S3 roundtrip checksum mismatch; keeping local files')
        restored={}
        with tarfile.open(downloaded,'r:gz') as tar:
            for member in tar:
                if not member.isfile():raise ValueError('Unexpected archive member')
                stream=tar.extractfile(member);h=hashlib.sha256()
                for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
                restored[member.name]=dict(bytes=member.size,sha256=h.hexdigest())
        if restored!=files:raise ValueError('Archive file inventory differs; keeping local files')
        if inventory()!=files:raise ValueError('Sources changed during backup; keeping local files')
        report=dict(verified=True,s3_uri='s3://'+config['bucket']+'/'+key,archive_sha256=sha,
                    archive_bytes=archive.stat().st_size,source_bytes=sum(x['bytes'] for x in files.values()),
                    file_count=len(files),targets=TARGETS,files=files,pruned=False)
        destination=ROOT/'evidence/s3_archives'/f'{ident}.json';destination.parent.mkdir(parents=True,exist_ok=True)
        dump(destination,report)
        # Keep inventory beside the archive so recovery does not depend on the local repository.
        aws.call('s3api','put-object','--bucket',config['bucket'],'--key',key+'.inventory.json','--body',str(destination),
                 '--expected-bucket-owner',config['account_id'],'--if-none-match','*','--server-side-encryption','AES256')
        if a.prune:
            for name in TARGETS:
                path=ROOT/name
                if path.exists():shutil.rmtree(path)
            report['pruned']=True;dump(destination,report)
        print(json.dumps({k:v for k,v in report.items() if k!='files'},indent=2))

if __name__=='__main__':main()
