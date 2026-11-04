"""Run isolated S3 step load and fault tests without Docker."""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import secrets
import shutil
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from src.s3_load import run

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rates',default='100,300,1000,3000');p.add_argument('--seconds',type=int,default=180)
    p.add_argument('--chunk-size',type=int,default=20000);p.add_argument('--flush-seconds',type=int,default=10)
    a=p.parse_args();rates=[int(x) for x in a.rates.split(',')]
    if min([a.seconds,a.chunk_size,a.flush_seconds]+rates)<1:p.error('parameters must be positive')
    config=json.loads((ROOT/'config/s3.local.json').read_text())
    ident=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'_'+secrets.token_hex(3)
    root=ROOT/'artifacts/week6_s3'/ident
    import os
    os.chdir(ROOT)
    evidence=run(root,config,rates,a.seconds,a.chunk_size,a.flush_seconds)
    target=ROOT/'evidence/week6_s3'/ident;target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copytree(evidence,target)
    for source in evidence.rglob('*'):
        if source.is_file() and source.read_bytes()!=(target/source.relative_to(evidence)).read_bytes():
            raise RuntimeError('Evidence copy differs; local files retained')
    # run() returns only after every S3 batch is verified; failure spools remain untouched.
    shutil.rmtree(root)
    print('PASS '+str(target),flush=True)

if __name__=='__main__':main()
