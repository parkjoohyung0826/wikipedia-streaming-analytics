import base64
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from src.s3_batch import prepare,publish


def event(identity='one'):
    return dict(meta={'id':identity},type='edit',server_name='ko.wikipedia.org',wiki='kowiki',
                title='문서',timestamp=1793545200,bot=False,length={'old':10,'new':15})


class MemoryS3:
    def __init__(self,fail_name=None):
        self.objects={};self.puts=[];self.fail_name=fail_name

    def call(self,*args,missing_ok=False):
        key=args[args.index('--key')+1]
        if args[1]=='head-object':return self.objects.get(key)
        if self.fail_name and key.endswith(self.fail_name):raise RuntimeError('simulated upload failure')
        path=Path(args[args.index('--body')+1]);data=path.read_bytes()
        self.objects[key]={'ContentLength':len(data),'ChecksumSHA256':base64.b64encode(hashlib.sha256(data).digest()).decode()}
        self.puts.append(key)
        return {}


class S3BatchTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.source=self.root/'input.jsonl'
        self.source.write_text('\n'.join([json.dumps(event()),json.dumps(event()),json.dumps({**event(),'type':'new'}),'{broken'])+'\n')
        self.config={'bucket':'test','account_id':'123','prefix':'wikipedia'}

    def test_counts_timezone_and_raw_preservation(self):
        summary=prepare(self.source,self.root/'out')
        self.assertEqual(summary['counts'],dict(input=4,selected=1,duplicates=1,filtered=1,rejected=1))
        self.assertEqual(gzip.decompress((self.root/'out/raw.jsonl.gz').read_bytes()),self.source.read_bytes())
        row=json.loads((self.root/'out/daily_wiki.json').read_text())[0]
        self.assertEqual(row['target_date'],'2026-11-02')
        self.assertEqual(row['net_bytes_delta'],5)

    def test_replay_is_immutable_and_manifest_last(self):
        aws=MemoryS3()
        summary=prepare(self.source,self.root/'a');publish(aws,self.config,self.root/'a',summary)
        self.assertEqual(len(aws.puts),6)
        self.assertTrue(aws.puts[-1].endswith('manifest.json'))
        summary2=prepare(self.source,self.root/'b');publish(aws,self.config,self.root/'b',summary2)
        self.assertEqual(len(aws.puts),6)

    def test_partial_upload_cannot_publish(self):
        aws=MemoryS3('edits.jsonl.gz');summary=prepare(self.source,self.root/'a')
        with self.assertRaises(RuntimeError):publish(aws,self.config,self.root/'a',summary)
        self.assertFalse(any(k.endswith('manifest.json') for k in aws.objects))

    def test_changed_remote_object_is_not_overwritten(self):
        aws=MemoryS3();summary=prepare(self.source,self.root/'a');publish(aws,self.config,self.root/'a',summary)
        key=aws.puts[0];aws.objects[key]['ChecksumSHA256']='wrong'
        with self.assertRaises(ValueError):publish(aws,self.config,self.root/'a',summary)
        self.assertEqual(len(aws.puts),6)

    def test_no_selected_events_fails_before_publication(self):
        self.source.write_text('{broken\n')
        with self.assertRaises(ValueError):prepare(self.source,self.root/'out')

if __name__=='__main__':unittest.main()
