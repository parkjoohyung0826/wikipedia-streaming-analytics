import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('week6_run',Path(__file__).parents[1]/'performance/week6_run.py')
runner=importlib.util.module_from_spec(spec)
try:spec.loader.exec_module(runner)
except ModuleNotFoundError:runner=None

@unittest.skipIf(runner is None,'Install tests/performance/requirements.txt for runner tests')
class RunnerTests(unittest.TestCase):
    def test_committed_ignores_partial_append_and_duplicate_batch(self):
        record={'batch_id':0,'rows':4,'offsets':[{'partition':0,'end_offset':4}]}
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'a.metrics.jsonl').write_text(json.dumps(record)+'\n{"batch_id":')
            (root/'b.metrics.jsonl').write_text(json.dumps(record)+'\n')
            rows,offsets=runner.committed(root)
        self.assertEqual(rows,4);self.assertEqual(offsets[0],4)
    def test_pacing_never_polls_negative_timeout(self):
        class Producer:
            def __init__(self,*a):pass
            def poll(self,timeout):
                if timeout<0:raise AssertionError('Negative timeout blocks indefinitely')
            def produce(self,*a,**kw):kw['on_delivery'](None,None)
            def flush(self,*a):return 0
        runner.RUN='test';runner.BOOTSTRAP='unused'
        with tempfile.TemporaryDirectory() as directory,patch.object(runner,'Producer',Producer),patch.object(runner.time,'monotonic',side_effect=[0,0,0.0099,0.0101,0.02]):
            self.assertEqual(runner.produce(Path(directory),'t','pace',100,count=2),2)
