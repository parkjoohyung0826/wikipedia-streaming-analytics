import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('week6_report',Path(__file__).parents[1]/'performance/week6_report.py')
report=importlib.util.module_from_spec(spec);spec.loader.exec_module(report)

class ReportTests(unittest.TestCase):
    def test_trend_uses_elapsed_time(self):
        self.assertEqual(report.slope([{'at':100,'lag':10},{'at':110,'lag':30},{'at':130,'lag':70}]),2)
        self.assertIsNone(report.slope([{'at':100,'lag':0}]))
    def test_incomplete_run_does_not_claim_success(self):
        with tempfile.TemporaryDirectory() as directory:
            result=report.summarize(Path(directory))
        self.assertFalse(result['execution']['success'])
        self.assertIsNone(result['load_verification'])
        self.assertIsNone(result['recovery'])
    def test_warmup_excluded_from_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);load=root/'load';load.mkdir()
            (load/'producer.jsonl').write_text(json.dumps({'stage':'warmup'})+'\n')
            result=report.summarize(root)
        self.assertEqual(result['stages'],[])
        self.assertEqual(result['spark_progress'],{})
