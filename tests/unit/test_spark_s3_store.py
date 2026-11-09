"""S3 publication contracts without AWS credentials or Spark runtime."""
import io
import json
import unittest
from src.spark_s3 import Store,retryable,await_query

class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Fake:
    def __init__(self):self.objects={};self.error=None;self.calls=[]
    def get_object(self,**kw):
        if self.error:raise Error(self.error)
        if kw['Key'] not in self.objects:raise Error('NoSuchKey')
        return {'Body':io.BytesIO(self.objects[kw['Key']])}
    def put_object(self,**kw):
        if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.objects:raise Error('PreconditionFailed')
        self.calls.append(kw);self.objects[kw['Key']]=kw['Body']

class StoreTests(unittest.TestCase):
    def setUp(self):
        self.client=Fake();self.store=Store({'bucket':'test','prefix':'wiki','region':'ap-northeast-2'},self.client)
    def test_missing_is_distinct_from_denied(self):
        self.assertIsNone(self.store.get('missing'))
        self.client.error='AccessDenied'
        with self.assertRaises(Error):self.store.get('missing')
    def test_batch_commit_is_immutable(self):
        self.store.put('commit.json',{'path':'complete'},immutable=True)
        with self.assertRaises(Error):self.store.put('commit.json',{'path':'partial'},immutable=True)
        self.assertEqual(self.store.get('commit.json'),{'path':'complete'})
    def test_daily_publication_replaces_reference(self):
        self.store.put('current.json',{'path':'old'})
        self.store.put('current.json',{'path':'validated-new'})
        self.assertEqual(self.store.get('current.json'),{'path':'validated-new'})
        self.assertEqual(self.client.calls[-1]['ServerSideEncryption'],'AES256')
    def test_separate_namespace(self):
        self.assertEqual(self.store.uri('stream/checkpoint'),'s3a://test/wiki/spark-v1/stream/checkpoint')

class RetryTests(unittest.TestCase):
    def test_transient_versus_permanent(self):
        self.assertTrue(retryable(Error('SlowDown')))
        self.assertFalse(retryable(Error('AccessDenied')))
        self.assertFalse(retryable(ValueError('Invalid data')))

class StreamRetryTests(unittest.TestCase):
    def test_wrapped_callback_preserves_retryable_cause(self):
        cause=Error('ServiceUnavailable')
        class Query:
            def awaitTermination(self,*args):raise RuntimeError('PySpark wrapper')
        with self.assertRaises(Error) as raised:await_query(Query(),None,[cause])
        self.assertIs(raised.exception,cause)
        self.assertTrue(retryable(raised.exception))
    def test_unrelated_query_error_is_not_marked_transient(self):
        class Query:
            def awaitTermination(self,*args):raise ValueError('Kafka configuration invalid')
        with self.assertRaises(ValueError):await_query(Query(),None,[])
