"""Tests for RQ_KEY_PREFIX environment variable support."""
from unittest.mock import patch

from rq import Queue
from rq.cron_scheduler_registry import get_registry_key
from rq.defaults import RQ_KEY_PREFIX
from rq.results import get_key as get_result_key
from rq.worker_registration import REDIS_WORKER_KEYS, WORKERS_BY_QUEUE_KEY
from tests import RQTestCase


class TestKeyPrefix(RQTestCase):
    def test_default_prefix_is_rq(self):
        self.assertEqual(RQ_KEY_PREFIX, 'rq')

    def test_worker_registration_constants_use_default_prefix(self):
        self.assertEqual(REDIS_WORKER_KEYS, 'rq:workers')
        self.assertEqual(WORKERS_BY_QUEUE_KEY, 'rq:workers:%s')

    def test_results_key_uses_prefix(self):
        with patch('rq.results.RQ_KEY_PREFIX', 'myapp'):
            self.assertEqual(get_result_key('abc123'), 'myapp:results:abc123')

    def test_queue_registry_cleaning_key_uses_prefix(self):
        q = Queue('default', connection=self.connection)
        with patch('rq.queue.RQ_KEY_PREFIX', 'myapp'):
            self.assertEqual(q.registry_cleaning_key, 'myapp:clean_registries:default')

    def test_cron_scheduler_registry_key_uses_prefix(self):
        with patch('rq.cron_scheduler_registry.RQ_KEY_PREFIX', 'myapp'):
            self.assertEqual(get_registry_key(), 'myapp:cron_schedulers')
