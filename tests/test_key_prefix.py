"""Tests for RQ_KEY_PREFIX environment variable support."""

import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from redis import Redis

from rq import Queue
from rq.cron_scheduler_registry import get_registry_key
from rq.defaults import RQ_KEY_PREFIX
from rq.results import Result
from rq.worker_registration import REDIS_WORKER_KEYS, WORKERS_BY_QUEUE_KEY


class TestKeyPrefix(unittest.TestCase):
    def test_default_prefix_is_rq(self):
        self.assertEqual(RQ_KEY_PREFIX, 'rq')

    def test_worker_registration_constants_use_default_prefix(self):
        self.assertEqual(REDIS_WORKER_KEYS, 'rq:workers')
        self.assertEqual(WORKERS_BY_QUEUE_KEY, 'rq:workers:%s')

    def test_results_key_uses_prefix(self):
        with patch('rq.results.RQ_KEY_PREFIX', 'myapp'):
            self.assertEqual(Result.get_key('abc123'), 'myapp:results:abc123')

    def test_queue_registry_cleaning_key_uses_prefix(self):
        queue = Queue('default', connection=Redis())
        with patch('rq.queue.RQ_KEY_PREFIX', 'myapp'):
            self.assertEqual(queue.registry_cleaning_key, 'myapp:clean_registries:default')

    def test_cron_scheduler_registry_key_uses_prefix(self):
        with patch('rq.cron_scheduler_registry.RQ_KEY_PREFIX', 'myapp'):
            self.assertEqual(get_registry_key(), 'myapp:cron_schedulers')

    def test_hash_tag_prefix_builds_keys(self):
        """A Redis Cluster hash tag prefix like `{rq}` must not be read as a format placeholder."""
        script = (
            'from rq.executions import Execution, ExecutionRegistry\n'
            'from rq.registry import FailedJobRegistry, StartedJobRegistry\n'
            'print(StartedJobRegistry("default", connection=None).key)\n'
            'print(FailedJobRegistry("default", connection=None).key)\n'
            'print(ExecutionRegistry("job1", connection=None).key)\n'
            'print(Execution("e1", "job1", connection=None, worker_name="w1").worker_executions_key)\n'
        )
        env = {**os.environ, 'RQ_KEY_PREFIX': '{rq}'}
        output = subprocess.run([sys.executable, '-c', script], env=env, capture_output=True, text=True, check=True)
        self.assertEqual(
            output.stdout.splitlines(),
            ['{rq}:wip:default', '{rq}:failed:default', '{rq}:executions:job1', '{rq}:worker:w1:executions'],
        )

    def test_invalid_prefix_rejected(self):
        """Prefixes with characters unsafe in %-templates or Lua source fail at import."""
        output = subprocess.run(
            [sys.executable, '-c', 'import rq'],
            env={**os.environ, 'RQ_KEY_PREFIX': 'app%s'},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(output.returncode, 0)
        self.assertIn('Invalid RQ_KEY_PREFIX', output.stderr)

        output = subprocess.run(
            [sys.executable, '-c', 'import rq'],
            env={**os.environ, 'RQ_KEY_PREFIX': 'team"x'},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(output.returncode, 0)
        self.assertIn('Invalid RQ_KEY_PREFIX', output.stderr)

        output = subprocess.run(
            [sys.executable, '-c', 'import rq'],
            env={**os.environ, 'RQ_KEY_PREFIX': 'rq\n'},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(output.returncode, 0)
        self.assertIn('Invalid RQ_KEY_PREFIX', output.stderr)

        output = subprocess.run(
            [sys.executable, '-c', 'import rq'], env={**os.environ, 'RQ_KEY_PREFIX': ''}, capture_output=True, text=True
        )
        self.assertNotEqual(output.returncode, 0)
        self.assertIn('Invalid RQ_KEY_PREFIX', output.stderr)
