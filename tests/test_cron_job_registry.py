import time

from rq import cron_job_registry
from rq.defaults import DEFAULT_CRON_JOB_HISTORY_TTL
from tests import RQTestCase


class TestCronJobRegistry(RQTestCase):
    """Tests for the cron job registry functions"""

    def test_add(self):
        """add() records a cron job name, directly or through a pipeline"""
        cron_job_registry.add('job-a', self.connection)
        self.assertEqual(cron_job_registry.get_names(self.connection), ['job-a'])

        with self.connection.pipeline() as pipeline:
            cron_job_registry.add('job-b', pipeline)
            pipeline.execute()

        names = cron_job_registry.get_names(self.connection)
        self.assertEqual(len(names), 2)
        self.assertIn('job-b', names)

        # An explicit enqueue timestamp is stored as-is
        explicit_timestamp = time.time() - 30
        cron_job_registry.add('job-c', self.connection, enqueue_timestamp=explicit_timestamp)
        self.assertEqual(self.connection.zscore(cron_job_registry.get_registry_key(), 'job-c'), explicit_timestamp)

    def test_add_same_name_updates_timestamp(self):
        """Adding an existing name refreshes its timestamp instead of raising"""
        cron_job_registry.add('job-a', self.connection, enqueue_timestamp=100.0)
        cron_job_registry.add('job-a', self.connection, enqueue_timestamp=200.0)

        self.assertEqual(self.connection.zcard(cron_job_registry.get_registry_key()), 1)
        self.assertEqual(self.connection.zscore(cron_job_registry.get_registry_key(), 'job-a'), 200.0)

    def test_get_names_orders_by_enqueue_time(self):
        """get_names() returns decoded names ordered oldest enqueue time first"""
        current_time = time.time()
        cron_job_registry.add('newer-job', self.connection, enqueue_timestamp=current_time)
        cron_job_registry.add('older-job', self.connection, enqueue_timestamp=current_time - 60)

        self.assertEqual(cron_job_registry.get_names(self.connection), ['older-job', 'newer-job'])

    def test_get_names_removes_stale_entries(self):
        """get_names() prunes stale entries unless cleanup=False"""
        current_time = time.time()
        stale_timestamp = current_time - DEFAULT_CRON_JOB_HISTORY_TTL - 100
        cron_job_registry.add('stale-job', self.connection, enqueue_timestamp=stale_timestamp)
        cron_job_registry.add('recent-job', self.connection, enqueue_timestamp=current_time)

        self.assertEqual(cron_job_registry.get_names(self.connection), ['recent-job'])
        self.assertIsNone(self.connection.zscore(cron_job_registry.get_registry_key(), 'stale-job'))

        cron_job_registry.add('stale-job', self.connection, enqueue_timestamp=stale_timestamp)
        names = cron_job_registry.get_names(self.connection, cleanup=False)
        self.assertEqual(names, ['stale-job', 'recent-job'])

    def test_remove_stale_entries(self):
        """remove_stale_entries() removes entries older than the threshold and returns the count"""
        current_time = time.time()
        stale_timestamp = current_time - DEFAULT_CRON_JOB_HISTORY_TTL - 100
        cron_job_registry.add('stale-job', self.connection, enqueue_timestamp=stale_timestamp)
        cron_job_registry.add('recent-job', self.connection, enqueue_timestamp=current_time - 60)

        self.assertEqual(cron_job_registry.remove_stale_entries(self.connection), 1)
        self.assertEqual(cron_job_registry.get_names(self.connection), ['recent-job'])

        self.assertEqual(cron_job_registry.remove_stale_entries(self.connection, threshold=30), 1)
        self.assertEqual(cron_job_registry.get_names(self.connection), [])
