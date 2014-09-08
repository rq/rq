# -*- coding: utf-8 -*-
from __future__ import absolute_import

from rq.job import Job
from rq.queue import FailedQueue, Queue
from rq.utils import current_timestamp
from rq.worker import Worker
from rq.registry import StartedJobRegistry

from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class TestQueue(RQTestCase):

    def setUp(self):
        super(TestQueue, self).setUp()
        self.registry = StartedJobRegistry(connection=self.testconn)

    def test_add_and_remove(self):
        """Adding and removing job to StartedJobRegistry."""
        timestamp = current_timestamp()
        job = Job()

        # Test that job is added with the right score
        self.registry.add(job, 1000)
        self.assertLess(self.testconn.zscore(self.registry.key, job.id),
                        timestamp + 1002)

        # Ensure that job is properly removed from sorted set
        self.registry.remove(job)
        self.assertIsNone(self.testconn.zscore(self.registry.key, job.id))

    def test_get_job_ids(self):
        """Getting job ids from StartedJobRegistry."""
        self.testconn.zadd(self.registry.key, 1, 'foo')
        self.testconn.zadd(self.registry.key, 10, 'bar')
        self.assertEqual(self.registry.get_job_ids(), ['foo', 'bar'])

    def test_get_expired_job_ids(self):
        """Getting expired job ids form StartedJobRegistry."""
        timestamp = current_timestamp()

        self.testconn.zadd(self.registry.key, 1, 'foo')
        self.testconn.zadd(self.registry.key, timestamp + 10, 'bar')

        self.assertEqual(self.registry.get_expired_job_ids(), ['foo'])

    def test_cleanup(self):
        """Moving expired jobs to FailedQueue."""
        failed_queue = FailedQueue(connection=self.testconn)
        self.assertTrue(failed_queue.is_empty())
        self.testconn.zadd(self.registry.key, 1, 'foo')
        self.registry.cleanup()
        self.assertIn('foo', failed_queue.job_ids)

    def test_job_execution(self):
        """Job is removed from StartedJobRegistry after execution."""
        registry = StartedJobRegistry(connection=self.testconn)
        queue = Queue(connection=self.testconn)
        worker = Worker([queue])

        job = queue.enqueue(say_hello)

        worker.prepare_job_execution(job)
        self.assertIn(job.id, registry.get_job_ids())

        worker.perform_job(job)
        self.assertNotIn(job.id, registry.get_job_ids())

        # Job that fails
        job = queue.enqueue(div_by_zero)

        worker.prepare_job_execution(job)
        self.assertIn(job.id, registry.get_job_ids())

        worker.perform_job(job)
        self.assertNotIn(job.id, registry.get_job_ids())
