# -*- coding: utf-8 -*-
from __future__ import absolute_import

from rq.job import Job
from rq.queue import FailedQueue, Queue
from rq.utils import current_timestamp
from rq.worker import Worker
from rq.working_queue import WorkingQueue

from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class TestQueue(RQTestCase):

    def setUp(self):
        super(TestQueue, self).setUp()
        self.working_queue = WorkingQueue(connection=self.testconn)

    def test_add_and_remove(self):
        """Adding and removing job to WorkingQueue."""
        timestamp = current_timestamp()
        job = Job()

        # Test that job is added with the right score
        self.working_queue.add(job, 1000)
        self.assertLess(self.testconn.zscore(self.working_queue.key, job.id),
                        timestamp + 1001)

        # Ensure that job is properly removed from sorted set
        self.working_queue.remove(job)
        self.assertIsNone(self.testconn.zscore(self.working_queue.key, job.id))

    def test_get_job_ids(self):
        """Getting job ids from WorkingQueue."""
        self.testconn.zadd(self.working_queue.key, 1, 'foo')
        self.testconn.zadd(self.working_queue.key, 10, 'bar')
        self.assertEqual(self.working_queue.get_job_ids(), ['foo', 'bar'])

    def test_get_expired_job_ids(self):
        """Getting expired job ids form WorkingQueue."""
        timestamp = current_timestamp()

        self.testconn.zadd(self.working_queue.key, 1, 'foo')
        self.testconn.zadd(self.working_queue.key, timestamp + 10, 'bar')

        self.assertEqual(self.working_queue.get_expired_job_ids(), ['foo'])

    def test_cleanup(self):
        """Moving expired jobs to FailedQueue."""
        failed_queue = FailedQueue(connection=self.testconn)
        self.assertTrue(failed_queue.is_empty())
        self.testconn.zadd(self.working_queue.key, 1, 'foo')
        self.working_queue.cleanup()
        self.assertIn('foo', failed_queue.job_ids)

    def test_job_execution(self):
        """Job is removed from WorkingQueue after execution."""
        working_queue = WorkingQueue(connection=self.testconn)
        queue = Queue(connection=self.testconn)
        worker = Worker([queue])

        job = queue.enqueue(say_hello)

        worker.prepare_job_execution(job)
        self.assertIn(job.id, working_queue.get_job_ids())

        worker.perform_job(job)
        self.assertNotIn(job.id, working_queue.get_job_ids())

        # Job that fails
        job = queue.enqueue(div_by_zero)

        worker.prepare_job_execution(job)
        self.assertIn(job.id, working_queue.get_job_ids())

        worker.perform_job(job)
        self.assertNotIn(job.id, working_queue.get_job_ids())
