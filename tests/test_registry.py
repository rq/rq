# -*- coding: utf-8 -*-
from __future__ import absolute_import

from rq.compat import as_text
from rq.job import Job, JobStatus
from rq.queue import FailedQueue, Queue
from rq.utils import current_timestamp
from rq.worker import Worker
from rq.registry import (clean_registries, DeferredJobRegistry,
                         FinishedJobRegistry, StartedJobRegistry)

from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class CustomJob(Job):
    """A custom job class just to test it"""


class TestRegistry(RQTestCase):

    def setUp(self):
        super(TestRegistry, self).setUp()
        self.registry = StartedJobRegistry(connection=self.testconn)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:wip:default')

    def test_custom_job_class(self):
        registry = StartedJobRegistry(job_class=CustomJob)
        self.assertFalse(registry.job_class == self.registry.job_class)

    def test_add_and_remove(self):
        """Adding and removing job to StartedJobRegistry."""
        timestamp = current_timestamp()
        job = Job()

        # Test that job is added with the right score
        self.registry.add(job, 1000)
        self.assertLess(self.testconn.zscore(self.registry.key, job.id),
                        timestamp + 1002)

        # Ensure that a timeout of -1 results in a score of -1
        self.registry.add(job, -1)
        self.assertEqual(self.testconn.zscore(self.registry.key, job.id), -1)

        # Ensure that job is properly removed from sorted set
        self.registry.remove(job)
        self.assertIsNone(self.testconn.zscore(self.registry.key, job.id))

    def test_get_job_ids(self):
        """Getting job ids from StartedJobRegistry."""
        timestamp = current_timestamp()
        self.testconn.zadd(self.registry.key, timestamp + 10, 'foo')
        self.testconn.zadd(self.registry.key, timestamp + 20, 'bar')
        self.assertEqual(self.registry.get_job_ids(), ['foo', 'bar'])

    def test_get_expired_job_ids(self):
        """Getting expired job ids form StartedJobRegistry."""
        timestamp = current_timestamp()

        self.testconn.zadd(self.registry.key, 1, 'foo')
        self.testconn.zadd(self.registry.key, timestamp + 10, 'bar')
        self.testconn.zadd(self.registry.key, timestamp + 30, 'baz')

        self.assertEqual(self.registry.get_expired_job_ids(), ['foo'])
        self.assertEqual(self.registry.get_expired_job_ids(timestamp + 20),
                         ['foo', 'bar'])

    def test_cleanup(self):
        """Moving expired jobs to FailedQueue."""
        failed_queue = FailedQueue(connection=self.testconn)
        self.assertTrue(failed_queue.is_empty())

        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)

        self.testconn.zadd(self.registry.key, 2, job.id)

        self.registry.cleanup(1)
        self.assertNotIn(job.id, failed_queue.job_ids)
        self.assertEqual(self.testconn.zscore(self.registry.key, job.id), 2)

        self.registry.cleanup()
        self.assertIn(job.id, failed_queue.job_ids)
        self.assertEqual(self.testconn.zscore(self.registry.key, job.id), None)
        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.FAILED)

    def test_job_execution(self):
        """Job is removed from StartedJobRegistry after execution."""
        registry = StartedJobRegistry(connection=self.testconn)
        queue = Queue(connection=self.testconn)
        worker = Worker([queue])

        job = queue.enqueue(say_hello)
        self.assertTrue(job.is_queued)

        worker.prepare_job_execution(job)
        self.assertIn(job.id, registry.get_job_ids())
        self.assertTrue(job.is_started)

        worker.perform_job(job, queue)
        self.assertNotIn(job.id, registry.get_job_ids())
        self.assertTrue(job.is_finished)

        # Job that fails
        job = queue.enqueue(div_by_zero)

        worker.prepare_job_execution(job)
        self.assertIn(job.id, registry.get_job_ids())

        worker.perform_job(job, queue)
        self.assertNotIn(job.id, registry.get_job_ids())

    def test_job_deletion(self):
        """Ensure job is removed from StartedJobRegistry when deleted."""
        registry = StartedJobRegistry(connection=self.testconn)
        queue = Queue(connection=self.testconn)
        worker = Worker([queue])

        job = queue.enqueue(say_hello)
        self.assertTrue(job.is_queued)

        worker.prepare_job_execution(job)
        self.assertIn(job.id, registry.get_job_ids())

        job.delete()
        self.assertNotIn(job.id, registry.get_job_ids())

    def test_get_job_count(self):
        """StartedJobRegistry returns the right number of job count."""
        timestamp = current_timestamp() + 10
        self.testconn.zadd(self.registry.key, timestamp, 'foo')
        self.testconn.zadd(self.registry.key, timestamp, 'bar')
        self.assertEqual(self.registry.count, 2)
        self.assertEqual(len(self.registry), 2)

    def test_clean_registries(self):
        """clean_registries() cleans Started and Finished job registries."""

        queue = Queue(connection=self.testconn)

        finished_job_registry = FinishedJobRegistry(connection=self.testconn)
        self.testconn.zadd(finished_job_registry.key, 1, 'foo')

        started_job_registry = StartedJobRegistry(connection=self.testconn)
        self.testconn.zadd(started_job_registry.key, 1, 'foo')

        clean_registries(queue)
        self.assertEqual(self.testconn.zcard(finished_job_registry.key), 0)
        self.assertEqual(self.testconn.zcard(started_job_registry.key), 0)


class TestFinishedJobRegistry(RQTestCase):

    def setUp(self):
        super(TestFinishedJobRegistry, self).setUp()
        self.registry = FinishedJobRegistry(connection=self.testconn)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:finished:default')

    def test_cleanup(self):
        """Finished job registry removes expired jobs."""
        timestamp = current_timestamp()
        self.testconn.zadd(self.registry.key, 1, 'foo')
        self.testconn.zadd(self.registry.key, timestamp + 10, 'bar')
        self.testconn.zadd(self.registry.key, timestamp + 30, 'baz')

        self.registry.cleanup()
        self.assertEqual(self.registry.get_job_ids(), ['bar', 'baz'])

        self.registry.cleanup(timestamp + 20)
        self.assertEqual(self.registry.get_job_ids(), ['baz'])

    def test_jobs_are_put_in_registry(self):
        """Completed jobs are added to FinishedJobRegistry."""
        self.assertEqual(self.registry.get_job_ids(), [])
        queue = Queue(connection=self.testconn)
        worker = Worker([queue])

        # Completed jobs are put in FinishedJobRegistry
        job = queue.enqueue(say_hello)
        worker.perform_job(job, queue)
        self.assertEqual(self.registry.get_job_ids(), [job.id])

        # When job is deleted, it should be removed from FinishedJobRegistry
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        job.delete()
        self.assertEqual(self.registry.get_job_ids(), [])

        # Failed jobs are not put in FinishedJobRegistry
        failed_job = queue.enqueue(div_by_zero)
        worker.perform_job(failed_job, queue)
        self.assertEqual(self.registry.get_job_ids(), [])


class TestDeferredRegistry(RQTestCase):

    def setUp(self):
        super(TestDeferredRegistry, self).setUp()
        self.registry = DeferredJobRegistry(connection=self.testconn)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:deferred:default')

    def test_add(self):
        """Adding a job to DeferredJobsRegistry."""
        job = Job()
        self.registry.add(job)
        job_ids = [as_text(job_id) for job_id in
                   self.testconn.zrange(self.registry.key, 0, -1)]
        self.assertEqual(job_ids, [job.id])

    def test_register_dependency(self):
        """Ensure job creation and deletion works properly with DeferredJobRegistry."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)
        job2 = queue.enqueue(say_hello, depends_on=job)

        registry = DeferredJobRegistry(connection=self.testconn)
        self.assertEqual(registry.get_job_ids(), [job2.id])

        # When deleted, job removes itself from DeferredJobRegistry
        job2.delete()
        self.assertEqual(registry.get_job_ids(), [])
