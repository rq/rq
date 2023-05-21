from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import ANY

from rq.defaults import DEFAULT_FAILURE_TTL
from rq.exceptions import AbandonedJobError, InvalidJobOperation
from rq.job import Job, JobStatus, requeue_job
from rq.queue import Queue
from rq.registry import (
    CanceledJobRegistry,
    DeferredJobRegistry,
    FailedJobRegistry,
    FinishedJobRegistry,
    StartedJobRegistry,
    clean_registries,
)
from rq.serializers import JSONSerializer
from rq.utils import as_text, current_timestamp
from rq.worker import Worker
from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class CustomJob(Job):
    """A custom job class just to test it"""


class TestRegistry(RQTestCase):
    def setUp(self):
        super().setUp()
        self.registry = StartedJobRegistry(connection=self.testconn)

    def test_init(self):
        """Registry can be instantiated with queue or name/Redis connection"""
        queue = Queue('foo', connection=self.testconn)
        registry = StartedJobRegistry(queue=queue)
        self.assertEqual(registry.name, queue.name)
        self.assertEqual(registry.connection, queue.connection)
        self.assertEqual(registry.serializer, queue.serializer)

        registry = StartedJobRegistry('bar', self.testconn, serializer=JSONSerializer)
        self.assertEqual(registry.name, 'bar')
        self.assertEqual(registry.connection, self.testconn)
        self.assertEqual(registry.serializer, JSONSerializer)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:wip:default')

    def test_custom_job_class(self):
        registry = StartedJobRegistry(job_class=CustomJob)
        self.assertFalse(registry.job_class == self.registry.job_class)

    def test_contains(self):
        registry = StartedJobRegistry(connection=self.testconn)
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)

        self.assertFalse(job in registry)
        self.assertFalse(job.id in registry)

        registry.add(job, 5)

        self.assertTrue(job in registry)
        self.assertTrue(job.id in registry)

    def test_get_expiration_time(self):
        """registry.get_expiration_time() returns correct datetime objects"""
        registry = StartedJobRegistry(connection=self.testconn)
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)

        registry.add(job, 5)
        time = registry.get_expiration_time(job)
        expected_time = (datetime.utcnow() + timedelta(seconds=5)).replace(microsecond=0)
        self.assertGreaterEqual(time, expected_time - timedelta(seconds=2))
        self.assertLessEqual(time, expected_time + timedelta(seconds=2))

    def test_add_and_remove(self):
        """Adding and removing job to StartedJobRegistry."""
        timestamp = current_timestamp()

        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)

        # Test that job is added with the right score
        self.registry.add(job, 1000)
        self.assertLess(self.testconn.zscore(self.registry.key, job.id), timestamp + 1002)

        # Ensure that a timeout of -1 results in a score of inf
        self.registry.add(job, -1)
        self.assertEqual(self.testconn.zscore(self.registry.key, job.id), float('inf'))

        # Ensure that job is removed from sorted set, but job key is not deleted
        self.registry.remove(job)
        self.assertIsNone(self.testconn.zscore(self.registry.key, job.id))
        self.assertTrue(self.testconn.exists(job.key))

        self.registry.add(job, -1)

        # registry.remove() also accepts job.id
        self.registry.remove(job.id)
        self.assertIsNone(self.testconn.zscore(self.registry.key, job.id))

        self.registry.add(job, -1)

        # delete_job = True deletes job key
        self.registry.remove(job, delete_job=True)
        self.assertIsNone(self.testconn.zscore(self.registry.key, job.id))
        self.assertFalse(self.testconn.exists(job.key))

        job = queue.enqueue(say_hello)

        self.registry.add(job, -1)

        # delete_job = True also works with job.id
        self.registry.remove(job.id, delete_job=True)
        self.assertIsNone(self.testconn.zscore(self.registry.key, job.id))
        self.assertFalse(self.testconn.exists(job.key))

    def test_add_and_remove_with_serializer(self):
        """Adding and removing job to StartedJobRegistry (with serializer)."""
        # delete_job = True also works with job.id and custom serializer
        queue = Queue(connection=self.testconn, serializer=JSONSerializer)
        registry = StartedJobRegistry(connection=self.testconn, serializer=JSONSerializer)
        job = queue.enqueue(say_hello)
        registry.add(job, -1)
        registry.remove(job.id, delete_job=True)
        self.assertIsNone(self.testconn.zscore(registry.key, job.id))
        self.assertFalse(self.testconn.exists(job.key))

    def test_get_job_ids(self):
        """Getting job ids from StartedJobRegistry."""
        timestamp = current_timestamp()
        self.testconn.zadd(self.registry.key, {'foo': timestamp + 10})
        self.testconn.zadd(self.registry.key, {'bar': timestamp + 20})
        self.assertEqual(self.registry.get_job_ids(), ['foo', 'bar'])

    def test_get_expired_job_ids(self):
        """Getting expired job ids form StartedJobRegistry."""
        timestamp = current_timestamp()

        self.testconn.zadd(self.registry.key, {'foo': 1})
        self.testconn.zadd(self.registry.key, {'bar': timestamp + 10})
        self.testconn.zadd(self.registry.key, {'baz': timestamp + 30})

        self.assertEqual(self.registry.get_expired_job_ids(), ['foo'])
        self.assertEqual(self.registry.get_expired_job_ids(timestamp + 20), ['foo', 'bar'])

        # CanceledJobRegistry does not implement get_expired_job_ids()
        registry = CanceledJobRegistry(connection=self.testconn)
        self.assertRaises(NotImplementedError, registry.get_expired_job_ids)

    def test_cleanup_moves_jobs_to_failed_job_registry(self):
        """Moving expired jobs to FailedJobRegistry."""
        queue = Queue(connection=self.testconn)
        failed_job_registry = FailedJobRegistry(connection=self.testconn)
        job = queue.enqueue(say_hello)

        self.testconn.zadd(self.registry.key, {job.id: 2})

        # Job has not been moved to FailedJobRegistry
        self.registry.cleanup(1)
        self.assertNotIn(job, failed_job_registry)
        self.assertIn(job, self.registry)

        with mock.patch.object(Job, 'execute_failure_callback') as mocked:
            self.registry.cleanup()
            mocked.assert_called_once_with(queue.death_penalty_class, AbandonedJobError, ANY, ANY)
        self.assertIn(job.id, failed_job_registry)
        self.assertNotIn(job, self.registry)
        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertTrue(job.exc_info)  # explanation is written to exc_info

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
        self.testconn.zadd(self.registry.key, {'foo': timestamp})
        self.testconn.zadd(self.registry.key, {'bar': timestamp})
        self.assertEqual(self.registry.count, 2)
        self.assertEqual(len(self.registry), 2)

        # Make sure

    def test_clean_registries(self):
        """clean_registries() cleans Started and Finished job registries."""

        queue = Queue(connection=self.testconn)

        finished_job_registry = FinishedJobRegistry(connection=self.testconn)
        self.testconn.zadd(finished_job_registry.key, {'foo': 1})

        started_job_registry = StartedJobRegistry(connection=self.testconn)
        self.testconn.zadd(started_job_registry.key, {'foo': 1})

        failed_job_registry = FailedJobRegistry(connection=self.testconn)
        self.testconn.zadd(failed_job_registry.key, {'foo': 1})

        clean_registries(queue)
        self.assertEqual(self.testconn.zcard(finished_job_registry.key), 0)
        self.assertEqual(self.testconn.zcard(started_job_registry.key), 0)
        self.assertEqual(self.testconn.zcard(failed_job_registry.key), 0)

    def test_clean_registries_with_serializer(self):
        """clean_registries() cleans Started and Finished job registries (with serializer)."""

        queue = Queue(connection=self.testconn, serializer=JSONSerializer)

        finished_job_registry = FinishedJobRegistry(connection=self.testconn, serializer=JSONSerializer)
        self.testconn.zadd(finished_job_registry.key, {'foo': 1})

        started_job_registry = StartedJobRegistry(connection=self.testconn, serializer=JSONSerializer)
        self.testconn.zadd(started_job_registry.key, {'foo': 1})

        failed_job_registry = FailedJobRegistry(connection=self.testconn, serializer=JSONSerializer)
        self.testconn.zadd(failed_job_registry.key, {'foo': 1})

        clean_registries(queue)
        self.assertEqual(self.testconn.zcard(finished_job_registry.key), 0)
        self.assertEqual(self.testconn.zcard(started_job_registry.key), 0)
        self.assertEqual(self.testconn.zcard(failed_job_registry.key), 0)

    def test_get_queue(self):
        """registry.get_queue() returns the right Queue object."""
        registry = StartedJobRegistry(connection=self.testconn)
        self.assertEqual(registry.get_queue(), Queue(connection=self.testconn))

        registry = StartedJobRegistry('foo', connection=self.testconn, serializer=JSONSerializer)
        self.assertEqual(registry.get_queue(), Queue('foo', connection=self.testconn, serializer=JSONSerializer))


class TestFinishedJobRegistry(RQTestCase):
    def setUp(self):
        super().setUp()
        self.registry = FinishedJobRegistry(connection=self.testconn)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:finished:default')

    def test_cleanup(self):
        """Finished job registry removes expired jobs."""
        timestamp = current_timestamp()
        self.testconn.zadd(self.registry.key, {'foo': 1})
        self.testconn.zadd(self.registry.key, {'bar': timestamp + 10})
        self.testconn.zadd(self.registry.key, {'baz': timestamp + 30})

        self.registry.cleanup()
        self.assertEqual(self.registry.get_job_ids(), ['bar', 'baz'])

        self.registry.cleanup(timestamp + 20)
        self.assertEqual(self.registry.get_job_ids(), ['baz'])

        # CanceledJobRegistry now implements noop cleanup, should not raise exception
        registry = CanceledJobRegistry(connection=self.testconn)
        registry.cleanup()

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
        super().setUp()
        self.registry = DeferredJobRegistry(connection=self.testconn)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:deferred:default')

    def test_add(self):
        """Adding a job to DeferredJobsRegistry."""
        job = Job()
        self.registry.add(job)
        job_ids = [as_text(job_id) for job_id in self.testconn.zrange(self.registry.key, 0, -1)]
        self.assertEqual(job_ids, [job.id])

    def test_register_dependency(self):
        """Ensure job creation and deletion works with DeferredJobRegistry."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)
        job2 = queue.enqueue(say_hello, depends_on=job)

        registry = DeferredJobRegistry(connection=self.testconn)
        self.assertEqual(registry.get_job_ids(), [job2.id])

        # When deleted, job removes itself from DeferredJobRegistry
        job2.delete()
        self.assertEqual(registry.get_job_ids(), [])


class TestFailedJobRegistry(RQTestCase):
    def test_default_failure_ttl(self):
        """Job TTL defaults to DEFAULT_FAILURE_TTL"""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)

        registry = FailedJobRegistry(connection=self.testconn)
        key = registry.key

        timestamp = current_timestamp()
        registry.add(job)
        score = self.testconn.zscore(key, job.id)
        self.assertLess(score, timestamp + DEFAULT_FAILURE_TTL + 2)
        self.assertGreater(score, timestamp + DEFAULT_FAILURE_TTL - 2)

        # Job key will also expire
        job_ttl = self.testconn.ttl(job.key)
        self.assertLess(job_ttl, DEFAULT_FAILURE_TTL + 2)
        self.assertGreater(job_ttl, DEFAULT_FAILURE_TTL - 2)

        timestamp = current_timestamp()
        ttl = 5
        registry.add(job, ttl=ttl)
        score = self.testconn.zscore(key, job.id)
        self.assertLess(score, timestamp + ttl + 2)
        self.assertGreater(score, timestamp + ttl - 2)

        job_ttl = self.testconn.ttl(job.key)
        self.assertLess(job_ttl, ttl + 2)
        self.assertGreater(job_ttl, ttl - 2)

    def test_requeue(self):
        """FailedJobRegistry.requeue works properly"""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(div_by_zero, failure_ttl=5)

        worker = Worker([queue])
        worker.work(burst=True)

        registry = FailedJobRegistry(connection=worker.connection)
        self.assertTrue(job in registry)

        registry.requeue(job.id)
        self.assertFalse(job in registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.started_at, None)
        self.assertEqual(job.ended_at, None)

        worker.work(burst=True)
        self.assertTrue(job in registry)

        # Should also work with job instance
        registry.requeue(job)
        self.assertFalse(job in registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        worker.work(burst=True)
        self.assertTrue(job in registry)

        # requeue_job should work the same way
        requeue_job(job.id, connection=self.testconn)
        self.assertFalse(job in registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        worker.work(burst=True)
        self.assertTrue(job in registry)

        # And so does job.requeue()
        job.requeue()
        self.assertFalse(job in registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_requeue_with_serializer(self):
        """FailedJobRegistry.requeue works properly (with serializer)"""
        queue = Queue(connection=self.testconn, serializer=JSONSerializer)
        job = queue.enqueue(div_by_zero, failure_ttl=5)

        worker = Worker([queue], serializer=JSONSerializer)
        worker.work(burst=True)

        registry = FailedJobRegistry(connection=worker.connection, serializer=JSONSerializer)
        self.assertTrue(job in registry)

        registry.requeue(job.id)
        self.assertFalse(job in registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.started_at, None)
        self.assertEqual(job.ended_at, None)

        worker.work(burst=True)
        self.assertTrue(job in registry)

        # Should also work with job instance
        registry.requeue(job)
        self.assertFalse(job in registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        worker.work(burst=True)
        self.assertTrue(job in registry)

        # requeue_job should work the same way
        requeue_job(job.id, connection=self.testconn, serializer=JSONSerializer)
        self.assertFalse(job in registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        worker.work(burst=True)
        self.assertTrue(job in registry)

        # And so does job.requeue()
        job.requeue()
        self.assertFalse(job in registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_invalid_job(self):
        """Requeuing a job that's not in FailedJobRegistry raises an error."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)

        registry = FailedJobRegistry(connection=self.testconn)
        with self.assertRaises(InvalidJobOperation):
            registry.requeue(job)

    def test_worker_handle_job_failure(self):
        """Failed jobs are added to FailedJobRegistry"""
        q = Queue(connection=self.testconn)

        w = Worker([q])
        registry = FailedJobRegistry(connection=w.connection)

        timestamp = current_timestamp()

        job = q.enqueue(div_by_zero, failure_ttl=5)
        w.handle_job_failure(job, q)
        # job is added to FailedJobRegistry with default failure ttl
        self.assertIn(job.id, registry.get_job_ids())
        self.assertLess(self.testconn.zscore(registry.key, job.id), timestamp + DEFAULT_FAILURE_TTL + 5)

        # job is added to FailedJobRegistry with specified ttl
        job = q.enqueue(div_by_zero, failure_ttl=5)
        w.handle_job_failure(job, q)
        self.assertLess(self.testconn.zscore(registry.key, job.id), timestamp + 7)
