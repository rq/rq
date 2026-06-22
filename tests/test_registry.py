import math
from datetime import timedelta
from unittest import mock
from unittest.mock import ANY

import pytest

from rq.defaults import DEFAULT_FAILURE_TTL
from rq.exceptions import AbandonedJobError, InvalidJobOperation
from rq.executions import Execution
from rq.job import Dependency, Job, JobStatus, requeue_job
from rq.queue import Queue
from rq.registry import (
    BaseRegistry,
    CanceledJobRegistry,
    DeferredJobRegistry,
    FailedJobRegistry,
    FinishedJobRegistry,
    ReadyJobRegistry,
    StartedJobRegistry,
    clean_registries,
)
from rq.serializers import JSONSerializer
from rq.utils import as_text, current_timestamp, now
from rq.worker import Worker
from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class CustomJob(Job):
    """A custom job class just to test it"""


class TestRegistry(RQTestCase):
    """Test all the BaseRegistry functionality"""

    def setUp(self):
        super().setUp()
        self.registry = BaseRegistry(connection=self.connection)

    def test_init(self):
        """Registry can be instantiated with queue or name/Redis connection"""
        queue = Queue('foo', connection=self.connection)
        registry = BaseRegistry(queue=queue)
        self.assertEqual(registry.name, queue.name)
        self.assertEqual(registry.connection, queue.connection)
        self.assertEqual(registry.serializer, queue.serializer)

        registry = BaseRegistry('bar', self.connection, serializer=JSONSerializer)
        self.assertEqual(registry.name, 'bar')
        self.assertEqual(registry.connection, self.connection)
        self.assertEqual(registry.serializer, JSONSerializer)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:registry:default')

    def test_custom_job_class(self):
        registry = BaseRegistry(job_class=CustomJob)
        self.assertIsNot(registry.job_class, self.registry.job_class)

    def test_contains(self):
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        self.assertNotIn(job, self.registry)
        self.assertNotIn(job.id, self.registry)

        self.registry.add(job, 5)

        self.assertIn(job, self.registry)
        self.assertIn(job.id, self.registry)

    def test_get_expiration_time(self):
        """registry.get_expiration_time() returns correct datetime objects"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        self.registry.add(job, 5)
        time = self.registry.get_expiration_time(job)
        expected_time = (now() + timedelta(seconds=5)).replace(microsecond=0)
        self.assertGreaterEqual(time, expected_time - timedelta(seconds=2))
        self.assertLessEqual(time, expected_time + timedelta(seconds=2))

    def test_add_and_remove(self):
        """Adding and removing job from BaseRegistry."""
        timestamp = current_timestamp()

        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        # Test that job is added with the right score
        self.registry.add(job, 1000)
        self.assertLess(self.connection.zscore(self.registry.key, job.id), timestamp + 1002)

        # Ensure that a timeout of -1 results in a score of inf
        self.registry.add(job, -1)
        self.assertEqual(self.connection.zscore(self.registry.key, job.id), float('inf'))

        # Ensure that job is removed from sorted set, but job key is not deleted
        self.registry.remove(job)
        self.assertIsNone(self.connection.zscore(self.registry.key, job.id))
        self.assertTrue(self.connection.exists(job.key))

        self.registry.add(job, -1)

        # registry.remove() also accepts job.id
        self.registry.remove(job.id)
        self.assertIsNone(self.connection.zscore(self.registry.key, job.id))

        self.registry.add(job, -1)

        # delete_job = True deletes job key
        self.registry.remove(job, delete_job=True)
        self.assertIsNone(self.connection.zscore(self.registry.key, job.id))
        self.assertFalse(self.connection.exists(job.key))

        job = queue.enqueue(say_hello)

        self.registry.add(job, -1)

        # delete_job = True also works with job.id
        self.registry.remove(job.id, delete_job=True)
        self.assertIsNone(self.connection.zscore(self.registry.key, job.id))
        self.assertFalse(self.connection.exists(job.key))

    def test_add_and_remove_with_serializer(self):
        """Adding and removing job from BaseRegistry (with serializer)."""
        # delete_job = True also works with job.id and custom serializer
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        registry = BaseRegistry(connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(say_hello)
        registry.add(job, -1)
        registry.remove(job.id, delete_job=True)
        self.assertIsNone(self.connection.zscore(registry.key, job.id))
        self.assertFalse(self.connection.exists(job.key))

    def test_get_job_ids(self):
        """Getting job ids from BaseRegistry."""
        timestamp = current_timestamp()
        self.connection.zadd(self.registry.key, {'will-be-cleaned-up': 1})
        self.connection.zadd(self.registry.key, {'foo': timestamp + 10})
        self.connection.zadd(self.registry.key, {'bar': timestamp + 20})
        self.assertEqual(self.registry.get_job_ids(), ['will-be-cleaned-up', 'foo', 'bar'])

    def test_get_expired_job_ids(self):
        """Getting expired job ids form BaseRegistry."""
        timestamp = current_timestamp()

        self.connection.zadd(self.registry.key, {'foo': 1})
        self.connection.zadd(self.registry.key, {'bar': timestamp + 10})
        self.connection.zadd(self.registry.key, {'baz': timestamp + 30})

        self.assertEqual(self.registry.get_expired_job_ids(), ['foo'])
        self.assertEqual(self.registry.get_expired_job_ids(timestamp + 20), ['foo', 'bar'])

        # CanceledJobRegistry does not implement get_expired_job_ids()
        registry = CanceledJobRegistry(connection=self.connection)
        self.assertRaises(NotImplementedError, registry.get_expired_job_ids)

    def test_count(self):
        """BaseRegistry returns the right number of job count."""
        timestamp = current_timestamp() + 10
        self.connection.zadd(self.registry.key, {'will-be-cleaned-up': 1})
        self.connection.zadd(self.registry.key, {'foo': timestamp})
        self.connection.zadd(self.registry.key, {'bar': timestamp})
        self.assertEqual(self.registry.count, 3)
        self.assertEqual(len(self.registry), 3)

    def test_get_job_count(self):
        """Ensure cleanup is not called and does not affect the reported number of jobs.

        Note, the original motivation to stop calling cleanup was to make the count operation O(1) to allow usage of
        monitoring tools and avoid side effects of failure callbacks that cleanup triggers.
        """
        timestamp = current_timestamp() + 10
        self.connection.zadd(self.registry.key, {'will-be-counted-despite-outdated': 1})
        self.connection.zadd(self.registry.key, {'foo': timestamp})
        self.connection.zadd(self.registry.key, {'bar': timestamp})
        with mock.patch.object(self.registry, 'cleanup') as mock_cleanup:
            self.assertEqual(self.registry.get_job_count(cleanup=False), 3)
        mock_cleanup.assert_not_called()

    def test_clean_registries(self):
        """clean_registries() cleans Started and Finished job registries."""

        queue = Queue(connection=self.connection)

        finished_job_registry = FinishedJobRegistry(connection=self.connection)
        self.connection.zadd(finished_job_registry.key, {'foo': 1})

        started_job_registry = StartedJobRegistry(connection=self.connection)
        self.connection.zadd(started_job_registry.key, {'foo:execution_id': 1})

        failed_job_registry = FailedJobRegistry(connection=self.connection)
        self.connection.zadd(failed_job_registry.key, {'foo': 1})

        clean_registries(queue)
        self.assertEqual(self.connection.zcard(finished_job_registry.key), 0)
        self.assertEqual(self.connection.zcard(started_job_registry.key), 0)
        self.assertEqual(self.connection.zcard(failed_job_registry.key), 0)

    def test_clean_registries_with_serializer(self):
        """clean_registries() cleans Started and Finished job registries (with serializer)."""

        queue = Queue(connection=self.connection, serializer=JSONSerializer)

        finished_job_registry = FinishedJobRegistry(connection=self.connection, serializer=JSONSerializer)
        self.connection.zadd(finished_job_registry.key, {'foo': 1})

        started_job_registry = StartedJobRegistry(connection=self.connection, serializer=JSONSerializer)
        self.connection.zadd(started_job_registry.key, {'foo:execution_id': 1})

        failed_job_registry = FailedJobRegistry(connection=self.connection, serializer=JSONSerializer)
        self.connection.zadd(failed_job_registry.key, {'foo': 1})

        clean_registries(queue)
        self.assertEqual(self.connection.zcard(finished_job_registry.key), 0)
        self.assertEqual(self.connection.zcard(started_job_registry.key), 0)
        self.assertEqual(self.connection.zcard(failed_job_registry.key), 0)

    def test_get_queue(self):
        """registry.get_queue() returns the right Queue object."""
        registry = BaseRegistry(connection=self.connection)
        self.assertEqual(registry.get_queue(), Queue(connection=self.connection))

        registry = BaseRegistry('foo', connection=self.connection, serializer=JSONSerializer)
        self.assertEqual(registry.get_queue(), Queue('foo', connection=self.connection, serializer=JSONSerializer))


class TestFinishedJobRegistry(RQTestCase):
    def setUp(self):
        super().setUp()
        self.registry = FinishedJobRegistry(connection=self.connection)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:finished:default')

    def test_cleanup(self):
        """Finished job registry removes expired jobs."""
        timestamp = current_timestamp()
        self.connection.zadd(self.registry.key, {'foo': 1})
        self.connection.zadd(self.registry.key, {'bar': timestamp + 10})
        self.connection.zadd(self.registry.key, {'baz': timestamp + 30})

        self.registry.cleanup()
        self.assertEqual(self.registry.get_job_ids(), ['bar', 'baz'])

        self.registry.cleanup(timestamp + 20)
        self.assertEqual(self.registry.get_job_ids(), ['baz'])

        # CanceledJobRegistry now implements noop cleanup, should not raise exception
        registry = CanceledJobRegistry(connection=self.connection)
        registry.cleanup()

    def test_jobs_are_put_in_registry(self):
        """Completed jobs are added to FinishedJobRegistry."""
        self.assertEqual(self.registry.get_job_ids(), [])
        queue = Queue(connection=self.connection)
        worker = Worker([queue], connection=self.connection)

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
        self.registry = DeferredJobRegistry(connection=self.connection)

    def test_key(self):
        self.assertEqual(self.registry.key, 'rq:deferred:default')

    def test_add(self):
        """Adding a job to DeferredJobsRegistry."""
        job = Job(connection=self.connection)
        self.registry.add(job)
        job_ids = [as_text(job_id) for job_id in self.connection.zrange(self.registry.key, 0, -1)]
        self.assertEqual(job_ids, [job.id])

    def test_add_with_deferred_ttl(self):
        """Job score is set to current timestamp (creation time), ttl is ignored."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        key = self.registry.key
        timestamp = current_timestamp()

        self.registry.add(job)
        score = self.connection.zscore(key, job.id)
        self.assertGreater(score, timestamp - 2)
        self.assertLess(score, timestamp + 2)

        # ttl parameter is ignored for deferred jobs
        self.registry.add(job, ttl=5)
        score = self.connection.zscore(key, job.id)
        self.assertGreater(score, timestamp - 2)
        self.assertLess(score, timestamp + 2)

    def test_register_dependency(self):
        """Ensure job creation and deletion works with DeferredJobRegistry."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        job2 = queue.enqueue(say_hello, depends_on=job)

        registry = DeferredJobRegistry(connection=self.connection)
        self.assertEqual(registry.get_job_ids(), [job2.id])

        # When deleted, job removes itself from DeferredJobRegistry
        job2.delete()
        self.assertEqual(registry.get_job_ids(), [])

    def test_cleanup_is_noop(self):
        """Deferred jobs don't expire based on time, so cleanup is a no-op."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        self.registry.add(job)

        self.assertEqual(self.registry.count, 1)
        self.registry.cleanup()
        self.assertEqual(self.registry.count, 1)


class TestReadyJobRegistry(RQTestCase):
    def setUp(self):
        super().setUp()
        self.registry = ReadyJobRegistry(connection=self.connection)

    def test_add_and_remove(self):
        """Adding/removing a job to ReadyJobRegistry."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        job.set_status(JobStatus.READY_TO_ENQUEUE)
        self.registry.add(job)
        self.assertIn(job, self.registry)
        self.assertEqual(self.registry.get_job_ids(cleanup=False), [job.id])

        self.registry.remove(job)
        self.assertNotIn(job, self.registry)
        self.assertEqual(self.registry.get_job_ids(cleanup=False), [])

    def test_queue_property(self):
        """Queue.ready_job_registry returns a ReadyJobRegistry bound to the queue."""
        queue = Queue('foo', connection=self.connection)
        registry = queue.ready_job_registry
        self.assertIsInstance(registry, ReadyJobRegistry)
        self.assertEqual(registry.key, 'rq:ready:foo')

    def test_delete_removes_ready_job_from_registry(self):
        """Deleting a READY_TO_ENQUEUE job removes it from ReadyJobRegistry."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        job.set_status(JobStatus.READY_TO_ENQUEUE)
        self.registry.add(job)
        self.assertIn(job, self.registry)

        job.delete()
        self.assertNotIn(job, self.registry)

    def test_job_is_ready_to_enqueue(self):
        """Job.is_ready_to_enqueue reflects the READY_TO_ENQUEUE status."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        self.assertFalse(job.is_ready_to_enqueue)
        job.set_status(JobStatus.READY_TO_ENQUEUE)
        self.assertTrue(job.is_ready_to_enqueue)

    def test_enqueue_jobs_moves_ready_jobs_to_queue(self):
        """enqueue_jobs() enqueues READY_TO_ENQUEUE jobs (respecting enqueue_at_front),
        sets their status to QUEUED, and removes them from the registry."""
        queue = Queue(connection=self.connection)
        sentinel = queue.enqueue(say_hello)

        back_job = queue.enqueue(say_hello)
        queue.remove(back_job)
        back_job.set_status(JobStatus.READY_TO_ENQUEUE)
        self.registry.add(back_job)

        enqueued_jobs = self.registry.enqueue_jobs([back_job.id])
        self.assertEqual(enqueued_jobs, [back_job])
        self.assertNotIn(back_job.id, self.registry.get_job_ids(cleanup=False))
        self.assertEqual(Job.fetch(back_job.id, connection=self.connection).get_status(), JobStatus.QUEUED)
        self.assertEqual(queue.get_job_ids(), [sentinel.id, back_job.id])

        front_job = queue.enqueue(say_hello)
        queue.remove(front_job)
        front_job.enqueue_at_front = True
        front_job.save()
        front_job.set_status(JobStatus.READY_TO_ENQUEUE)
        self.registry.add(front_job)

        self.registry.enqueue_jobs([front_job.id])
        self.assertEqual(queue.get_job_ids(), [front_job.id, sentinel.id, back_job.id])

    def test_enqueue_jobs_drops_stale_entries(self):
        """Stale entries — wrong status or missing job — are removed without enqueuing."""
        queue = Queue(connection=self.connection)

        # Wrong status: registry says ready, but job's status was changed
        stale_status_job = queue.enqueue(say_hello)
        queue.remove(stale_status_job)
        self.registry.add(stale_status_job)
        stale_status_job.set_status(JobStatus.CANCELED)

        # Missing job: dangling registry entry pointing at a deleted job
        missing_job = queue.enqueue(say_hello)
        missing_job.delete()
        self.connection.zadd(self.registry.key, {missing_job.id: current_timestamp()})

        self.assertEqual(self.connection.zcard(self.registry.key), 2)

        enqueued_jobs = self.registry.enqueue_jobs([stale_status_job.id, missing_job.id])

        self.assertEqual(enqueued_jobs, [])
        self.assertEqual(self.connection.zcard(self.registry.key), 0)
        self.assertEqual(queue.get_job_ids(), [])

    def test_enqueue_jobs_isolates_failures(self):
        """A failure on one job leaves it in the registry; other jobs still enqueue."""
        queue = Queue(connection=self.connection)
        job_a = queue.enqueue(say_hello)
        job_b = queue.enqueue(say_hello)
        queue.remove(job_a)
        queue.remove(job_b)
        for job in (job_a, job_b):
            job.set_status(JobStatus.READY_TO_ENQUEUE)
            self.registry.add(job)

        original = Queue._enqueue_job

        def fake_enqueue(self_queue, job, *args, **kwargs):
            if job.id == job_b.id:
                raise RuntimeError()
            return original(self_queue, job, *args, **kwargs)

        with mock.patch.object(Queue, '_enqueue_job', fake_enqueue):
            enqueued_jobs = self.registry.enqueue_jobs([job_a.id, job_b.id])

        self.assertEqual(enqueued_jobs, [job_a])
        self.assertIn(job_b.id, self.registry.get_job_ids(cleanup=False))
        self.assertNotIn(job_a.id, self.registry.get_job_ids(cleanup=False))

    def test_cleanup_recovers_ready_jobs(self):
        """ReadyJobRegistry.cleanup() enqueues anything left in the registry."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        queue.remove(job)
        job.set_status(JobStatus.READY_TO_ENQUEUE)
        self.registry.add(job)

        self.registry.cleanup()

        self.assertEqual(self.registry.count, 0)
        self.assertIn(job.id, queue.get_job_ids())

    def test_enqueue_jobs_concedes_on_watcherror(self):
        """On WatchError, the loser returns [] and does not enqueue the job."""
        from redis.client import Pipeline
        from redis.exceptions import WatchError

        queue = Queue(connection=self.connection)
        job = Job.create(say_hello, connection=self.connection, origin=queue.name, status=JobStatus.READY_TO_ENQUEUE)
        job.save()
        self.registry.add(job)

        original_execute = Pipeline.execute

        def fake_execute(self_pipe, *args, **kwargs):
            if getattr(self_pipe, 'watching', False):
                raise WatchError('simulated contention')
            return original_execute(self_pipe, *args, **kwargs)

        with mock.patch('redis.client.Pipeline.execute', fake_execute):
            enqueued_jobs = self.registry.enqueue_jobs([job.id])

        self.assertEqual(enqueued_jobs, [])
        # Loser did not enqueue — no duplicate
        self.assertEqual(queue.get_job_ids().count(job.id), 0)
        # EXEC aborted, so the watched zrem didn't fire — entry remains for next cleanup
        self.assertIn(job.id, self.registry.get_job_ids(cleanup=False))

    def test_enqueue_jobs_drops_stale_status_under_watch(self):
        """If the watched re-read finds non-READY status, the entry is dropped inside MULTI."""
        from redis.client import Pipeline

        queue = Queue(connection=self.connection)
        job = Job.create(say_hello, connection=self.connection, origin=queue.name, status=JobStatus.READY_TO_ENQUEUE)
        job.save()
        self.registry.add(job)

        original_hget = Pipeline.hget

        def fake_hget(self_pipe, name, key):
            if name == job.key and key == 'status':
                return b'canceled'
            return original_hget(self_pipe, name, key)

        with mock.patch('redis.client.Pipeline.hget', fake_hget):
            enqueued_jobs = self.registry.enqueue_jobs([job.id])

        self.assertEqual(enqueued_jobs, [])
        self.assertEqual(self.connection.zcard(self.registry.key), 0)
        self.assertNotIn(job.id, queue.get_job_ids())

    def test_register_jobs_moves_deferred_jobs_to_ready(self):
        """register_jobs() appends deferred-remove + status-set + ready-add to the caller's pipeline."""
        queue = Queue(connection=self.connection)
        deferred_registry = DeferredJobRegistry(connection=self.connection)

        job = Job.create(say_hello, connection=self.connection, origin=queue.name, status=JobStatus.DEFERRED)
        job.save()
        deferred_registry.add(job)
        self.assertIn(job.id, deferred_registry.get_job_ids())

        with self.connection.pipeline() as pipeline:
            pipeline.watch(self.registry.key)
            pipeline.multi()
            self.registry.register_jobs([job], pipeline=pipeline)
            pipeline.execute()

        self.assertNotIn(job.id, deferred_registry.get_job_ids())
        self.assertIn(job.id, self.registry.get_job_ids(cleanup=False))
        self.assertEqual(Job.fetch(job.id, connection=self.connection).get_status(), JobStatus.READY_TO_ENQUEUE)

    def test_clean_registries_invokes_ready_cleanup(self):
        """clean_registries(queue) recovers jobs from the ready registry."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        queue.remove(job)
        job.set_status(JobStatus.READY_TO_ENQUEUE)
        self.registry.add(job)

        clean_registries(queue)

        self.assertEqual(self.registry.count, 0)
        self.assertIn(job.id, queue.get_job_ids())

    def test_enqueue_ready_jobs_by_queue_isolates_drain_failure(self):
        """A drain failure is swallowed; the job stays ready and is recovered by cleanup."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        queue.remove(job)
        job.set_status(JobStatus.READY_TO_ENQUEUE)
        self.registry.add(job)

        # Patch scoped to ONLY the drain call — if it leaked into clean_registries below,
        # recovery would fail for the wrong reason.
        with mock.patch.object(ReadyJobRegistry, 'enqueue_jobs', side_effect=RuntimeError()):
            queue.enqueue_ready_jobs_by_queue({queue.name: [job.id]})  # must not raise

        self.assertEqual(Job.fetch(job.id, connection=self.connection).get_status(), JobStatus.READY_TO_ENQUEUE)
        self.assertNotIn(job.id, queue.get_job_ids())
        self.assertIn(job.id, self.registry.get_job_ids(cleanup=False))

        # Real enqueue_jobs (patch lifted) recovers the job.
        clean_registries(queue)
        self.assertEqual(self.registry.count, 0)
        self.assertIn(job.id, queue.get_job_ids())


class TestFailedJobRegistry(RQTestCase):
    def test_default_failure_ttl(self):
        """Job TTL defaults to DEFAULT_FAILURE_TTL"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        registry = FailedJobRegistry(connection=self.connection)
        key = registry.key

        timestamp = current_timestamp()
        registry.add(job)
        score = self.connection.zscore(key, job.id)
        self.assertLess(score, timestamp + DEFAULT_FAILURE_TTL + 2)
        self.assertGreater(score, timestamp + DEFAULT_FAILURE_TTL - 2)

        # Job key will also expire
        job_ttl = self.connection.ttl(job.key)
        self.assertLess(job_ttl, DEFAULT_FAILURE_TTL + 2)
        self.assertGreater(job_ttl, DEFAULT_FAILURE_TTL - 2)

        timestamp = current_timestamp()
        ttl = 5
        registry.add(job, ttl=ttl)
        score = self.connection.zscore(key, job.id)
        self.assertLess(score, timestamp + ttl + 2)
        self.assertGreater(score, timestamp + ttl - 2)

        job_ttl = self.connection.ttl(job.key)
        self.assertLess(job_ttl, ttl + 2)
        self.assertGreater(job_ttl, ttl - 2)

    def test_requeue(self):
        """FailedJobRegistry.requeue works properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(div_by_zero, failure_ttl=5)

        worker = Worker([queue], connection=self.connection)
        worker.work(burst=True)

        registry = FailedJobRegistry(connection=worker.connection)
        self.assertIn(job, registry)

        registry.requeue(job.id)
        self.assertNotIn(job, registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.started_at, None)
        self.assertEqual(job.ended_at, None)

        worker.work(burst=True)
        self.assertIn(job, registry)

        # Should also work with job instance
        registry.requeue(job)
        self.assertNotIn(job, registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        worker.work(burst=True)
        self.assertIn(job, registry)

        # requeue_job should work the same way
        requeue_job(job.id, connection=self.connection)
        self.assertNotIn(job, registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        worker.work(burst=True)
        self.assertIn(job, registry)

        # And so does job.requeue()
        job.requeue()
        self.assertNotIn(job, registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_requeue_with_serializer(self):
        """FailedJobRegistry.requeue works properly (with serializer)"""
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(div_by_zero, failure_ttl=5)

        worker = Worker([queue], serializer=JSONSerializer, connection=self.connection)
        worker.work(burst=True)

        registry = FailedJobRegistry(connection=worker.connection, serializer=JSONSerializer)
        self.assertIn(job, registry)

        registry.requeue(job.id)
        self.assertNotIn(job, registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.started_at, None)
        self.assertEqual(job.ended_at, None)

        worker.work(burst=True)
        self.assertIn(job, registry)

        # Should also work with job instance
        registry.requeue(job)
        self.assertNotIn(job, registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        worker.work(burst=True)
        self.assertIn(job, registry)

        # requeue_job should work the same way
        requeue_job(job.id, connection=self.connection, serializer=JSONSerializer)
        self.assertNotIn(job, registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        worker.work(burst=True)
        self.assertIn(job, registry)

        # And so does job.requeue()
        job.requeue()
        self.assertNotIn(job, registry)
        self.assertIn(job.id, queue.get_job_ids())

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_invalid_job(self):
        """Requeuing a job that's not in FailedJobRegistry raises an error."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        registry = FailedJobRegistry(connection=self.connection)
        with self.assertRaises(InvalidJobOperation):
            registry.requeue(job)

    def test_worker_handle_job_failure(self):
        """Failed jobs are added to FailedJobRegistry"""
        q = Queue(connection=self.connection)

        w = Worker([q], connection=self.connection)
        registry = FailedJobRegistry(connection=w.connection)

        timestamp = current_timestamp()

        job = q.enqueue(div_by_zero, failure_ttl=5)
        w.handle_job_failure(job, q)
        # job is added to FailedJobRegistry with default failure ttl
        self.assertIn(job.id, registry.get_job_ids())
        self.assertLess(self.connection.zscore(registry.key, job.id), timestamp + DEFAULT_FAILURE_TTL + 5)

        # job is added to FailedJobRegistry with specified ttl
        job = q.enqueue(div_by_zero, failure_ttl=5)
        w.handle_job_failure(job, q)
        self.assertLess(self.connection.zscore(registry.key, job.id), timestamp + 7)


class TestStartedJobRegistry(RQTestCase):
    def setUp(self):
        super().setUp()
        self.registry = StartedJobRegistry(connection=self.connection)
        self.queue = Queue(connection=self.connection)

    def test_job_deletion(self):
        """Ensure job is removed from StartedJobRegistry when deleted."""
        worker = Worker([self.queue], connection=self.connection)

        job = self.queue.enqueue(say_hello)
        self.assertTrue(job.is_queued)

        execution = worker.prepare_execution(job)
        worker.prepare_job_execution(job)
        self.assertIn(execution.job_id, self.registry.get_job_ids())

        pipeline = self.connection.pipeline()
        job.delete(pipeline=pipeline)
        pipeline.execute()
        self.assertNotIn(execution.job_id, self.registry.get_job_ids())

    def test_contains(self):
        """Test the StartedJobRegistry __contains__ method. It is slightly different
        because the entries in the registry are {job_id}:{execution_id} format."""
        job = self.queue.enqueue(say_hello)

        self.assertNotIn(job, self.registry)
        self.assertNotIn(job.id, self.registry)

        with self.connection.pipeline() as pipe:
            self.registry.add_execution(
                Execution(id='execution', job_id=job.id, connection=self.connection), pipeline=pipe, ttl=5
            )
            pipe.execute()
        self.assertIn(job, self.registry)
        self.assertIn(job.id, self.registry)

    def test_infinite_score(self):
        """Test the StartedJobRegistry __contains__ method. It is slightly different
        because the entries in the registry are {job_id}:{execution_id} format."""
        job = self.queue.enqueue(say_hello)

        self.assertNotIn(job, self.registry)
        self.assertNotIn(job.id, self.registry)

        with self.connection.pipeline() as pipe:
            execution = Execution(id='execution', job_id=job.id, connection=self.connection)
            self.registry.add_execution(execution=execution, pipeline=pipe, ttl=-1)
            pipe.execute()
        self.assertIn(job, self.registry)
        self.assertIn(job.id, self.registry)
        self.assertEqual(self.connection.zscore(self.registry.key, execution.composite_key), math.inf)

    def test_remove_executions(self):
        """Ensure all executions for a job are removed from registry."""
        worker = Worker([self.queue], connection=self.connection)
        job = self.queue.enqueue(say_hello)

        execution_1 = worker.prepare_execution(job)
        execution_2 = worker.prepare_execution(job)
        self.assertIn((job.id, execution_1.id), self.registry.get_job_and_execution_ids())
        self.assertIn((job.id, execution_2.id), self.registry.get_job_and_execution_ids())

        self.registry.remove_executions(job)

        self.assertNotIn((job.id, execution_1.id), self.registry.get_job_and_execution_ids())
        self.assertNotIn((job.id, execution_2.id), self.registry.get_job_and_execution_ids())

        job.delete()

    def test_job_execution(self):
        """Job is removed from StartedJobRegistry after execution."""
        worker = Worker([self.queue], connection=self.connection)

        job = self.queue.enqueue(say_hello)
        self.assertTrue(job.is_queued)
        execution = worker.prepare_execution(job)
        worker.prepare_job_execution(job)
        self.assertIn(job.id, self.registry.get_job_ids())
        self.assertIn((job.id, execution.id), self.registry.get_job_and_execution_ids())
        self.assertTrue(job.is_started)

        worker.perform_job(job, self.queue)
        self.assertNotIn(job.id, self.registry.get_job_ids())
        self.assertNotIn((job.id, execution.id), self.registry.get_job_and_execution_ids())
        self.assertTrue(job.is_finished)

        # Job that fails
        job = self.queue.enqueue(div_by_zero)
        execution = worker.prepare_execution(job)
        worker.prepare_job_execution(job)
        self.assertIn(job.id, self.registry.get_job_ids())
        self.assertIn((job.id, execution.id), self.registry.get_job_and_execution_ids())

        worker.perform_job(job, self.queue)
        self.assertNotIn(job.id, self.registry.get_job_ids())
        self.assertNotIn((job.id, execution.id), self.registry.get_job_and_execution_ids())

    def test_get_job_ids(self):
        """Getting job ids with cleanup."""
        timestamp = current_timestamp()
        self.connection.zadd(self.registry.key, {'will-be-cleaned-up:execution_id1': 1})
        self.connection.zadd(self.registry.key, {'foo:execution_id2': timestamp + 10})
        self.connection.zadd(self.registry.key, {'bar:execution_id3': timestamp + 20})
        self.assertEqual(self.registry.get_job_ids(), ['foo', 'bar'])

    def test_get_job_ids_does_not_cleanup(self):
        """Getting job ids without a cleanup."""
        timestamp = current_timestamp()
        self.connection.zadd(self.registry.key, {'will-be-returned-despite-outdated:execution_id1': 1})
        self.connection.zadd(self.registry.key, {'foo:execution_id2': timestamp + 10})
        self.connection.zadd(self.registry.key, {'bar:execution_id3': timestamp + 20})
        self.assertEqual(self.registry.get_job_ids(cleanup=False), ['will-be-returned-despite-outdated', 'foo', 'bar'])

    def test_count(self):
        """Return the right number of job count (cleanup should be performed)"""
        timestamp = current_timestamp() + 10
        self.connection.zadd(self.registry.key, {'will-be-cleaned-up': 1})
        self.connection.zadd(self.registry.key, {'foo': timestamp})
        self.connection.zadd(self.registry.key, {'bar': timestamp})
        self.assertEqual(self.registry.count, 2)
        self.assertEqual(len(self.registry), 2)

    def test_cleanup_moves_jobs_to_failed_job_registry(self):
        """Moving expired jobs to FailedJobRegistry."""

        failed_job_registry = FailedJobRegistry(connection=self.connection)
        job = self.queue.enqueue(say_hello)

        self.connection.zadd(self.registry.key, {f'{job.id}:execution_id': 100})

        # Job has not been moved to FailedJobRegistry
        self.registry.cleanup(1)
        self.assertNotIn(job, failed_job_registry)
        self.assertIn(job, self.registry)

        with mock.patch.object(Job, 'execute_failure_callback') as mocked:
            mock_handler = mock.MagicMock()
            mock_handler.return_value = False
            mock_handler_no_return = mock.MagicMock()
            mock_handler_no_return.return_value = None
            self.registry.cleanup(exception_handlers=[mock_handler_no_return, mock_handler])
            mocked.assert_called_once_with(self.queue.death_penalty_class, AbandonedJobError, ANY, None)
            mock_handler.assert_called_once_with(job, AbandonedJobError, ANY, None)
            mock_handler_no_return.assert_called_once_with(job, AbandonedJobError, ANY, None)
        self.assertIn(job.id, failed_job_registry)
        self.assertNotIn(job, self.registry)
        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        latest_result = job.latest_result()
        self.assertIsNotNone(latest_result)
        self.assertTrue(latest_result.exc_string)  # explanation is written to exc_info

    def test_cleanup_continues_when_failure_callback_raises(self):
        """A raising failure callback must not stop the job from being moved to the
        FailedJobRegistry."""
        failed_job_registry = FailedJobRegistry(connection=self.connection)
        job = self.queue.enqueue(say_hello)
        self.connection.zadd(self.registry.key, {f'{job.id}:execution_id': 1})

        with mock.patch.object(Job, 'execute_failure_callback', side_effect=Exception()):
            self.registry.cleanup()

        self.assertIn(job.id, failed_job_registry)
        self.assertNotIn(job, self.registry)

    def test_enqueue_dependents_when_parent_job_is_abandoned(self):
        """Enqueuing parent job's dependencies after moving it to FailedJobRegistry due to AbandonedJobError."""
        queue = Queue(connection=self.connection)
        worker = Worker([queue])
        failed_job_registry = FailedJobRegistry(connection=self.connection)
        finished_job_registry = FinishedJobRegistry(connection=self.connection)
        deferred_job_registry = DeferredJobRegistry(connection=self.connection)

        parent_job = queue.enqueue(say_hello)
        job_to_be_executed = queue.enqueue_call(say_hello, depends_on=Dependency(jobs=[parent_job], allow_failure=True))
        job_not_to_be_executed = queue.enqueue_call(
            say_hello, depends_on=Dependency(jobs=[parent_job], allow_failure=False)
        )
        self.assertIn(job_to_be_executed, deferred_job_registry)
        self.assertIn(job_not_to_be_executed, deferred_job_registry)

        self.connection.zadd(self.registry.key, {f'{parent_job.id}:execution': 2})
        queue.remove(parent_job.id)

        with mock.patch.object(Job, 'execute_failure_callback') as mocked:
            self.registry.cleanup()
            mocked.assert_called_once_with(queue.death_penalty_class, AbandonedJobError, ANY, ANY)

        # check that parent job was moved to FailedJobRegistry and has correct status
        self.assertIn(parent_job, failed_job_registry)
        self.assertNotIn(parent_job, self.registry)
        self.assertTrue(parent_job.is_failed)

        # check that only job_to_be_executed has been queued and executed
        self.assertEqual(len(queue.get_job_ids()), 1)
        self.assertTrue(job_to_be_executed.is_queued)
        self.assertFalse(job_not_to_be_executed.is_queued)

        worker.work(burst=True)
        self.assertTrue(job_to_be_executed.is_finished)
        self.assertNotIn(job_to_be_executed, deferred_job_registry)
        self.assertIn(job_to_be_executed, finished_job_registry)

        self.assertFalse(job_not_to_be_executed.is_finished)
        self.assertNotIn(job_not_to_be_executed, finished_job_registry)

    def test_warnings_on_add_remove_and_exception(self):
        """Test backwards compatibility of the .add and .remove methods for
        the StartedJobRegistry."""
        with pytest.raises(NotImplementedError):
            self.registry.add('job_id')

        with pytest.raises(NotImplementedError):
            self.registry.remove('job_id')
