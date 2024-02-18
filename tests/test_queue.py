import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from redis import Redis

from rq import Queue, Retry
from rq.job import Job, JobStatus
from rq.registry import (
    CanceledJobRegistry,
    DeferredJobRegistry,
    FailedJobRegistry,
    FinishedJobRegistry,
    ScheduledJobRegistry,
    StartedJobRegistry,
)
from rq.serializers import JSONSerializer
from rq.utils import get_version
from rq.worker import Worker
from tests import RQTestCase
from tests.fixtures import echo, say_hello


class MultipleDependencyJob(Job):
    """
    Allows for the patching of `_dependency_ids` to simulate multi-dependency
    support without modifying the public interface of `Job`
    """

    create_job = Job.create

    @classmethod
    def create(cls, *args, **kwargs):
        dependency_ids = kwargs.pop('kwargs').pop('_dependency_ids')
        _job = cls.create_job(*args, **kwargs)
        _job._dependency_ids = dependency_ids
        return _job


class TestQueue(RQTestCase):
    def test_create_queue(self):
        """Creating queues."""
        q = Queue('my-queue', connection=self.connection)
        self.assertEqual(q.name, 'my-queue')
        self.assertEqual(str(q), '<Queue my-queue>')

    def test_create_queue_with_serializer(self):
        """Creating queues with serializer."""
        # Test using json serializer
        q = Queue('queue-with-serializer', connection=self.connection, serializer=json)
        self.assertEqual(q.name, 'queue-with-serializer')
        self.assertEqual(str(q), '<Queue queue-with-serializer>')
        self.assertIsNotNone(q.serializer)

    def test_create_default_queue(self):
        """Instantiating the default queue."""
        q = Queue(connection=self.connection)
        self.assertEqual(q.name, 'default')

    def test_equality(self):
        """Mathematical equality of queues."""
        q1 = Queue('foo', connection=self.connection)
        q2 = Queue('foo', connection=self.connection)
        q3 = Queue('bar', connection=self.connection)

        self.assertEqual(q1, q2)
        self.assertEqual(q2, q1)
        self.assertNotEqual(q1, q3)
        self.assertNotEqual(q2, q3)
        self.assertGreater(q1, q3)
        self.assertRaises(TypeError, lambda: q1 == 'some string')
        self.assertRaises(TypeError, lambda: q1 < 'some string')

    def test_empty_queue(self):
        """Emptying queues."""
        q = Queue('example', connection=self.connection)

        self.connection.rpush('rq:queue:example', 'foo')
        self.connection.rpush('rq:queue:example', 'bar')
        self.assertEqual(q.is_empty(), False)

        q.empty()

        self.assertEqual(q.is_empty(), True)
        self.assertIsNone(self.connection.lpop('rq:queue:example'))

    def test_empty_removes_jobs(self):
        """Emptying a queue deletes the associated job objects"""
        q = Queue('example', connection=self.connection)
        job = q.enqueue(say_hello)
        self.assertTrue(Job.exists(job.id, connection=self.connection))
        q.empty()
        self.assertFalse(Job.exists(job.id, connection=self.connection))

    def test_queue_is_empty(self):
        """Detecting empty queues."""
        q = Queue('example', connection=self.connection)
        self.assertEqual(q.is_empty(), True)

        self.connection.rpush('rq:queue:example', 'sentinel message')
        self.assertEqual(q.is_empty(), False)

    def test_queue_delete(self):
        """Test queue.delete properly removes queue"""
        q = Queue('example', connection=self.connection)
        job = q.enqueue(say_hello)
        job2 = q.enqueue(say_hello)

        self.assertEqual(2, len(q.get_job_ids()))

        q.delete()

        self.assertEqual(0, len(q.get_job_ids()))
        self.assertEqual(False, self.connection.exists(job.key))
        self.assertEqual(False, self.connection.exists(job2.key))
        self.assertEqual(0, len(self.connection.smembers(Queue.redis_queues_keys)))
        self.assertEqual(False, self.connection.exists(q.key))

    def test_queue_delete_but_keep_jobs(self):
        """Test queue.delete properly removes queue but keeps the job keys in the redis store"""
        q = Queue('example', connection=self.connection)
        job = q.enqueue(say_hello)
        job2 = q.enqueue(say_hello)

        self.assertEqual(2, len(q.get_job_ids()))

        q.delete(delete_jobs=False)

        self.assertEqual(0, len(q.get_job_ids()))
        self.assertEqual(True, self.connection.exists(job.key))
        self.assertEqual(True, self.connection.exists(job2.key))
        self.assertEqual(0, len(self.connection.smembers(Queue.redis_queues_keys)))
        self.assertEqual(False, self.connection.exists(q.key))

    def test_position(self):
        """Test queue.delete properly removes queue but keeps the job keys in the redis store"""
        q = Queue('example', connection=self.connection)
        job = q.enqueue(say_hello)
        job2 = q.enqueue(say_hello)
        job3 = q.enqueue(say_hello)

        self.assertEqual(0, q.get_job_position(job.id))
        self.assertEqual(1, q.get_job_position(job2.id))
        self.assertEqual(2, q.get_job_position(job3))
        self.assertEqual(None, q.get_job_position("no_real_job"))

    def test_remove(self):
        """Ensure queue.remove properly removes Job from queue."""
        q = Queue('example', serializer=JSONSerializer, connection=self.connection)
        job = q.enqueue(say_hello)
        self.assertIn(job.id, q.job_ids)
        q.remove(job)
        self.assertNotIn(job.id, q.job_ids)

        job = q.enqueue(say_hello)
        self.assertIn(job.id, q.job_ids)
        q.remove(job.id)
        self.assertNotIn(job.id, q.job_ids)

    def test_jobs(self):
        """Getting jobs out of a queue."""
        q = Queue('example', connection=self.connection)
        self.assertEqual(q.jobs, [])
        job = q.enqueue(say_hello)
        self.assertEqual(q.jobs, [job])

        # Deleting job removes it from queue
        job.delete()
        self.assertEqual(q.job_ids, [])

    def test_compact(self):
        """Queue.compact() removes non-existing jobs."""
        q = Queue(connection=self.connection)

        q.enqueue(say_hello, 'Alice')
        q.enqueue(say_hello, 'Charlie')
        self.connection.lpush(q.key, '1', '2')

        self.assertEqual(q.count, 4)
        self.assertEqual(len(q), 4)

        q.compact()

        self.assertEqual(q.count, 2)
        self.assertEqual(len(q), 2)

    def test_enqueue(self):
        """Enqueueing job onto queues."""
        q = Queue(connection=self.connection)
        self.assertEqual(q.is_empty(), True)

        # say_hello spec holds which queue this is sent to
        job = q.enqueue(say_hello, 'Nick', foo='bar')
        job_id = job.id
        self.assertEqual(job.origin, q.name)

        # Inspect data inside Redis
        q_key = 'rq:queue:default'
        self.assertEqual(self.connection.llen(q_key), 1)
        self.assertEqual(self.connection.lrange(q_key, 0, -1)[0].decode('ascii'), job_id)

    def test_enqueue_sets_metadata(self):
        """Enqueueing job onto queues modifies meta data."""
        q = Queue(connection=self.connection)
        job = Job.create(func=say_hello, args=('Nick',), kwargs=dict(foo='bar'), connection=self.connection)

        # Preconditions
        self.assertIsNone(job.enqueued_at)

        # Action
        q.enqueue_job(job)

        # Postconditions
        self.assertIsNotNone(job.enqueued_at)

    def test_pop_job_id(self):
        """Popping job IDs from queues."""
        # Set up
        q = Queue(connection=self.connection)
        uuid = '112188ae-4e9d-4a5b-a5b3-f26f2cb054da'
        q.push_job_id(uuid)

        # Pop it off the queue...
        self.assertEqual(q.count, 1)
        self.assertEqual(q.pop_job_id(), uuid)

        # ...and assert the queue count when down
        self.assertEqual(q.count, 0)

    def test_dequeue_any(self):
        """Fetching work from any given queue."""
        fooq = Queue('foo', connection=self.connection)
        barq = Queue('bar', connection=self.connection)

        self.assertRaises(ValueError, Queue.dequeue_any, [fooq, barq], timeout=0, connection=self.connection)

        self.assertEqual(Queue.dequeue_any([fooq, barq], connection=self.connection, timeout=None), None)

        # Enqueue a single item
        barq.enqueue(say_hello)
        job, queue = Queue.dequeue_any([fooq, barq], connection=self.connection, timeout=None)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(queue, barq)

        # Enqueue items on both queues
        barq.enqueue(say_hello, 'for Bar')
        fooq.enqueue(say_hello, 'for Foo')

        job, queue = Queue.dequeue_any([fooq, barq], connection=self.connection, timeout=None)
        self.assertEqual(queue, fooq)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, fooq.name)
        self.assertEqual(job.args[0], 'for Foo', 'Foo should be dequeued first.')

        job, queue = Queue.dequeue_any([fooq, barq], connection=self.connection, timeout=None)
        self.assertEqual(queue, barq)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, barq.name)
        self.assertEqual(job.args[0], 'for Bar', 'Bar should be dequeued second.')

    @unittest.skipIf(get_version(Redis()) < (6, 2, 0), 'Skip if Redis server < 6.2.0')
    def test_dequeue_any_reliable(self):
        """Dequeueing job from a single queue moves job to intermediate queue."""
        foo_queue = Queue('foo', connection=self.connection)
        job_1 = foo_queue.enqueue(say_hello)
        self.assertRaises(ValueError, Queue.dequeue_any, [foo_queue], timeout=0, connection=self.connection)

        # Job ID is not in intermediate queue
        self.assertIsNone(self.connection.lpos(foo_queue.intermediate_queue_key, job_1.id))
        job, queue = Queue.dequeue_any([foo_queue], timeout=None, connection=self.connection)
        self.assertEqual(queue, foo_queue)
        self.assertEqual(job.func, say_hello)
        # After job is dequeued, the job ID is in the intermediate queue
        self.assertEqual(self.connection.lpos(foo_queue.intermediate_queue_key, job.id), 0)

        # Test the blocking version
        foo_queue.enqueue(say_hello)
        job, queue = Queue.dequeue_any([foo_queue], timeout=1, connection=self.connection)
        self.assertEqual(queue, foo_queue)
        self.assertEqual(job.func, say_hello)
        # After job is dequeued, the job ID is in the intermediate queue
        self.assertEqual(self.connection.lpos(foo_queue.intermediate_queue_key, job.id), 1)

    @unittest.skipIf(get_version(Redis()) < (6, 2, 0), 'Skip if Redis server < 6.2.0')
    def test_intermediate_queue(self):
        """Job should be stuck in intermediate queue if execution fails after dequeued."""
        queue = Queue('foo', connection=self.connection)
        job = queue.enqueue(say_hello)

        # If job execution fails after it's dequeued, job should be in the intermediate queue
        # # and it's status is still QUEUED
        with patch.object(Worker, 'execute_job'):
            # mocked.execute_job.side_effect = Exception()
            worker = Worker(queue, connection=self.connection)
            worker.work(burst=True)

            # Job status is still QUEUED even though it's already dequeued
            self.assertEqual(job.get_status(refresh=True), JobStatus.QUEUED)
            self.assertFalse(job.id in queue.get_job_ids())
            self.assertIsNotNone(self.connection.lpos(queue.intermediate_queue_key, job.id))

    def test_dequeue_any_ignores_nonexisting_jobs(self):
        """Dequeuing (from any queue) silently ignores non-existing jobs."""

        q = Queue('low', connection=self.connection)
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)

        # Dequeue simply ignores the missing job and returns None
        self.assertEqual(q.count, 1)
        self.assertEqual(
            Queue.dequeue_any(
                [Queue(connection=self.connection), Queue('low', connection=self.connection)],
                timeout=None,
                connection=self.connection,
            ),
            None,
        )
        self.assertEqual(q.count, 0)

    def test_enqueue_with_ttl(self):
        """Negative TTL value is not allowed"""
        queue = Queue(connection=self.connection)
        self.assertRaises(ValueError, queue.enqueue, echo, 1, ttl=0)
        self.assertRaises(ValueError, queue.enqueue, echo, 1, ttl=-1)

    def test_enqueue_sets_status(self):
        """Enqueueing a job sets its status to "queued"."""
        q = Queue(connection=self.connection)
        job = q.enqueue(say_hello)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_enqueue_meta_arg(self):
        """enQueue(connection=self.connection) can set the job.meta contents."""
        q = Queue(connection=self.connection)
        job = q.enqueue(say_hello, meta={'foo': 'bar', 'baz': 42})
        self.assertEqual(job.meta['foo'], 'bar')
        self.assertEqual(job.meta['baz'], 42)

    def test_enqueue_with_failure_ttl(self):
        """enQueue(connection=self.connection) properly sets job.failure_ttl"""
        q = Queue(connection=self.connection)
        job = q.enqueue(say_hello, failure_ttl=10)
        job.refresh()
        self.assertEqual(job.failure_ttl, 10)

    def test_job_timeout(self):
        """Timeout can be passed via job_timeout argument"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(echo, 1, job_timeout=15)
        self.assertEqual(job.timeout, 15)

        # Not passing job_timeout will use queue._default_timeout
        job = queue.enqueue(echo, 1)
        self.assertEqual(job.timeout, queue._default_timeout)

        # job_timeout = 0 is not allowed
        self.assertRaises(ValueError, queue.enqueue, echo, 1, job_timeout=0)

    def test_default_timeout(self):
        """Timeout can be passed via job_timeout argument"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(echo, 1)
        self.assertEqual(job.timeout, queue.DEFAULT_TIMEOUT)

        job = Job.create(func=echo, connection=self.connection)
        job = queue.enqueue_job(job)
        self.assertEqual(job.timeout, queue.DEFAULT_TIMEOUT)

        queue = Queue(connection=self.connection, default_timeout=15)
        job = queue.enqueue(echo, 1)
        self.assertEqual(job.timeout, 15)

        job = Job.create(func=echo, connection=self.connection)
        job = queue.enqueue_job(job)
        self.assertEqual(job.timeout, 15)

    def test_synchronous_timeout(self):
        queue = Queue(is_async=False, connection=self.connection)
        self.assertFalse(queue.is_async)

        no_expire_job = queue.enqueue(echo, result_ttl=-1)
        self.assertEqual(queue.connection.ttl(no_expire_job.key), -1)

        delete_job = queue.enqueue(echo, result_ttl=0)
        self.assertEqual(queue.connection.ttl(delete_job.key), -2)

        keep_job = queue.enqueue(echo, result_ttl=100)
        self.assertLessEqual(queue.connection.ttl(keep_job.key), 100)

    def test_enqueue_explicit_args(self):
        """enQueue(connection=self.connection) works for both implicit/explicit args."""
        q = Queue(connection=self.connection)

        # Implicit args/kwargs mode
        job = q.enqueue(echo, 1, job_timeout=1, result_ttl=1, bar='baz')
        self.assertEqual(job.timeout, 1)
        self.assertEqual(job.result_ttl, 1)
        self.assertEqual(job.perform(), ((1,), {'bar': 'baz'}))

        # Explicit kwargs mode
        kwargs = {
            'timeout': 1,
            'result_ttl': 1,
        }
        job = q.enqueue(echo, job_timeout=2, result_ttl=2, args=[1], kwargs=kwargs)
        self.assertEqual(job.timeout, 2)
        self.assertEqual(job.result_ttl, 2)
        self.assertEqual(job.perform(), ((1,), {'timeout': 1, 'result_ttl': 1}))

        # Explicit args and kwargs should also work with enqueue_at
        time = datetime.now(timezone.utc) + timedelta(seconds=10)
        job = q.enqueue_at(time, echo, job_timeout=2, result_ttl=2, args=[1], kwargs=kwargs)
        self.assertEqual(job.timeout, 2)
        self.assertEqual(job.result_ttl, 2)
        self.assertEqual(job.perform(), ((1,), {'timeout': 1, 'result_ttl': 1}))

        # Positional arguments is not allowed if explicit args and kwargs are used
        self.assertRaises(Exception, q.enqueue, echo, 1, kwargs=kwargs)

    def test_all_queues(self):
        """All queues"""
        q1 = Queue('first-queue', connection=self.connection)
        q2 = Queue('second-queue', connection=self.connection)
        q3 = Queue('third-queue', connection=self.connection)

        # Ensure a queue is added only once a job is enqueued
        self.assertEqual(len(Queue.all(connection=self.connection)), 0)
        q1.enqueue(say_hello)
        self.assertEqual(len(Queue.all(connection=self.connection)), 1)

        # Ensure this holds true for multiple queues
        q2.enqueue(say_hello)
        q3.enqueue(say_hello)
        names = [q.name for q in Queue.all(connection=self.connection)]
        self.assertEqual(len(Queue.all(connection=self.connection)), 3)

        # Verify names
        self.assertTrue('first-queue' in names)
        self.assertTrue('second-queue' in names)
        self.assertTrue('third-queue' in names)

        # Now empty two queues
        w = Worker([q2, q3], connection=self.connection)
        w.work(burst=True)

        # Queue.all(connection=self.connection) should still report the empty queues
        self.assertEqual(len(Queue.all(connection=self.connection)), 3)

    def test_all_custom_job(self):
        class CustomJob(Job):
            pass

        q = Queue('all-queue', connection=self.connection)
        q.enqueue(say_hello)
        queues = Queue.all(job_class=CustomJob, connection=self.connection)
        self.assertEqual(len(queues), 1)
        self.assertIs(queues[0].job_class, CustomJob)

    def test_from_queue_key(self):
        """Ensure being able to get a Queue instance manually from Redis"""
        q = Queue(connection=self.connection)
        key = Queue.redis_queue_namespace_prefix + 'default'
        reverse_q = Queue.from_queue_key(key, connection=self.connection)
        self.assertEqual(q, reverse_q)

    def test_from_queue_key_error(self):
        """Ensure that an exception is raised if the queue prefix is wrong"""
        key = 'some:weird:prefix:' + 'default'
        self.assertRaises(ValueError, Queue.from_queue_key, key, connection=self.connection)

    def test_enqueue_dependents(self):
        """Enqueueing dependent jobs pushes all jobs in the depends set to the queue
        and removes them from DeferredJobQueue."""
        q = Queue(connection=self.connection)
        parent_job = Job.create(func=say_hello, connection=self.connection)
        parent_job.save()
        job_1 = q.enqueue(say_hello, depends_on=parent_job)
        job_2 = q.enqueue(say_hello, depends_on=parent_job)

        registry = DeferredJobRegistry(q.name, connection=self.connection)

        parent_job.set_status(JobStatus.FINISHED)

        self.assertEqual(set(registry.get_job_ids()), set([job_1.id, job_2.id]))
        # After dependents is enqueued, job_1 and job_2 should be in queue
        self.assertEqual(q.job_ids, [])
        q.enqueue_dependents(parent_job)
        self.assertEqual(set(q.job_ids), set([job_2.id, job_1.id]))
        self.assertFalse(self.connection.exists(parent_job.dependents_key))

        # DeferredJobRegistry should also be empty
        self.assertEqual(registry.get_job_ids(), [])

    def test_enqueue_dependents_on_multiple_queues(self):
        """Enqueueing dependent jobs on multiple queues pushes jobs in the queues
        and removes them from DeferredJobRegistry for each different queue."""
        q_1 = Queue("queue_1", connection=self.connection)
        q_2 = Queue("queue_2", connection=self.connection)
        parent_job = Job.create(func=say_hello, connection=self.connection)
        parent_job.save()
        job_1 = q_1.enqueue(say_hello, depends_on=parent_job)
        job_2 = q_2.enqueue(say_hello, depends_on=parent_job)

        # Each queue has its own DeferredJobRegistry
        registry_1 = DeferredJobRegistry(q_1.name, connection=self.connection)
        self.assertEqual(set(registry_1.get_job_ids()), set([job_1.id]))
        registry_2 = DeferredJobRegistry(q_2.name, connection=self.connection)

        parent_job.set_status(JobStatus.FINISHED)

        self.assertEqual(set(registry_2.get_job_ids()), set([job_2.id]))

        # After dependents is enqueued, job_1 on queue_1 and
        # job_2 should be in queue_2
        self.assertEqual(q_1.job_ids, [])
        self.assertEqual(q_2.job_ids, [])
        q_1.enqueue_dependents(parent_job)
        q_2.enqueue_dependents(parent_job)
        self.assertEqual(set(q_1.job_ids), set([job_1.id]))
        self.assertEqual(set(q_2.job_ids), set([job_2.id]))
        self.assertFalse(self.connection.exists(parent_job.dependents_key))

        # DeferredJobRegistry should also be empty
        self.assertEqual(registry_1.get_job_ids(), [])
        self.assertEqual(registry_2.get_job_ids(), [])

    def test_enqueue_job_with_dependency(self):
        """Jobs are enqueued only when their dependencies are finished."""
        # Job with unfinished dependency is not immediately enqueued
        parent_job = Job.create(func=say_hello, connection=self.connection)
        parent_job.save()
        q = Queue(connection=self.connection)
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(JobStatus.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, Queue.DEFAULT_TIMEOUT)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_enqueue_job_with_dependency_and_pipeline(self):
        """Jobs are enqueued only when their dependencies are finished, and by the caller when passing a pipeline."""
        # Job with unfinished dependency is not immediately enqueued
        parent_job = Job.create(func=say_hello, connection=self.connection)
        parent_job.save()
        q = Queue(connection=self.connection)
        with q.connection.pipeline() as pipe:
            job = q.enqueue_call(say_hello, depends_on=parent_job, pipeline=pipe)
            self.assertEqual(q.job_ids, [])
            self.assertEqual(job.get_status(refresh=False), JobStatus.DEFERRED)
            # Not in registry before execute, since passed in pipeline
            self.assertEqual(len(q.deferred_job_registry), 0)
            pipe.execute()
            # Only in registry after execute, since passed in pipeline
        self.assertEqual(len(q.deferred_job_registry), 1)

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(JobStatus.FINISHED)
        parent_job.save()
        with q.connection.pipeline() as pipe:
            job = q.enqueue_call(say_hello, depends_on=parent_job, pipeline=pipe)
            # Pre execute conditions
            self.assertEqual(q.job_ids, [])
            self.assertEqual(job.timeout, Queue.DEFAULT_TIMEOUT)
            self.assertEqual(job.get_status(refresh=False), JobStatus.QUEUED)
            pipe.execute()
        # Post execute conditions
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, Queue.DEFAULT_TIMEOUT)
        self.assertEqual(job.get_status(refresh=False), JobStatus.QUEUED)

    def test_enqueue_job_with_no_dependency_prior_watch_and_pipeline(self):
        """Jobs are enqueued only when their dependencies are finished, and by the caller when passing a pipeline."""
        q = Queue(connection=self.connection)
        with q.connection.pipeline() as pipe:
            pipe.watch(b'fake_key')  # Test watch then enqueue
            job = q.enqueue_call(say_hello, pipeline=pipe)
            self.assertEqual(q.job_ids, [])
            self.assertEqual(job.get_status(refresh=False), JobStatus.QUEUED)
            # Not in queue before execute, since passed in pipeline
            self.assertEqual(len(q), 0)
            # Make sure modifying key doesn't cause issues, if in multi mode won't fail
            pipe.set(b'fake_key', b'fake_value')
            pipe.execute()
            # Only in registry after execute, since passed in pipeline
        self.assertEqual(len(q), 1)

    def test_enqueue_many_internal_pipeline(self):
        """Jobs should be enqueued in bulk with an internal pipeline, enqueued in order provided
        (but at_front still applies)"""
        # Job with unfinished dependency is not immediately enqueued
        q = Queue(connection=self.connection)
        job_1_data = Queue.prepare_data(say_hello, job_id='fake_job_id_1', at_front=False)
        job_2_data = Queue.prepare_data(say_hello, job_id='fake_job_id_2', at_front=False)
        job_3_data = Queue.prepare_data(say_hello, job_id='fake_job_id_3', at_front=True)
        jobs = q.enqueue_many(
            [job_1_data, job_2_data, job_3_data],
        )
        for job in jobs:
            self.assertEqual(job.get_status(refresh=False), JobStatus.QUEUED)
        # Only in registry after execute, since passed in pipeline
        self.assertEqual(len(q), 3)
        self.assertEqual(q.job_ids, ['fake_job_id_3', 'fake_job_id_1', 'fake_job_id_2'])

    def test_enqueue_many_with_passed_pipeline(self):
        """Jobs should be enqueued in bulk with a passed pipeline, enqueued in order provided
        (but at_front still applies)"""
        # Job with unfinished dependency is not immediately enqueued
        q = Queue(connection=self.connection)
        with q.connection.pipeline() as pipe:
            job_1_data = Queue.prepare_data(say_hello, job_id='fake_job_id_1', at_front=False)
            job_2_data = Queue.prepare_data(say_hello, job_id='fake_job_id_2', at_front=False)
            job_3_data = Queue.prepare_data(say_hello, job_id='fake_job_id_3', at_front=True)
            jobs = q.enqueue_many([job_1_data, job_2_data, job_3_data], pipeline=pipe)
            self.assertEqual(q.job_ids, [])
            for job in jobs:
                self.assertEqual(job.get_status(refresh=False), JobStatus.QUEUED)
            pipe.execute()
            # Only in registry after execute, since passed in pipeline
            self.assertEqual(len(q), 3)
            self.assertEqual(q.job_ids, ['fake_job_id_3', 'fake_job_id_1', 'fake_job_id_2'])

    def test_enqueue_job_with_dependency_by_id(self):
        """Can specify job dependency with job object or job id."""
        parent_job = Job.create(func=say_hello, connection=self.connection)
        parent_job.save()

        q = Queue(connection=self.connection)
        q.enqueue_call(say_hello, depends_on=parent_job.id)
        self.assertEqual(q.job_ids, [])

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(JobStatus.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job.id)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, Queue.DEFAULT_TIMEOUT)

    def test_enqueue_job_with_dependency_and_timeout(self):
        """Jobs remember their timeout when enqueued as a dependency."""
        # Job with unfinished dependency is not immediately enqueued
        parent_job = Job.create(func=say_hello, connection=self.connection)
        parent_job.save()
        q = Queue(connection=self.connection)
        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=123)
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.timeout, 123)

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(JobStatus.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=123)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, 123)

    def test_enqueue_job_with_multiple_queued_dependencies(self):
        parent_jobs = [Job.create(func=say_hello, connection=self.connection) for _ in range(2)]

        for job in parent_jobs:
            job._status = JobStatus.QUEUED
            job.save()

        q = Queue(connection=self.connection)
        with patch('rq.queue.Job.create', new=MultipleDependencyJob.create):
            job = q.enqueue(say_hello, depends_on=parent_jobs[0], _dependency_ids=[job.id for job in parent_jobs])
            self.assertEqual(job.get_status(), JobStatus.DEFERRED)
            self.assertEqual(q.job_ids, [])
            self.assertEqual(job.fetch_dependencies(), parent_jobs)

    def test_enqueue_job_with_multiple_finished_dependencies(self):
        parent_jobs = [Job.create(func=say_hello, connection=self.connection) for _ in range(2)]

        for job in parent_jobs:
            job._status = JobStatus.FINISHED
            job.save()

        q = Queue(connection=self.connection)
        with patch('rq.queue.Job.create', new=MultipleDependencyJob.create):
            job = q.enqueue(say_hello, depends_on=parent_jobs[0], _dependency_ids=[job.id for job in parent_jobs])
            self.assertEqual(job.get_status(), JobStatus.QUEUED)
            self.assertEqual(q.job_ids, [job.id])
            self.assertEqual(job.fetch_dependencies(), parent_jobs)

    def test_enqueues_dependent_if_other_dependencies_finished(self):
        parent_jobs = [Job.create(func=say_hello, connection=self.connection) for _ in range(3)]

        parent_jobs[0]._status = JobStatus.STARTED
        parent_jobs[0].save()

        parent_jobs[1]._status = JobStatus.FINISHED
        parent_jobs[1].save()

        parent_jobs[2]._status = JobStatus.FINISHED
        parent_jobs[2].save()

        q = Queue(connection=self.connection)
        with patch('rq.queue.Job.create', new=MultipleDependencyJob.create):
            # dependent job deferred, b/c parent_job 0 is still 'started'
            dependent_job = q.enqueue(
                say_hello, depends_on=parent_jobs[0], _dependency_ids=[job.id for job in parent_jobs]
            )
            self.assertEqual(dependent_job.get_status(), JobStatus.DEFERRED)

        # now set parent job 0 to 'finished'
        parent_jobs[0].set_status(JobStatus.FINISHED)

        q.enqueue_dependents(parent_jobs[0])
        self.assertEqual(dependent_job.get_status(), JobStatus.QUEUED)
        self.assertEqual(q.job_ids, [dependent_job.id])

    def test_does_not_enqueue_dependent_if_other_dependencies_not_finished(self):
        started_dependency = Job.create(func=say_hello, status=JobStatus.STARTED, connection=self.connection)
        started_dependency.save()

        queued_dependency = Job.create(func=say_hello, status=JobStatus.QUEUED, connection=self.connection)
        queued_dependency.save()

        q = Queue(connection=self.connection)
        with patch('rq.queue.Job.create', new=MultipleDependencyJob.create):
            dependent_job = q.enqueue(
                say_hello,
                depends_on=[started_dependency],
                _dependency_ids=[started_dependency.id, queued_dependency.id],
            )
            self.assertEqual(dependent_job.get_status(), JobStatus.DEFERRED)

        q.enqueue_dependents(started_dependency)
        self.assertEqual(dependent_job.get_status(), JobStatus.DEFERRED)
        self.assertEqual(q.job_ids, [])

    def test_fetch_job_successful(self):
        """Fetch a job from a queue."""
        q = Queue('example', connection=self.connection)
        job_orig = q.enqueue(say_hello)
        job_fetch: Job = q.fetch_job(job_orig.id)  # type: ignore
        self.assertIsNotNone(job_fetch)
        self.assertEqual(job_orig.id, job_fetch.id)
        self.assertEqual(job_orig.description, job_fetch.description)

    def test_fetch_job_missing(self):
        """Fetch a job from a queue which doesn't exist."""
        q = Queue('example', connection=self.connection)
        job = q.fetch_job('123')
        self.assertIsNone(job)

    def test_fetch_job_different_queue(self):
        """Fetch a job from a queue which is in a different queue."""
        q1 = Queue('example1', connection=self.connection)
        q2 = Queue('example2', connection=self.connection)
        job_orig = q1.enqueue(say_hello)
        job_fetch = q2.fetch_job(job_orig.id)
        self.assertIsNone(job_fetch)

        job_fetch = q1.fetch_job(job_orig.id)
        self.assertIsNotNone(job_fetch)

    def test_getting_registries(self):
        """Getting job registries from queue object"""
        queue = Queue('example', connection=self.connection)
        self.assertEqual(queue.scheduled_job_registry, ScheduledJobRegistry(queue=queue))
        self.assertEqual(queue.started_job_registry, StartedJobRegistry(queue=queue))
        self.assertEqual(queue.failed_job_registry, FailedJobRegistry(queue=queue))
        self.assertEqual(queue.deferred_job_registry, DeferredJobRegistry(queue=queue))
        self.assertEqual(queue.finished_job_registry, FinishedJobRegistry(queue=queue))
        self.assertEqual(queue.canceled_job_registry, CanceledJobRegistry(queue=queue))

    def test_getting_registries_with_serializer(self):
        """Getting job registries from queue object (with custom serializer)"""
        queue = Queue('example', connection=self.connection, serializer=JSONSerializer)
        self.assertEqual(queue.scheduled_job_registry, ScheduledJobRegistry(queue=queue))
        self.assertEqual(queue.started_job_registry, StartedJobRegistry(queue=queue))
        self.assertEqual(queue.failed_job_registry, FailedJobRegistry(queue=queue))
        self.assertEqual(queue.deferred_job_registry, DeferredJobRegistry(queue=queue))
        self.assertEqual(queue.finished_job_registry, FinishedJobRegistry(queue=queue))
        self.assertEqual(queue.canceled_job_registry, CanceledJobRegistry(queue=queue))

        # Make sure we don't use default when queue has custom
        self.assertEqual(queue.scheduled_job_registry.serializer, JSONSerializer)
        self.assertEqual(queue.started_job_registry.serializer, JSONSerializer)
        self.assertEqual(queue.failed_job_registry.serializer, JSONSerializer)
        self.assertEqual(queue.deferred_job_registry.serializer, JSONSerializer)
        self.assertEqual(queue.finished_job_registry.serializer, JSONSerializer)
        self.assertEqual(queue.canceled_job_registry.serializer, JSONSerializer)

    def test_enqueue_with_retry(self):
        """Enqueueing with retry_strategy works"""
        queue = Queue('example', connection=self.connection)
        job = queue.enqueue(say_hello, retry=Retry(max=3, interval=5))

        job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.retry_intervals, [5])


class TestJobScheduling(RQTestCase):
    def test_enqueue_at(self):
        """enqueue_at() creates a job in ScheduledJobRegistry"""
        queue = Queue(connection=self.connection)
        scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=10)
        job = queue.enqueue_at(scheduled_time, say_hello)
        registry = ScheduledJobRegistry(queue=queue)
        self.assertIn(job, registry)
        self.assertTrue(registry.get_expiration_time(job), scheduled_time)
