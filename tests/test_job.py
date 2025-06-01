import json
import queue
import time
import unittest
import zlib
from datetime import datetime, timezone
from pickle import dumps, loads
from uuid import uuid4

from redis import Redis

from rq.defaults import CALLBACK_TIMEOUT
from rq.exceptions import DeserializationError, InvalidJobOperation, NoSuchJobError
from rq.executions import Execution
from rq.job import Callback, Job, JobStatus, cancel_job, get_current_job
from rq.queue import Queue
from rq.registry import (
    CanceledJobRegistry,
    DeferredJobRegistry,
    FailedJobRegistry,
    FinishedJobRegistry,
    ScheduledJobRegistry,
    StartedJobRegistry,
)
from rq.serializers import JSONSerializer
from rq.utils import as_text, get_version, now, utcformat
from rq.worker import Worker
from tests import RQTestCase, fixtures


class TestJob(RQTestCase):
    def test_unicode(self):
        """Unicode in job description [issue405]"""
        job = Job.create(
            'myfunc',
            args=[12, '☃'],
            kwargs=dict(snowman='☃', null=None),
            connection=self.connection,
        )
        self.assertEqual(
            job.description,
            "myfunc(12, '☃', null=None, snowman='☃')",
        )

    def test_create_empty_job(self):
        """Creation of new empty jobs."""
        job = Job(uuid4().hex, connection=self.connection)
        job.description = 'test job'

        # Jobs have a random UUID and a creation date
        self.assertIsNotNone(job.id)
        self.assertIsNotNone(job.created_at)
        self.assertEqual(str(job), f'<Job {job.id}: test job>')

        # ...and nothing else
        self.assertEqual(job.origin, '')
        self.assertIsNone(job.enqueued_at)
        self.assertIsNone(job.started_at)
        self.assertIsNone(job.ended_at)
        self.assertIsNone(job.result)
        self.assertIsNone(job._exc_info)

        with self.assertRaises(DeserializationError):
            job.func
        with self.assertRaises(DeserializationError):
            job.instance
        with self.assertRaises(DeserializationError):
            job.args
        with self.assertRaises(DeserializationError):
            job.kwargs

    def test_create_param_errors(self):
        """Creation of jobs may result in errors"""
        self.assertRaises(TypeError, Job.create, fixtures.say_hello, args='string')
        self.assertRaises(TypeError, Job.create, fixtures.say_hello, kwargs='string')
        self.assertRaises(TypeError, Job.create, func=42)

    def test_create_typical_job(self):
        """Creation of jobs for function calls."""
        job = Job.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2), connection=self.connection)

        # Jobs have a random UUID
        self.assertIsNotNone(job.id)
        self.assertIsNotNone(job.created_at)
        self.assertIsNotNone(job.description)
        self.assertIsNone(job.instance)

        # Job data is set...
        self.assertEqual(job.func, fixtures.some_calculation)
        self.assertEqual(job.args, (3, 4))
        self.assertEqual(job.kwargs, {'z': 2})

        # ...but metadata is not
        self.assertEqual(job.origin, '')
        self.assertIsNone(job.enqueued_at)
        self.assertIsNone(job.result)

    def test_create_instance_method_job(self):
        """Creation of jobs for instance methods."""
        n = fixtures.Number(2)
        job = Job.create(func=n.div, args=(4,), connection=self.connection)

        # Job data is set
        self.assertEqual(job.func, n.div)
        self.assertEqual(job.instance, n)
        self.assertEqual(job.args, (4,))

    def test_create_job_with_serializer(self):
        """Creation of jobs with serializer for instance methods."""
        # Test using json serializer
        n = fixtures.Number(2)
        job = Job.create(func=n.div, args=(4,), serializer=json, connection=self.connection)

        self.assertIsNotNone(job.serializer)
        self.assertEqual(job.func, n.div)
        self.assertEqual(job.instance, n)
        self.assertEqual(job.args, (4,))

    def test_create_job_from_string_function(self):
        """Creation of jobs using string specifier."""
        job = Job.create(func='tests.fixtures.say_hello', args=('World',), connection=self.connection)

        # Job data is set
        self.assertEqual(job.func, fixtures.say_hello)
        self.assertIsNone(job.instance)
        self.assertEqual(job.args, ('World',))

    def test_create_job_from_callable_class(self):
        """Creation of jobs using a callable class specifier."""
        kallable = fixtures.CallableObject()
        job = Job.create(func=kallable, connection=self.connection)

        self.assertEqual(job.func, kallable.__call__)
        self.assertEqual(job.instance, kallable)

    def test_job_properties_set_data_property(self):
        """Data property gets derived from the job tuple."""
        job = Job(id=uuid4().hex, connection=self.connection)
        job.func_name = 'foo'
        fname, instance, args, kwargs = loads(job.data)

        self.assertEqual(fname, job.func_name)
        self.assertEqual(instance, None)
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {})

    def test_data_property_sets_job_properties(self):
        """Job tuple gets derived lazily from data property."""
        job = Job(id=uuid4().hex, connection=self.connection)
        job.data = dumps(('foo', None, (1, 2, 3), {'bar': 'qux'}))

        self.assertEqual(job.func_name, 'foo')
        self.assertEqual(job.instance, None)
        self.assertEqual(job.args, (1, 2, 3))
        self.assertEqual(job.kwargs, {'bar': 'qux'})

    def test_save(self):  # noqa
        """Storing jobs."""
        job = Job.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2), connection=self.connection)

        # Saving creates a Redis hash
        self.assertEqual(self.connection.exists(job.key), False)
        job.save()
        self.assertEqual(self.connection.type(job.key), b'hash')

        # Saving writes pickled job data
        unpickled_data = loads(zlib.decompress(self.connection.hget(job.key, 'data')))
        self.assertEqual(unpickled_data[0], 'tests.fixtures.some_calculation')

    def test_fetch(self):
        """Fetching jobs."""
        # Prepare test
        self.connection.hset(
            'rq:job:some_id', 'data', "(S'tests.fixtures.some_calculation'\nN(I3\nI4\nt(dp1\nS'z'\nI2\nstp2\n."
        )
        self.connection.hset('rq:job:some_id', 'created_at', '2012-02-07T22:13:24.123456Z')

        # Fetch returns a job
        job = Job.fetch('some_id', connection=self.connection)
        self.assertEqual(job.id, 'some_id')
        self.assertEqual(job.func_name, 'tests.fixtures.some_calculation')
        self.assertIsNone(job.instance)
        self.assertEqual(job.args, (3, 4))
        self.assertEqual(job.kwargs, dict(z=2))
        self.assertEqual(job.created_at, datetime(2012, 2, 7, 22, 13, 24, 123456, tzinfo=timezone.utc))

        # Job.fetch also works with execution IDs
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.prepare_execution(job=job)
        worker.prepare_job_execution(job=job)
        execution = worker.execution
        self.assertEqual(Job.fetch(execution.composite_key, self.connection), job)  # type: ignore
        self.assertEqual(Job.fetch(job.id, self.connection), job)

    def test_fetch_many(self):
        """Fetching many jobs at once."""
        data = {
            'func': fixtures.some_calculation,
            'args': (3, 4),
            'kwargs': dict(z=2),
            'connection': self.connection,
        }
        job = Job.create(**data)
        job.save()

        job2 = Job.create(**data)
        job2.save()

        jobs = Job.fetch_many([job.id, job2.id, 'invalid_id'], self.connection)
        self.assertEqual(jobs, [job, job2, None])

        # Job.fetch_many also works with execution IDs
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.prepare_execution(job=job)
        worker.prepare_job_execution(job=job)
        execution = worker.execution
        self.assertEqual(Job.fetch_many([execution.composite_key], self.connection), [job])  # type: ignore
        self.assertEqual(Job.fetch_many([job.id], self.connection), [job])

    def test_persistence_of_empty_jobs(self):  # noqa
        """Storing empty jobs."""
        job = Job(id='foo', connection=self.connection)
        with self.assertRaises(ValueError):
            job.save()

    def test_persistence_of_typical_jobs(self):
        """Storing typical jobs."""
        job = Job.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2), connection=self.connection)
        job.save()

        stored_date = self.connection.hget(job.key, 'created_at').decode('utf-8')
        self.assertEqual(stored_date, utcformat(job.created_at))

        # ... and no other keys are stored
        self.assertEqual(
            {
                b'created_at',
                b'data',
                b'description',
                b'ended_at',
                b'last_heartbeat',
                b'started_at',
                b'worker_name',
                b'success_callback_name',
                b'failure_callback_name',
                b'stopped_callback_name',
                b'group_id',
            },
            set(self.connection.hkeys(job.key)),
        )

        self.assertEqual(job.last_heartbeat, None)
        self.assertEqual(job.last_heartbeat, None)

        ts = now()
        job.heartbeat(ts, 0)
        self.assertEqual(job.last_heartbeat, ts)

    def test_persistence_of_parent_job(self):
        """Storing jobs with parent job, either instance or key."""
        parent_job = Job.create(func=fixtures.some_calculation, connection=self.connection)
        parent_job.save()
        job = Job.create(func=fixtures.some_calculation, depends_on=parent_job, connection=self.connection)
        job.save()
        stored_job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(stored_job._dependency_id, parent_job.id)
        self.assertEqual(stored_job._dependency_ids, [parent_job.id])
        self.assertEqual(stored_job.dependency.id, parent_job.id)
        self.assertEqual(stored_job.dependency, parent_job)

        job = Job.create(func=fixtures.some_calculation, depends_on=parent_job.id, connection=self.connection)
        job.save()
        stored_job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(stored_job._dependency_id, parent_job.id)
        self.assertEqual(stored_job._dependency_ids, [parent_job.id])
        self.assertEqual(stored_job.dependency.id, parent_job.id)
        self.assertEqual(stored_job.dependency, parent_job)

    def test_persistence_of_callbacks(self):
        """Storing jobs with success and/or failure callbacks."""
        job = Job.create(
            func=fixtures.some_calculation,
            on_success=Callback(fixtures.say_hello, timeout=10),
            on_failure=fixtures.say_pid,
            on_stopped=fixtures.say_hello,
            connection=self.connection,
        )  # deprecated callable
        job.save()
        stored_job = Job.fetch(job.id, connection=self.connection)

        self.assertEqual(fixtures.say_hello, stored_job.success_callback)
        self.assertEqual(10, stored_job.success_callback_timeout)
        self.assertEqual(fixtures.say_pid, stored_job.failure_callback)
        self.assertEqual(fixtures.say_hello, stored_job.stopped_callback)
        self.assertEqual(CALLBACK_TIMEOUT, stored_job.failure_callback_timeout)
        self.assertEqual(CALLBACK_TIMEOUT, stored_job.stopped_callback_timeout)

        # None(s)
        job = Job.create(func=fixtures.some_calculation, on_failure=None, connection=self.connection)
        job.save()
        stored_job = Job.fetch(job.id, connection=self.connection)
        self.assertIsNone(stored_job.success_callback)
        self.assertEqual(CALLBACK_TIMEOUT, job.success_callback_timeout)  # timeout should be never none
        self.assertEqual(CALLBACK_TIMEOUT, stored_job.success_callback_timeout)
        self.assertIsNone(stored_job.failure_callback)
        self.assertEqual(CALLBACK_TIMEOUT, job.failure_callback_timeout)  # timeout should be never none
        self.assertEqual(CALLBACK_TIMEOUT, stored_job.failure_callback_timeout)
        self.assertEqual(CALLBACK_TIMEOUT, job.stopped_callback_timeout)  # timeout should be never none
        self.assertIsNone(stored_job.stopped_callback)

    def test_store_then_fetch(self):
        """Store, then fetch."""
        job = Job.create(
            func=fixtures.some_calculation, timeout=3600, args=(3, 4), kwargs=dict(z=2), connection=self.connection
        )
        job.save()

        job2 = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.func, job2.func)
        self.assertEqual(job.args, job2.args)
        self.assertEqual(job.kwargs, job2.kwargs)
        self.assertEqual(job.timeout, job2.timeout)

        # Mathematical equation
        self.assertEqual(job, job2)

    def test_fetching_can_fail(self):
        """Fetching fails for non-existing jobs."""
        with self.assertRaises(NoSuchJobError):
            Job.fetch('b4a44d44-da16-4620-90a6-798e8cd72ca0', connection=self.connection)

    def test_fetching_unreadable_data(self):
        """Fetching succeeds on unreadable data, but lazy props fail."""
        # Set up
        job = Job.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2), connection=self.connection)
        job.save()

        # Just replace the data hkey with some random noise
        self.connection.hset(job.key, 'data', 'this is no pickle string')
        job.refresh()

        for attr in ('func_name', 'instance', 'args', 'kwargs'):
            with self.assertRaises(Exception):
                getattr(job, attr)

    def test_job_is_unimportable(self):
        """Jobs that cannot be imported throw exception on access."""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job.save()

        # Now slightly modify the job to make it unimportable (this is
        # equivalent to a worker not having the most up-to-date source code
        # and unable to import the function)
        job_data = job.data
        unimportable_data = job_data.replace(b'say_hello', b'nay_hello')

        self.connection.hset(job.key, 'data', zlib.compress(unimportable_data))

        job.refresh()
        with self.assertRaises(ValueError):
            job.func  # accessing the func property should fail

    def test_compressed_exc_info_handling(self):
        """Jobs handle both compressed and uncompressed exc_info"""
        exception_string = 'Some exception'

        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job._exc_info = exception_string
        job.save()

        # exc_info is stored in compressed format
        exc_info = self.connection.hget(job.key, 'exc_info')
        self.assertEqual(as_text(zlib.decompress(exc_info)), exception_string)

        job.refresh()
        self.assertEqual(job.exc_info, exception_string)

        # Uncompressed exc_info is also handled
        self.connection.hset(job.key, 'exc_info', exception_string)

        job.refresh()
        self.assertEqual(job.exc_info, exception_string)

    def test_compressed_job_data_handling(self):
        """Jobs handle both compressed and uncompressed data"""

        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job.save()

        # Job data is stored in compressed format
        job_data = job.data
        self.assertEqual(zlib.compress(job_data), self.connection.hget(job.key, 'data'))

        self.connection.hset(job.key, 'data', job_data)
        job.refresh()
        self.assertEqual(job.data, job_data)

    def test_custom_meta_is_persisted(self):
        """Additional meta data on jobs are stored persisted correctly."""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job.meta['foo'] = 'bar'
        job.save()

        raw_data = self.connection.hget(job.key, 'meta')
        self.assertEqual(loads(raw_data)['foo'], 'bar')

        job2 = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job2.meta['foo'], 'bar')

    def test_get_meta(self):
        """Test get_meta() function"""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job.meta['foo'] = 'bar'
        job.save()
        self.assertEqual(job.get_meta()['foo'], 'bar')

        # manually write different data in meta
        self.connection.hset(job.key, 'meta', dumps({'fee': 'boo'}))

        # check if refresh=False keeps old data
        self.assertEqual(job.get_meta(False)['foo'], 'bar')

        # check if meta is updated
        self.assertEqual(job.get_meta()['fee'], 'boo')

    def test_custom_meta_is_rewriten_by_save_meta(self):
        """New meta data can be stored by save_meta."""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job.save()
        serialized = job.to_dict()

        job.meta['foo'] = 'bar'
        job.save_meta()

        raw_meta = self.connection.hget(job.key, 'meta')
        self.assertEqual(loads(raw_meta)['foo'], 'bar')

        job2 = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job2.meta['foo'], 'bar')

        # nothing else was changed
        serialized2 = job2.to_dict()
        serialized2.pop('meta')
        self.assertDictEqual(serialized, serialized2)

    def test_unpickleable_result(self):
        """Unpickleable job result doesn't crash job.save() and job.refresh()"""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job._result = queue.Queue()
        job.save()

        self.assertEqual(self.connection.hget(job.key, 'result').decode('utf-8'), 'Unserializable return value')

        job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.result, 'Unserializable return value')

    def test_result_ttl_is_persisted(self):
        """Ensure that job's result_ttl is set properly"""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), result_ttl=10, connection=self.connection)
        job.save()
        Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.result_ttl, 10)

        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job.save()
        Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.result_ttl, None)

    def test_failure_ttl_is_persisted(self):
        """Ensure job.failure_ttl is set and restored properly"""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), failure_ttl=15, connection=self.connection)
        job.save()
        Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.failure_ttl, 15)

        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job.save()
        Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.failure_ttl, None)

    def test_description_is_persisted(self):
        """Ensure that job's custom description is set properly"""
        job = Job.create(
            func=fixtures.say_hello, args=('Lionel',), description='Say hello!', connection=self.connection
        )
        job.save()
        Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.description, 'Say hello!')

        # Ensure job description is constructed from function call string
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), connection=self.connection)
        job.save()
        Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.description, "tests.fixtures.say_hello('Lionel')")

    def test_prepare_for_execution(self):
        """job.prepare_for_execution works properly"""
        job = Job.create(func=fixtures.say_hello, connection=self.connection)
        job.save()
        with self.connection.pipeline() as pipeline:
            job.prepare_for_execution('worker_name', pipeline)
            pipeline.execute()
        job.refresh()
        self.assertEqual(job.worker_name, 'worker_name')
        self.assertEqual(job.get_status(), JobStatus.STARTED)
        self.assertIsNotNone(job.last_heartbeat)
        self.assertIsNotNone(job.started_at)

    def test_unset_job_status_fails(self):
        """None is an invalid status for Job."""
        job = Job.create(func=fixtures.say_hello, connection=self.connection)
        job.save()
        self.assertRaises(InvalidJobOperation, job.get_status)

    def test_job_access_outside_job_fails(self):
        """The current job is accessible only within a job context."""
        self.assertIsNone(get_current_job())

    def test_job_access_within_job_function(self):
        """The current job is accessible within the job function."""
        q = Queue(connection=self.connection)
        job = q.enqueue(fixtures.access_self)
        w = Worker([q])
        w.work(burst=True)
        # access_self calls get_current_job() and executes successfully
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_job_access_within_synchronous_job_function(self):
        queue = Queue(is_async=False, connection=self.connection)
        queue.enqueue(fixtures.access_self)

    def test_job_async_status_finished(self):
        queue = Queue(is_async=False, connection=self.connection)
        job = queue.enqueue(fixtures.say_hello)
        self.assertEqual(job.result, 'Hi there, Stranger!')
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_enqueue_job_async_status_finished(self):
        queue = Queue(is_async=False, connection=self.connection)
        job = Job.create(func=fixtures.say_hello, connection=self.connection)
        job = queue.enqueue_job(job)
        self.assertEqual(job.result, 'Hi there, Stranger!')
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_get_result_ttl(self):
        """Getting job result TTL."""
        job_result_ttl = 1
        default_ttl = 2
        job = Job.create(func=fixtures.say_hello, result_ttl=job_result_ttl, connection=self.connection)
        job.save()
        self.assertEqual(job.get_result_ttl(default_ttl=default_ttl), job_result_ttl)
        job = Job.create(func=fixtures.say_hello, connection=self.connection)
        job.save()
        self.assertEqual(job.get_result_ttl(default_ttl=default_ttl), default_ttl)

    def test_get_job_ttl(self):
        """Getting job TTL."""
        ttl = 1
        job = Job.create(func=fixtures.say_hello, ttl=ttl, connection=self.connection)
        job.save()
        self.assertEqual(job.get_ttl(), ttl)
        job = Job.create(func=fixtures.say_hello, connection=self.connection)
        job.save()
        self.assertEqual(job.get_ttl(), None)

    def test_ttl_via_enqueue(self):
        ttl = 1
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.say_hello, ttl=ttl)
        self.assertEqual(job.get_ttl(), ttl)

    def test_never_expire_during_execution(self):
        """Test what happens when job expires during execution"""
        ttl = 1
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.long_running_job, args=(2,), ttl=ttl)
        self.assertEqual(job.get_ttl(), ttl)
        job.save()
        job.perform()
        self.assertEqual(job.get_ttl(), ttl)
        self.assertTrue(job.exists(job.id, connection=self.connection))
        self.assertEqual(job.result, 'Done sleeping...')

    def test_cleanup(self):
        """Test that jobs and results are expired properly."""
        job = Job.create(func=fixtures.say_hello, connection=self.connection, status=JobStatus.QUEUED)
        job.save()

        # Jobs with negative TTLs don't expire
        job.cleanup(ttl=-1)
        self.assertEqual(self.connection.ttl(job.key), -1)

        # Jobs with positive TTLs are eventually deleted
        job.cleanup(ttl=100)
        self.assertEqual(self.connection.ttl(job.key), 100)

        # Jobs with 0 TTL are immediately deleted
        job.cleanup(ttl=0)
        self.assertRaises(NoSuchJobError, Job.fetch, job.id, self.connection)

    def test_job_get_position(self):
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.say_hello)
        job2 = queue.enqueue(fixtures.say_hello)
        job3 = Job(uuid4().hex, connection=self.connection)

        self.assertEqual(0, job.get_position())
        self.assertEqual(1, job2.get_position())
        self.assertEqual(None, job3.get_position())

    def test_job_delete_removes_itself_from_registries(self):
        """job.delete() should remove itself from job registries"""
        job = Job.create(
            func=fixtures.say_hello,
            status=JobStatus.FAILED,
            connection=self.connection,
            origin='default',
            serializer=JSONSerializer,
        )
        job.save()
        registry = FailedJobRegistry(connection=self.connection, serializer=JSONSerializer)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

        job = Job.create(
            func=fixtures.say_hello,
            status=JobStatus.STOPPED,
            connection=self.connection,
            origin='default',
            serializer=JSONSerializer,
        )
        job.save()
        registry = FailedJobRegistry(connection=self.connection, serializer=JSONSerializer)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

        job = Job.create(
            func=fixtures.say_hello,
            status=JobStatus.FINISHED,
            connection=self.connection,
            origin='default',
            serializer=JSONSerializer,
        )
        job.save()

        registry = FinishedJobRegistry(connection=self.connection, serializer=JSONSerializer)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

        job = Job.create(
            func=fixtures.say_hello,
            status=JobStatus.STARTED,
            connection=self.connection,
            origin='default',
            serializer=JSONSerializer,
        )
        job.save()

        registry = StartedJobRegistry(connection=self.connection, serializer=JSONSerializer)
        with self.connection.pipeline() as pipe:
            # this will also add the execution to the registry
            Execution.create(job, ttl=500, pipeline=pipe)
            pipe.execute()

        job.delete()
        self.assertFalse(job in registry)

        job = Job.create(
            func=fixtures.say_hello,
            status=JobStatus.DEFERRED,
            connection=self.connection,
            origin='default',
            serializer=JSONSerializer,
        )
        job.save()

        registry = DeferredJobRegistry(connection=self.connection, serializer=JSONSerializer)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

        job = Job.create(
            func=fixtures.say_hello,
            status=JobStatus.SCHEDULED,
            connection=self.connection,
            origin='default',
            serializer=JSONSerializer,
        )
        job.save()

        registry = ScheduledJobRegistry(connection=self.connection, serializer=JSONSerializer)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

    def test_job_delete_execution_registry(self):
        """job.delete() also deletes ExecutionRegistry and all job executions"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.say_hello)
        worker = Worker([queue], connection=self.connection)

        execution = worker.prepare_execution(job=job)

        self.assertTrue(self.connection.exists(job.execution_registry.key))
        self.assertTrue(self.connection.exists(execution.key))
        job.delete()
        self.assertFalse(self.connection.exists(job.execution_registry.key))
        self.assertFalse(self.connection.exists(execution.key))

    def test_create_job_with_id(self):
        """test creating jobs with a custom ID"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.say_hello, job_id='1234')
        self.assertEqual(job.id, '1234')
        job.perform()

        self.assertRaises(TypeError, queue.enqueue, fixtures.say_hello, job_id=1234)

    def test_create_job_with_invalid_id(self):
        """test creating jobs with a custom invalid ID (with character :)"""
        queue = Queue(connection=self.connection)

        with self.assertRaises(ValueError):
            queue.enqueue(fixtures.say_hello, job_id='1234:4321')

    def test_create_job_with_async(self):
        """test creating jobs with async function"""
        queue = Queue(connection=self.connection)

        async_job = queue.enqueue(fixtures.say_hello_async, job_id='async_job')
        sync_job = queue.enqueue(fixtures.say_hello, job_id='sync_job')

        self.assertEqual(async_job.id, 'async_job')
        self.assertEqual(sync_job.id, 'sync_job')

        async_task_result = async_job.perform()
        sync_task_result = sync_job.perform()

        self.assertEqual(sync_task_result, async_task_result)

    def test_get_call_string_unicode(self):
        """test call string with unicode keyword arguments"""
        queue = Queue(connection=self.connection)

        job = queue.enqueue(fixtures.echo, arg_with_unicode=fixtures.UnicodeStringObject())
        self.assertIsNotNone(job.get_call_string())
        job.perform()

    def test_create_job_from_static_method(self):
        """test creating jobs with static method"""
        queue = Queue(connection=self.connection)

        job = queue.enqueue(fixtures.ClassWithAStaticMethod.static_method)
        self.assertIsNotNone(job.get_call_string())
        job.perform()

    def test_create_job_with_ttl_should_have_ttl_after_enqueued(self):
        """test creating jobs with ttl and checks if get_jobs returns it properly [issue502]"""
        queue = Queue(connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='1234', ttl=10)
        job = queue.get_jobs()[0]
        self.assertEqual(job.ttl, 10)

    def test_create_job_with_ttl_should_expire(self):
        """test if a job created with ttl expires [issue502]"""
        queue = Queue(connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='1234', ttl=1)
        time.sleep(1.1)
        self.assertEqual(0, len(queue.get_jobs()))

    def test_create_and_cancel_job(self):
        """Ensure job.cancel() works properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.say_hello)
        self.assertEqual(1, len(queue.get_jobs()))
        cancel_job(job.id, connection=self.connection)
        self.assertEqual(0, len(queue.get_jobs()))
        registry = CanceledJobRegistry(connection=self.connection, queue=queue)
        self.assertIn(job, registry)
        self.assertEqual(job.get_status(), JobStatus.CANCELED)

        # If job is deleted, it's also removed from CanceledJobRegistry
        job.delete()
        self.assertNotIn(job, registry)

    def test_create_and_cancel_job_fails_already_canceled(self):
        """Ensure job.cancel() fails on already canceled job"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.say_hello, job_id='fake_job_id')
        self.assertEqual(1, len(queue.get_jobs()))

        # First cancel should be fine
        cancel_job(job.id, connection=self.connection)
        self.assertEqual(0, len(queue.get_jobs()))
        registry = CanceledJobRegistry(connection=self.connection, queue=queue)
        self.assertIn(job, registry)
        self.assertEqual(job.get_status(), JobStatus.CANCELED)

        # Second cancel should fail
        self.assertRaisesRegex(
            InvalidJobOperation,
            r'Cannot cancel already canceled job: fake_job_id',
            cancel_job,
            job.id,
            connection=self.connection,
        )

    def test_create_and_cancel_job_with_serializer(self):
        """test creating and using cancel_job (with serializer) deletes job properly"""
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(fixtures.say_hello)
        self.assertEqual(1, len(queue.get_jobs()))
        cancel_job(job.id, serializer=JSONSerializer, connection=self.connection)
        self.assertEqual(0, len(queue.get_jobs()))

    def test_key_for_should_return_prefixed_job_id(self):
        """test redis key to store job hash under"""
        job_id = 'random'
        key = Job.key_for(job_id=job_id)

        assert key == (Job.redis_job_namespace_prefix + job_id).encode('utf-8')

    @unittest.skipIf(get_version(Redis()) < (5, 0, 0), 'Skip if Redis server < 5.0')
    def test_blocking_result_fetch(self):
        # Ensure blocking waits for the time to run the job, but not right up until the timeout.
        job_sleep_seconds = 2
        block_seconds = 5
        queue_name = 'test_blocking_queue'
        q = Queue(queue_name, connection=self.connection)
        job = q.enqueue(fixtures.long_running_job, job_sleep_seconds)
        started_at = time.time()
        fixtures.start_worker_process(queue_name, connection=self.connection, burst=True)
        result = job.latest_result(timeout=block_seconds)
        blocked_for = time.time() - started_at
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertIsNotNone(result)
        self.assertGreaterEqual(blocked_for, job_sleep_seconds)
        self.assertLess(blocked_for, block_seconds)
