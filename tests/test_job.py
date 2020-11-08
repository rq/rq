# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import json
import time
import queue
import zlib
from datetime import datetime, timedelta

from redis import WatchError

from rq.compat import as_text
from rq.exceptions import NoSuchJobError
from rq.job import Job, JobStatus, cancel_job, get_current_job, Retry
from rq.queue import Queue
from rq.registry import (DeferredJobRegistry, FailedJobRegistry,
                         FinishedJobRegistry, StartedJobRegistry,
                         ScheduledJobRegistry)
from rq.utils import utcformat, utcnow
from rq.worker import Worker
from tests import RQTestCase, fixtures

from pickle import loads, dumps


class TestJob(RQTestCase):
    def test_unicode(self):
        """Unicode in job description [issue405]"""
        job = Job.create(
            'myfunc',
            args=[12, "☃"],
            kwargs=dict(snowman="☃", null=None),
        )
        self.assertEqual(
            job.description,
            "myfunc(12, '☃', null=None, snowman='☃')",
        )

    def test_create_empty_job(self):
        """Creation of new empty jobs."""
        job = Job()
        job.description = 'test job'

        # Jobs have a random UUID and a creation date
        self.assertIsNotNone(job.id)
        self.assertIsNotNone(job.created_at)
        self.assertEqual(str(job), "<Job %s: test job>" % job.id)

        # ...and nothing else
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)
        self.assertIsNone(job.started_at)
        self.assertIsNone(job.ended_at)
        self.assertIsNone(job.result)
        self.assertIsNone(job.exc_info)

        with self.assertRaises(ValueError):
            job.func
        with self.assertRaises(ValueError):
            job.instance
        with self.assertRaises(ValueError):
            job.args
        with self.assertRaises(ValueError):
            job.kwargs

    def test_create_param_errors(self):
        """Creation of jobs may result in errors"""
        self.assertRaises(TypeError, Job.create, fixtures.say_hello, args="string")
        self.assertRaises(TypeError, Job.create, fixtures.say_hello, kwargs="string")
        self.assertRaises(TypeError, Job.create, func=42)

    def test_create_typical_job(self):
        """Creation of jobs for function calls."""
        job = Job.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2))

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
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)
        self.assertIsNone(job.result)

    def test_create_instance_method_job(self):
        """Creation of jobs for instance methods."""
        n = fixtures.Number(2)
        job = Job.create(func=n.div, args=(4,))

        # Job data is set
        self.assertEqual(job.func, n.div)
        self.assertEqual(job.instance, n)
        self.assertEqual(job.args, (4,))

    def test_create_job_with_serializer(self):
        """Creation of jobs with serializer for instance methods."""
        # Test using json serializer
        n = fixtures.Number(2)
        job = Job.create(func=n.div, args=(4,), serializer=json)

        self.assertIsNotNone(job.serializer)
        self.assertEqual(job.func, n.div)
        self.assertEqual(job.instance, n)
        self.assertEqual(job.args, (4,))

    def test_create_job_from_string_function(self):
        """Creation of jobs using string specifier."""
        job = Job.create(func='tests.fixtures.say_hello', args=('World',))

        # Job data is set
        self.assertEqual(job.func, fixtures.say_hello)
        self.assertIsNone(job.instance)
        self.assertEqual(job.args, ('World',))

    def test_create_job_from_callable_class(self):
        """Creation of jobs using a callable class specifier."""
        kallable = fixtures.CallableObject()
        job = Job.create(func=kallable)

        self.assertEqual(job.func, kallable.__call__)
        self.assertEqual(job.instance, kallable)

    def test_job_properties_set_data_property(self):
        """Data property gets derived from the job tuple."""
        job = Job()
        job.func_name = 'foo'
        fname, instance, args, kwargs = loads(job.data)

        self.assertEqual(fname, job.func_name)
        self.assertEqual(instance, None)
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {})

    def test_data_property_sets_job_properties(self):
        """Job tuple gets derived lazily from data property."""
        job = Job()
        job.data = dumps(('foo', None, (1, 2, 3), {'bar': 'qux'}))

        self.assertEqual(job.func_name, 'foo')
        self.assertEqual(job.instance, None)
        self.assertEqual(job.args, (1, 2, 3))
        self.assertEqual(job.kwargs, {'bar': 'qux'})

    def test_save(self):  # noqa
        """Storing jobs."""
        job = Job.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2))

        # Saving creates a Redis hash
        self.assertEqual(self.testconn.exists(job.key), False)
        job.save()
        self.assertEqual(self.testconn.type(job.key), b'hash')

        # Saving writes pickled job data
        unpickled_data = loads(zlib.decompress(self.testconn.hget(job.key, 'data')))
        self.assertEqual(unpickled_data[0], 'tests.fixtures.some_calculation')

    def test_fetch(self):
        """Fetching jobs."""
        # Prepare test
        self.testconn.hset('rq:job:some_id', 'data',
                           "(S'tests.fixtures.some_calculation'\nN(I3\nI4\nt(dp1\nS'z'\nI2\nstp2\n.")
        self.testconn.hset('rq:job:some_id', 'created_at',
                           '2012-02-07T22:13:24.123456Z')

        # Fetch returns a job
        job = Job.fetch('some_id')
        self.assertEqual(job.id, 'some_id')
        self.assertEqual(job.func_name, 'tests.fixtures.some_calculation')
        self.assertIsNone(job.instance)
        self.assertEqual(job.args, (3, 4))
        self.assertEqual(job.kwargs, dict(z=2))
        self.assertEqual(job.created_at, datetime(2012, 2, 7, 22, 13, 24, 123456))

    def test_fetch_many(self):
        """Fetching many jobs at once."""
        data = {
            'func': fixtures.some_calculation,
            'args': (3, 4),
            'kwargs': dict(z=2),
            'connection': self.testconn,
        }
        job = Job.create(**data)
        job.save()

        job2 = Job.create(**data)
        job2.save()

        jobs = Job.fetch_many([job.id, job2.id, 'invalid_id'], self.testconn)
        self.assertEqual(jobs, [job, job2, None])

    def test_persistence_of_empty_jobs(self):  # noqa
        """Storing empty jobs."""
        job = Job()
        with self.assertRaises(ValueError):
            job.save()

    def test_persistence_of_typical_jobs(self):
        """Storing typical jobs."""
        job = Job.create(func=fixtures.some_calculation, args=(3, 4), kwargs=dict(z=2))
        job.save()

        stored_date = self.testconn.hget(job.key, 'created_at').decode('utf-8')
        self.assertEqual(stored_date, utcformat(job.created_at))

        # ... and no other keys are stored
        self.assertEqual(
            sorted(self.testconn.hkeys(job.key)),
            [b'created_at', b'data', b'description', b'ended_at', b'last_heartbeat', b'started_at', b'worker_name'])

        self.assertEqual(job.last_heartbeat, None)
        self.assertEqual(job.last_heartbeat, None)

        ts = utcnow()
        job.heartbeat(ts)
        self.assertEqual(job.last_heartbeat, ts)

    def test_persistence_of_retry_data(self):
        """Retry related data is stored and restored properly"""
        job = Job.create(func=fixtures.some_calculation)
        job.retries_left = 3
        job.retry_intervals = [1, 2, 3]
        job.save()

        job.retries_left = None
        job.retry_intervals = None
        job.refresh()
        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.retry_intervals, [1, 2, 3])

    def test_persistence_of_parent_job(self):
        """Storing jobs with parent job, either instance or key."""
        parent_job = Job.create(func=fixtures.some_calculation)
        parent_job.save()
        job = Job.create(func=fixtures.some_calculation, depends_on=parent_job)
        job.save()
        stored_job = Job.fetch(job.id)
        self.assertEqual(stored_job._dependency_id, parent_job.id)
        self.assertEqual(stored_job._dependency_ids, [parent_job.id])
        self.assertEqual(stored_job.dependency.id, parent_job.id)
        self.assertEqual(stored_job.dependency, parent_job)

        job = Job.create(func=fixtures.some_calculation, depends_on=parent_job.id)
        job.save()
        stored_job = Job.fetch(job.id)
        self.assertEqual(stored_job._dependency_id, parent_job.id)
        self.assertEqual(stored_job._dependency_ids, [parent_job.id])
        self.assertEqual(stored_job.dependency.id, parent_job.id)
        self.assertEqual(stored_job.dependency, parent_job)

    def test_store_then_fetch(self):
        """Store, then fetch."""
        job = Job.create(func=fixtures.some_calculation, timeout='1h', args=(3, 4),
                         kwargs=dict(z=2))
        job.save()

        job2 = Job.fetch(job.id)
        self.assertEqual(job.func, job2.func)
        self.assertEqual(job.args, job2.args)
        self.assertEqual(job.kwargs, job2.kwargs)
        self.assertEqual(job.timeout, job2.timeout)

        # Mathematical equation
        self.assertEqual(job, job2)

    def test_fetching_can_fail(self):
        """Fetching fails for non-existing jobs."""
        with self.assertRaises(NoSuchJobError):
            Job.fetch('b4a44d44-da16-4620-90a6-798e8cd72ca0')

    def test_fetching_unreadable_data(self):
        """Fetching succeeds on unreadable data, but lazy props fail."""
        # Set up
        job = Job.create(func=fixtures.some_calculation, args=(3, 4),
                         kwargs=dict(z=2))
        job.save()

        # Just replace the data hkey with some random noise
        self.testconn.hset(job.key, 'data', 'this is no pickle string')
        job.refresh()

        for attr in ('func_name', 'instance', 'args', 'kwargs'):
            with self.assertRaises(Exception):
                getattr(job, attr)

    def test_job_is_unimportable(self):
        """Jobs that cannot be imported throw exception on access."""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',))
        job.save()

        # Now slightly modify the job to make it unimportable (this is
        # equivalent to a worker not having the most up-to-date source code
        # and unable to import the function)
        job_data = job.data
        unimportable_data = job_data.replace(b'say_hello', b'nay_hello')

        self.testconn.hset(job.key, 'data', zlib.compress(unimportable_data))

        job.refresh()
        with self.assertRaises(AttributeError):
            job.func  # accessing the func property should fail

    def test_compressed_exc_info_handling(self):
        """Jobs handle both compressed and uncompressed exc_info"""
        exception_string = 'Some exception'

        job = Job.create(func=fixtures.say_hello, args=('Lionel',))
        job.exc_info = exception_string
        job.save()

        # exc_info is stored in compressed format
        exc_info = self.testconn.hget(job.key, 'exc_info')
        self.assertEqual(
            as_text(zlib.decompress(exc_info)),
            exception_string
        )

        job.refresh()
        self.assertEqual(job.exc_info, exception_string)

        # Uncompressed exc_info is also handled
        self.testconn.hset(job.key, 'exc_info', exception_string)

        job.refresh()
        self.assertEqual(job.exc_info, exception_string)

    def test_compressed_job_data_handling(self):
        """Jobs handle both compressed and uncompressed data"""

        job = Job.create(func=fixtures.say_hello, args=('Lionel',))
        job.save()

        # Job data is stored in compressed format
        job_data = job.data
        self.assertEqual(
            zlib.compress(job_data),
            self.testconn.hget(job.key, 'data')
        )

        self.testconn.hset(job.key, 'data', job_data)
        job.refresh()
        self.assertEqual(job.data, job_data)

    def test_custom_meta_is_persisted(self):
        """Additional meta data on jobs are stored persisted correctly."""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',))
        job.meta['foo'] = 'bar'
        job.save()

        raw_data = self.testconn.hget(job.key, 'meta')
        self.assertEqual(loads(raw_data)['foo'], 'bar')

        job2 = Job.fetch(job.id)
        self.assertEqual(job2.meta['foo'], 'bar')

    def test_custom_meta_is_rewriten_by_save_meta(self):
        """New meta data can be stored by save_meta."""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',))
        job.save()
        serialized = job.to_dict()

        job.meta['foo'] = 'bar'
        job.save_meta()

        raw_meta = self.testconn.hget(job.key, 'meta')
        self.assertEqual(loads(raw_meta)['foo'], 'bar')

        job2 = Job.fetch(job.id)
        self.assertEqual(job2.meta['foo'], 'bar')

        # nothing else was changed
        serialized2 = job2.to_dict()
        serialized2.pop('meta')
        self.assertDictEqual(serialized, serialized2)

    def test_unpickleable_result(self):
        """Unpickleable job result doesn't crash job.save() and job.refresh()"""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',))
        job._result = queue.Queue()
        job.save()

        self.assertEqual(
            self.testconn.hget(job.key, 'result').decode('utf-8'),
            'Unserializable return value'
        )

        job = Job.fetch(job.id)
        self.assertEqual(job.result, 'Unserializable return value')

    def test_result_ttl_is_persisted(self):
        """Ensure that job's result_ttl is set properly"""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), result_ttl=10)
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.result_ttl, 10)

        job = Job.create(func=fixtures.say_hello, args=('Lionel',))
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.result_ttl, None)

    def test_failure_ttl_is_persisted(self):
        """Ensure job.failure_ttl is set and restored properly"""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',), failure_ttl=15)
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.failure_ttl, 15)

        job = Job.create(func=fixtures.say_hello, args=('Lionel',))
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.failure_ttl, None)

    def test_description_is_persisted(self):
        """Ensure that job's custom description is set properly"""
        job = Job.create(func=fixtures.say_hello, args=('Lionel',),
                         description='Say hello!')
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.description, 'Say hello!')

        # Ensure job description is constructed from function call string
        job = Job.create(func=fixtures.say_hello, args=('Lionel',))
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.description, "tests.fixtures.say_hello('Lionel')")

    def test_prepare_for_execution(self):
        """job.prepare_for_execution works properly"""
        job = Job.create(func=fixtures.say_hello)
        job.save()
        with self.testconn.pipeline() as pipeline:
            job.prepare_for_execution("worker_name", pipeline)
            pipeline.execute()
        job.refresh()
        self.assertEqual(job.worker_name, "worker_name")
        self.assertEqual(job.get_status(), JobStatus.STARTED)
        self.assertIsNotNone(job.last_heartbeat)
        self.assertIsNotNone(job.started_at)

    def test_job_access_outside_job_fails(self):
        """The current job is accessible only within a job context."""
        self.assertIsNone(get_current_job())

    def test_job_access_within_job_function(self):
        """The current job is accessible within the job function."""
        q = Queue()
        job = q.enqueue(fixtures.access_self)
        w = Worker([q])
        w.work(burst=True)
        # access_self calls get_current_job() and executes successfully
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_job_access_within_synchronous_job_function(self):
        queue = Queue(is_async=False)
        queue.enqueue(fixtures.access_self)

    def test_job_async_status_finished(self):
        queue = Queue(is_async=False)
        job = queue.enqueue(fixtures.say_hello)
        self.assertEqual(job.result, 'Hi there, Stranger!')
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_enqueue_job_async_status_finished(self):
        queue = Queue(is_async=False)
        job = Job.create(func=fixtures.say_hello)
        job = queue.enqueue_job(job)
        self.assertEqual(job.result, 'Hi there, Stranger!')
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_get_result_ttl(self):
        """Getting job result TTL."""
        job_result_ttl = 1
        default_ttl = 2
        job = Job.create(func=fixtures.say_hello, result_ttl=job_result_ttl)
        job.save()
        self.assertEqual(job.get_result_ttl(default_ttl=default_ttl), job_result_ttl)
        self.assertEqual(job.get_result_ttl(), job_result_ttl)
        job = Job.create(func=fixtures.say_hello)
        job.save()
        self.assertEqual(job.get_result_ttl(default_ttl=default_ttl), default_ttl)
        self.assertEqual(job.get_result_ttl(), None)

    def test_get_job_ttl(self):
        """Getting job TTL."""
        ttl = 1
        job = Job.create(func=fixtures.say_hello, ttl=ttl)
        job.save()
        self.assertEqual(job.get_ttl(), ttl)
        job = Job.create(func=fixtures.say_hello)
        job.save()
        self.assertEqual(job.get_ttl(), None)

    def test_ttl_via_enqueue(self):
        ttl = 1
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(fixtures.say_hello, ttl=ttl)
        self.assertEqual(job.get_ttl(), ttl)

    def test_never_expire_during_execution(self):
        """Test what happens when job expires during execution"""
        ttl = 1
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(fixtures.long_running_job, args=(2,), ttl=ttl)
        self.assertEqual(job.get_ttl(), ttl)
        job.save()
        job.perform()
        self.assertEqual(job.get_ttl(), ttl)
        self.assertTrue(job.exists(job.id))
        self.assertEqual(job.result, 'Done sleeping...')

    def test_cleanup(self):
        """Test that jobs and results are expired properly."""
        job = Job.create(func=fixtures.say_hello)
        job.save()

        # Jobs with negative TTLs don't expire
        job.cleanup(ttl=-1)
        self.assertEqual(self.testconn.ttl(job.key), -1)

        # Jobs with positive TTLs are eventually deleted
        job.cleanup(ttl=100)
        self.assertEqual(self.testconn.ttl(job.key), 100)

        # Jobs with 0 TTL are immediately deleted
        job.cleanup(ttl=0)
        self.assertRaises(NoSuchJobError, Job.fetch, job.id, self.testconn)

    def test_cleanup_expires_dependency_keys(self):

        dependency_job = Job.create(func=fixtures.say_hello)
        dependency_job.save()

        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job)

        dependent_job.register_dependency()
        dependent_job.save()

        dependent_job.cleanup(ttl=100)
        dependency_job.cleanup(ttl=100)

        self.assertEqual(self.testconn.ttl(dependent_job.dependencies_key), 100)
        self.assertEqual(self.testconn.ttl(dependency_job.dependents_key), 100)

    def test_job_get_position(self):
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(fixtures.say_hello)
        job2 = queue.enqueue(fixtures.say_hello)
        job3 = Job(fixtures.say_hello)

        self.assertEqual(0, job.get_position())
        self.assertEqual(1, job2.get_position())
        self.assertEqual(None, job3.get_position())

    def test_job_with_dependents_delete_parent(self):
        """job.delete() deletes itself from Redis but not dependents.
        Wthout a save, the dependent job is never saved into redis. The delete
        method will get and pass a NoSuchJobError.
        """
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(fixtures.say_hello)
        job2 = Job.create(func=fixtures.say_hello, depends_on=job)
        job2.register_dependency()

        job.delete()
        self.assertFalse(self.testconn.exists(job.key))
        self.assertFalse(self.testconn.exists(job.dependents_key))

        # By default, dependents are not deleted, but The job is in redis only
        # if it was saved!
        self.assertFalse(self.testconn.exists(job2.key))

        self.assertNotIn(job.id, queue.get_job_ids())

    def test_job_delete_removes_itself_from_registries(self):
        """job.delete() should remove itself from job registries"""
        connection = self.testconn
        job = Job.create(func=fixtures.say_hello, status=JobStatus.FAILED,
                         connection=self.testconn, origin='default')
        job.save()
        registry = FailedJobRegistry(connection=self.testconn)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

        job = Job.create(func=fixtures.say_hello, status=JobStatus.FINISHED,
                         connection=self.testconn, origin='default')
        job.save()

        registry = FinishedJobRegistry(connection=self.testconn)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

        job = Job.create(func=fixtures.say_hello, status=JobStatus.STARTED,
                         connection=self.testconn, origin='default')
        job.save()

        registry = StartedJobRegistry(connection=self.testconn)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

        job = Job.create(func=fixtures.say_hello, status=JobStatus.DEFERRED,
                         connection=self.testconn, origin='default')
        job.save()

        registry = DeferredJobRegistry(connection=self.testconn)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

        job = Job.create(func=fixtures.say_hello, status=JobStatus.SCHEDULED,
                         connection=self.testconn, origin='default')
        job.save()

        registry = ScheduledJobRegistry(connection=self.testconn)
        registry.add(job, 500)

        job.delete()
        self.assertFalse(job in registry)

    def test_job_with_dependents_delete_parent_with_saved(self):
        """job.delete() deletes itself from Redis but not dependents. If the
        dependent job was saved, it will remain in redis."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(fixtures.say_hello)
        job2 = Job.create(func=fixtures.say_hello, depends_on=job)
        job2.register_dependency()
        job2.save()

        job.delete()
        self.assertFalse(self.testconn.exists(job.key))
        self.assertFalse(self.testconn.exists(job.dependents_key))

        # By default, dependents are not deleted, but The job is in redis only
        # if it was saved!
        self.assertTrue(self.testconn.exists(job2.key))

        self.assertNotIn(job.id, queue.get_job_ids())

    def test_job_with_dependents_deleteall(self):
        """job.delete() deletes itself from Redis. Dependents need to be
        deleted explictely."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(fixtures.say_hello)
        job2 = Job.create(func=fixtures.say_hello, depends_on=job)
        job2.register_dependency()

        job.delete(delete_dependents=True)
        self.assertFalse(self.testconn.exists(job.key))
        self.assertFalse(self.testconn.exists(job.dependents_key))
        self.assertFalse(self.testconn.exists(job2.key))

        self.assertNotIn(job.id, queue.get_job_ids())

    def test_job_with_dependents_delete_all_with_saved(self):
        """job.delete() deletes itself from Redis. Dependents need to be
        deleted explictely. Without a save, the dependent job is never saved
        into redis. The delete method will get and pass a NoSuchJobError.
        """
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(fixtures.say_hello)
        job2 = Job.create(func=fixtures.say_hello, depends_on=job)
        job2.register_dependency()
        job2.save()

        job.delete(delete_dependents=True)
        self.assertFalse(self.testconn.exists(job.key))
        self.assertFalse(self.testconn.exists(job.dependents_key))
        self.assertFalse(self.testconn.exists(job2.key))

        self.assertNotIn(job.id, queue.get_job_ids())

    def test_dependent_job_creates_dependencies_key(self):

        queue = Queue(connection=self.testconn)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job)

        dependent_job.register_dependency()
        dependent_job.save()

        self.assertTrue(self.testconn.exists(dependent_job.dependencies_key))

    def test_dependent_job_deletes_dependencies_key(self):
        """
        job.delete() deletes itself from Redis.
        """
        queue = Queue(connection=self.testconn)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job)

        dependent_job.register_dependency()
        dependent_job.save()
        dependent_job.delete()

        self.assertTrue(self.testconn.exists(dependency_job.key))
        self.assertFalse(self.testconn.exists(dependent_job.dependencies_key))
        self.assertFalse(self.testconn.exists(dependent_job.key))

    def test_create_job_with_id(self):
        """test creating jobs with a custom ID"""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(fixtures.say_hello, job_id="1234")
        self.assertEqual(job.id, "1234")
        job.perform()

        self.assertRaises(TypeError, queue.enqueue, fixtures.say_hello, job_id=1234)

    def test_get_call_string_unicode(self):
        """test call string with unicode keyword arguments"""
        queue = Queue(connection=self.testconn)

        job = queue.enqueue(fixtures.echo,
                            arg_with_unicode=fixtures.UnicodeStringObject())
        self.assertIsNotNone(job.get_call_string())
        job.perform()

    def test_create_job_with_ttl_should_have_ttl_after_enqueued(self):
        """test creating jobs with ttl and checks if get_jobs returns it properly [issue502]"""
        queue = Queue(connection=self.testconn)
        queue.enqueue(fixtures.say_hello, job_id="1234", ttl=10)
        job = queue.get_jobs()[0]
        self.assertEqual(job.ttl, 10)

    def test_create_job_with_ttl_should_expire(self):
        """test if a job created with ttl expires [issue502]"""
        queue = Queue(connection=self.testconn)
        queue.enqueue(fixtures.say_hello, job_id="1234", ttl=1)
        time.sleep(1.1)
        self.assertEqual(0, len(queue.get_jobs()))

    def test_create_and_cancel_job(self):
        """test creating and using cancel_job deletes job properly"""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(fixtures.say_hello)
        self.assertEqual(1, len(queue.get_jobs()))
        cancel_job(job.id)
        self.assertEqual(0, len(queue.get_jobs()))

    def test_dependents_key_for_should_return_prefixed_job_id(self):
        """test redis key to store job dependents hash under"""
        job_id = 'random'
        key = Job.dependents_key_for(job_id=job_id)

        assert key == Job.redis_job_namespace_prefix + job_id + ':dependents'

    def test_key_for_should_return_prefixed_job_id(self):
        """test redis key to store job hash under"""
        job_id = 'random'
        key = Job.key_for(job_id=job_id)

        assert key == (Job.redis_job_namespace_prefix + job_id).encode('utf-8')

    def test_dependencies_key_should_have_prefixed_job_id(self):
        job_id = 'random'
        job = Job(id=job_id)
        expected_key = Job.redis_job_namespace_prefix + ":" + job_id + ':dependencies'

        assert job.dependencies_key == expected_key

    def test_fetch_dependencies_returns_dependency_jobs(self):
        queue = Queue(connection=self.testconn)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job)

        dependent_job.register_dependency()
        dependent_job.save()

        dependencies = dependent_job.fetch_dependencies(pipeline=self.testconn)

        self.assertListEqual(dependencies, [dependency_job])

    def test_fetch_dependencies_returns_empty_if_not_dependent_job(self):
        queue = Queue(connection=self.testconn)
        dependent_job = Job.create(func=fixtures.say_hello)

        dependent_job.register_dependency()
        dependent_job.save()

        dependencies = dependent_job.fetch_dependencies(pipeline=self.testconn)

        self.assertListEqual(dependencies, [])

    def test_fetch_dependencies_raises_if_dependency_deleted(self):
        queue = Queue(connection=self.testconn)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job)

        dependent_job.register_dependency()
        dependent_job.save()

        dependency_job.delete()

        self.assertNotIn(
            dependent_job.id,
            [job.id for job in dependent_job.fetch_dependencies(
                pipeline=self.testconn
            )]
        )

    def test_fetch_dependencies_watches(self):
        queue = Queue(connection=self.testconn)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job)

        dependent_job.register_dependency()
        dependent_job.save()

        with self.testconn.pipeline() as pipeline:
            dependent_job.fetch_dependencies(
                watch=True,
                pipeline=pipeline
            )

            pipeline.multi()

            with self.assertRaises(WatchError):
                self.testconn.set(dependency_job.id, 'somethingelsehappened')
                pipeline.touch(dependency_job.id)
                pipeline.execute()

    def test_dependencies_finished_returns_false_if_dependencies_queued(self):
        queue = Queue(connection=self.testconn)

        dependency_job_ids = [
            queue.enqueue(fixtures.say_hello).id
            for _ in range(5)
        ]

        dependent_job = Job.create(func=fixtures.say_hello)
        dependent_job._dependency_ids = dependency_job_ids
        dependent_job.register_dependency()

        dependencies_finished = dependent_job.dependencies_are_met()

        self.assertFalse(dependencies_finished)

    def test_dependencies_finished_returns_true_if_no_dependencies(self):
        queue = Queue(connection=self.testconn)

        dependent_job = Job.create(func=fixtures.say_hello)
        dependent_job.register_dependency()

        dependencies_finished = dependent_job.dependencies_are_met()

        self.assertTrue(dependencies_finished)

    def test_dependencies_finished_returns_true_if_all_dependencies_finished(self):
        dependency_jobs = [
            Job.create(fixtures.say_hello)
            for _ in range(5)
        ]

        dependent_job = Job.create(func=fixtures.say_hello)
        dependent_job._dependency_ids = [job.id for job in dependency_jobs]
        dependent_job.register_dependency()

        now = utcnow()

        # Set ended_at timestamps
        for i, job in enumerate(dependency_jobs):
            job._status = JobStatus.FINISHED
            job.ended_at = now - timedelta(seconds=i)
            job.save()

        dependencies_finished = dependent_job.dependencies_are_met()

        self.assertTrue(dependencies_finished)

    def test_dependencies_finished_returns_false_if_unfinished_job(self):
        dependency_jobs = [Job.create(fixtures.say_hello) for _ in range(2)]

        dependency_jobs[0]._status = JobStatus.FINISHED
        dependency_jobs[0].ended_at = utcnow()
        dependency_jobs[0].save()

        dependency_jobs[1]._status = JobStatus.STARTED
        dependency_jobs[1].ended_at = None
        dependency_jobs[1].save()

        dependent_job = Job.create(func=fixtures.say_hello)
        dependent_job._dependency_ids = [job.id for job in dependency_jobs]
        dependent_job.register_dependency()

        now = utcnow()

        dependencies_finished = dependent_job.dependencies_are_met()

        self.assertFalse(dependencies_finished)

    def test_dependencies_finished_watches_job(self):
        queue = Queue(connection=self.testconn)

        dependency_job = queue.enqueue(fixtures.say_hello)

        dependent_job = Job.create(func=fixtures.say_hello)
        dependent_job._dependency_ids = [dependency_job.id]
        dependent_job.register_dependency()

        with self.testconn.pipeline() as pipeline:
            dependent_job.dependencies_are_met(
                pipeline=pipeline,
            )

            dependency_job.set_status(JobStatus.FAILED, pipeline=self.testconn)
            pipeline.multi()

            with self.assertRaises(WatchError):
                pipeline.touch(Job.key_for(dependent_job.id))
                pipeline.execute()

    def test_can_enqueue_job_if_dependency_is_deleted(self):
        queue = Queue(connection=self.testconn)

        dependency_job = queue.enqueue(fixtures.say_hello, result_ttl=0)

        w = Worker([queue])
        w.work(burst=True)

        assert queue.enqueue(fixtures.say_hello, depends_on=dependency_job)

    def test_dependents_are_met_if_dependency_is_deleted(self):
        queue = Queue(connection=self.testconn)

        dependency_job = queue.enqueue(fixtures.say_hello, result_ttl=0)
        dependent_job = queue.enqueue(fixtures.say_hello, depends_on=dependency_job)

        w = Worker([queue])
        w.work(burst=True, max_jobs=1)

        assert dependent_job.dependencies_are_met()
        assert dependent_job.get_status() == JobStatus.QUEUED
    
    def test_retry(self):
        """Retry parses `max` and `interval` correctly"""
        retry = Retry(max=1)
        self.assertEqual(retry.max, 1)
        self.assertEqual(retry.intervals, [0])
        self.assertRaises(ValueError, Retry, max=0)

        retry = Retry(max=2, interval=5)
        self.assertEqual(retry.max, 2)
        self.assertEqual(retry.intervals, [5])

        retry = Retry(max=3, interval=[5, 10])
        self.assertEqual(retry.max, 3)
        self.assertEqual(retry.intervals, [5, 10])

        # interval can't be negative
        self.assertRaises(ValueError, Retry, max=1, interval=-5)
        self.assertRaises(ValueError, Retry, max=1, interval=[1, -5])
    
    def test_get_retry_interval(self):
        """get_retry_interval() returns the right retry interval"""
        job = Job.create(func=fixtures.say_hello)

        # Handle case where self.retry_intervals is None
        job.retries_left = 2
        self.assertEqual(job.get_retry_interval(), 0)

        # Handle the most common case
        job.retry_intervals = [1, 2]
        self.assertEqual(job.get_retry_interval(), 1)
        job.retries_left = 1
        self.assertEqual(job.get_retry_interval(), 2)
        
        # Handle cases where number of retries > length of interval
        job.retries_left = 4
        job.retry_intervals = [1, 2, 3]
        self.assertEqual(job.get_retry_interval(), 1)
        job.retries_left = 3
        self.assertEqual(job.get_retry_interval(), 1)
        job.retries_left = 2
        self.assertEqual(job.get_retry_interval(), 2)
        job.retries_left = 1
        self.assertEqual(job.get_retry_interval(), 3)
        
