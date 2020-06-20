# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import json
import os
import shutil
import signal
import subprocess
import sys
import time
import zlib

from datetime import datetime, timedelta
from multiprocessing import Process
from time import sleep

from unittest import skipIf

import pytest
import mock
from mock import Mock

from tests import RQTestCase, slow
from tests.fixtures import (
    access_self, create_file, create_file_after_timeout, div_by_zero, do_nothing,
    kill_worker, long_running_job, modify_self, modify_self_and_error,
    run_dummy_heroku_worker, save_key_ttl, say_hello, say_pid,
)

from rq import Queue, SimpleWorker, Worker, get_current_connection
from rq.compat import as_text, PY2
from rq.job import Job, JobStatus
from rq.registry import StartedJobRegistry, FailedJobRegistry, FinishedJobRegistry
from rq.suspension import resume, suspend
from rq.utils import utcnow
from rq.version import VERSION
from rq.worker import HerokuWorker, WorkerStatus


class CustomJob(Job):
    pass


class CustomQueue(Queue):
    pass


class TestWorker(RQTestCase):

    def test_create_worker(self):
        """Worker creation using various inputs."""

        # With single string argument
        w = Worker('foo')
        self.assertEqual(w.queues[0].name, 'foo')

        # With list of strings
        w = Worker(['foo', 'bar'])
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        self.assertEqual(w.queue_keys(), [w.queues[0].key, w.queues[1].key])
        self.assertEqual(w.queue_names(), ['foo', 'bar'])

        # With iterable of strings
        w = Worker(iter(['foo', 'bar']))
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        # Also accept byte strings in Python 2
        if PY2:
            # With single byte string argument
            w = Worker(b'foo')
            self.assertEqual(w.queues[0].name, 'foo')

            # With list of byte strings
            w = Worker([b'foo', b'bar'])
            self.assertEqual(w.queues[0].name, 'foo')
            self.assertEqual(w.queues[1].name, 'bar')

            # With iterable of byte strings
            w = Worker(iter([b'foo', b'bar']))
            self.assertEqual(w.queues[0].name, 'foo')
            self.assertEqual(w.queues[1].name, 'bar')

        # With single Queue
        w = Worker(Queue('foo'))
        self.assertEqual(w.queues[0].name, 'foo')

        # With iterable of Queues
        w = Worker(iter([Queue('foo'), Queue('bar')]))
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        # With list of Queues
        w = Worker([Queue('foo'), Queue('bar')])
        self.assertEqual(w.queues[0].name, 'foo')
        self.assertEqual(w.queues[1].name, 'bar')

        # With string and serializer
        w = Worker('foo', serializer=json)
        self.assertEqual(w.queues[0].name, 'foo')

        # With queue having serializer
        w = Worker(Queue('foo'), serializer=json)
        self.assertEqual(w.queues[0].name, 'foo')

    def test_work_and_quit(self):
        """Worker processes work, then quits."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = Worker([fooq, barq])
        self.assertEqual(
            w.work(burst=True), False,
            'Did not expect any work on the queue.'
        )

        fooq.enqueue(say_hello, name='Frank')
        self.assertEqual(
            w.work(burst=True), True,
            'Expected at least some work done.'
        )

    def test_worker_all(self):
        """Worker.all() works properly"""
        foo_queue = Queue('foo')
        bar_queue = Queue('bar')

        w1 = Worker([foo_queue, bar_queue], name='w1')
        w1.register_birth()
        w2 = Worker([foo_queue], name='w2')
        w2.register_birth()

        self.assertEqual(
            set(Worker.all(connection=foo_queue.connection)),
            set([w1, w2])
        )
        self.assertEqual(set(Worker.all(queue=foo_queue)), set([w1, w2]))
        self.assertEqual(set(Worker.all(queue=bar_queue)), set([w1]))

        w1.register_death()
        w2.register_death()

    def test_find_by_key(self):
        """Worker.find_by_key restores queues, state and job_id."""
        queues = [Queue('foo'), Queue('bar')]
        w = Worker(queues)
        w.register_death()
        w.register_birth()
        w.set_state(WorkerStatus.STARTED)
        worker = Worker.find_by_key(w.key)
        self.assertEqual(worker.queues, queues)
        self.assertEqual(worker.get_state(), WorkerStatus.STARTED)
        self.assertEqual(worker._job_id, None)
        self.assertTrue(worker.key in Worker.all_keys(worker.connection))
        self.assertEqual(worker.version, VERSION)

        # If worker is gone, its keys should also be removed
        worker.connection.delete(worker.key)
        Worker.find_by_key(worker.key)
        self.assertFalse(worker.key in Worker.all_keys(worker.connection))

        self.assertRaises(ValueError, Worker.find_by_key, 'foo')

    def test_worker_ttl(self):
        """Worker ttl."""
        w = Worker([])
        w.register_birth()
        [worker_key] = self.testconn.smembers(Worker.redis_workers_keys)
        self.assertIsNotNone(self.testconn.ttl(worker_key))
        w.register_death()

    def test_work_via_string_argument(self):
        """Worker processes work fed via string arguments."""
        q = Queue('foo')
        w = Worker([q])
        job = q.enqueue('tests.fixtures.say_hello', name='Frank')
        self.assertEqual(
            w.work(burst=True), True,
            'Expected at least some work done.'
        )
        self.assertEqual(job.result, 'Hi there, Frank!')

    def test_job_times(self):
        """job times are set correctly."""
        q = Queue('foo')
        w = Worker([q])
        before = utcnow()
        before = before.replace(microsecond=0)
        job = q.enqueue(say_hello)
        self.assertIsNotNone(job.enqueued_at)
        self.assertIsNone(job.started_at)
        self.assertIsNone(job.ended_at)
        self.assertEqual(
            w.work(burst=True), True,
            'Expected at least some work done.'
        )
        self.assertEqual(job.result, 'Hi there, Stranger!')
        after = utcnow()
        job.refresh()
        self.assertTrue(
            before <= job.enqueued_at <= after,
            'Not %s <= %s <= %s' % (before, job.enqueued_at, after)
        )
        self.assertTrue(
            before <= job.started_at <= after,
            'Not %s <= %s <= %s' % (before, job.started_at, after)
        )
        self.assertTrue(
            before <= job.ended_at <= after,
            'Not %s <= %s <= %s' % (before, job.ended_at, after)
        )

    def test_work_is_unreadable(self):
        """Unreadable jobs are put on the failed job registry."""
        q = Queue()
        self.assertEqual(q.count, 0)

        # NOTE: We have to fake this enqueueing for this test case.
        # What we're simulating here is a call to a function that is not
        # importable from the worker process.
        job = Job.create(func=div_by_zero, args=(3,), origin=q.name)
        job.save()

        job_data = job.data
        invalid_data = job_data.replace(b'div_by_zero', b'nonexisting')
        assert job_data != invalid_data
        self.testconn.hset(job.key, 'data', zlib.compress(invalid_data))

        # We use the low-level internal function to enqueue any data (bypassing
        # validity checks)
        q.push_job_id(job.id)

        self.assertEqual(q.count, 1)

        # All set, we're going to process it
        w = Worker([q])
        w.work(burst=True)   # should silently pass
        self.assertEqual(q.count, 0)

        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)

    def test_heartbeat(self):
        """Heartbeat saves last_heartbeat"""
        q = Queue()
        w = Worker([q])
        w.register_birth()

        self.assertEqual(str(w.pid), as_text(self.testconn.hget(w.key, 'pid')))
        self.assertEqual(w.hostname,
                         as_text(self.testconn.hget(w.key, 'hostname')))
        last_heartbeat = self.testconn.hget(w.key, 'last_heartbeat')
        self.assertIsNotNone(self.testconn.hget(w.key, 'birth'))
        self.assertTrue(last_heartbeat is not None)
        w = Worker.find_by_key(w.key)
        self.assertIsInstance(w.last_heartbeat, datetime)

        # worker.refresh() shouldn't fail if last_heartbeat is None
        # for compatibility reasons
        self.testconn.hdel(w.key, 'last_heartbeat')
        w.refresh()
        # worker.refresh() shouldn't fail if birth is None
        # for compatibility reasons
        self.testconn.hdel(w.key, 'birth')
        w.refresh()

    @slow
    def test_heartbeat_busy(self):
        """Periodic heartbeats while horse is busy with long jobs"""
        q = Queue()
        w = Worker([q], job_monitoring_interval=5)

        for timeout, expected_heartbeats in [(2, 0), (7, 1), (12, 2)]:
            job = q.enqueue(long_running_job,
                            args=(timeout,),
                            job_timeout=30,
                            result_ttl=-1)
            with mock.patch.object(w, 'heartbeat', wraps=w.heartbeat) as mocked:
                w.execute_job(job, q)
                self.assertEqual(mocked.call_count, expected_heartbeats)
            job = Job.fetch(job.id)
            self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_work_fails(self):
        """Failing jobs are put on the failed queue."""
        q = Queue()
        self.assertEqual(q.count, 0)

        # Action
        job = q.enqueue(div_by_zero)
        self.assertEqual(q.count, 1)

        # keep for later
        enqueued_at_date = str(job.enqueued_at)

        w = Worker([q])
        w.work(burst=True)  # should silently pass

        # Postconditions
        self.assertEqual(q.count, 0)
        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(w.get_current_job_id(), None)

        # Check the job
        job = Job.fetch(job.id)
        self.assertEqual(job.origin, q.name)

        # Should be the original enqueued_at date, not the date of enqueueing
        # to the failed queue
        self.assertEqual(str(job.enqueued_at), enqueued_at_date)
        self.assertTrue(job.exc_info)  # should contain exc_info

    def test_statistics(self):
        """Successful and failed job counts are saved properly"""
        queue = Queue()
        job = queue.enqueue(div_by_zero)
        worker = Worker([queue])
        worker.register_birth()

        self.assertEqual(worker.failed_job_count, 0)
        self.assertEqual(worker.successful_job_count, 0)
        self.assertEqual(worker.total_working_time, 0)

        registry = StartedJobRegistry(connection=worker.connection)
        job.started_at = utcnow()
        job.ended_at = job.started_at + timedelta(seconds=0.75)
        worker.handle_job_failure(job)
        worker.handle_job_success(job, queue, registry)

        worker.refresh()
        self.assertEqual(worker.failed_job_count, 1)
        self.assertEqual(worker.successful_job_count, 1)
        self.assertEqual(worker.total_working_time, 1.5)  # 1.5 seconds

        worker.handle_job_failure(job)
        worker.handle_job_success(job, queue, registry)

        worker.refresh()
        self.assertEqual(worker.failed_job_count, 2)
        self.assertEqual(worker.successful_job_count, 2)
        self.assertEqual(worker.total_working_time, 3.0)

    def test_total_working_time(self):
        """worker.total_working_time is stored properly"""
        queue = Queue()
        job = queue.enqueue(long_running_job, 0.05)
        worker = Worker([queue])
        worker.register_birth()

        worker.perform_job(job, queue)
        worker.refresh()
        # total_working_time should be a little bit more than 0.05 seconds
        self.assertGreaterEqual(worker.total_working_time, 0.05)
        # in multi-user environments delays might be unpredictable,
        # please adjust this magic limit accordingly in case if It takes even longer to run
        self.assertLess(worker.total_working_time, 1)

    def test_max_jobs(self):
        """Worker exits after number of jobs complete."""
        queue = Queue()
        job1 = queue.enqueue(do_nothing)
        job2 = queue.enqueue(do_nothing)
        worker = Worker([queue])
        worker.work(max_jobs=1)

        self.assertEqual(JobStatus.FINISHED, job1.get_status())
        self.assertEqual(JobStatus.QUEUED, job2.get_status())

    def test_disable_default_exception_handler(self):
        """
        Job is not moved to FailedJobRegistry when default custom exception
        handler is disabled.
        """
        queue = Queue(name='default', connection=self.testconn)

        job = queue.enqueue(div_by_zero)
        worker = Worker([queue], disable_default_exception_handler=False)
        worker.work(burst=True)

        registry = FailedJobRegistry(queue=queue)
        self.assertTrue(job in registry)

        # Job is not added to FailedJobRegistry if
        # disable_default_exception_handler is True
        job = queue.enqueue(div_by_zero)
        worker = Worker([queue], disable_default_exception_handler=True)
        worker.work(burst=True)
        self.assertFalse(job in registry)

    def test_custom_exc_handling(self):
        """Custom exception handling."""

        def first_handler(job, *exc_info):
            job.meta = {'first_handler': True}
            job.save_meta()
            return True

        def second_handler(job, *exc_info):
            job.meta.update({'second_handler': True})
            job.save_meta()

        def black_hole(job, *exc_info):
            # Don't fall through to default behaviour (moving to failed queue)
            return False

        q = Queue()
        self.assertEqual(q.count, 0)
        job = q.enqueue(div_by_zero)

        w = Worker([q], exception_handlers=first_handler)
        w.work(burst=True)

        # Check the job
        job.refresh()
        self.assertEqual(job.is_failed, True)
        self.assertTrue(job.meta['first_handler'])

        job = q.enqueue(div_by_zero)
        w = Worker([q], exception_handlers=[first_handler, second_handler])
        w.work(burst=True)

        # Both custom exception handlers are run
        job.refresh()
        self.assertEqual(job.is_failed, True)
        self.assertTrue(job.meta['first_handler'])
        self.assertTrue(job.meta['second_handler'])

        job = q.enqueue(div_by_zero)
        w = Worker([q], exception_handlers=[first_handler, black_hole,
                                            second_handler])
        w.work(burst=True)

        # second_handler is not run since it's interrupted by black_hole
        job.refresh()
        self.assertEqual(job.is_failed, True)
        self.assertTrue(job.meta['first_handler'])
        self.assertEqual(job.meta.get('second_handler'), None)

    def test_cancelled_jobs_arent_executed(self):
        """Cancelling jobs."""

        SENTINEL_FILE = '/tmp/rq-tests.txt'  # noqa

        try:
            # Remove the sentinel if it is leftover from a previous test run
            os.remove(SENTINEL_FILE)
        except OSError as e:
            if e.errno != 2:
                raise

        q = Queue()
        job = q.enqueue(create_file, SENTINEL_FILE)

        # Here, we cancel the job, so the sentinel file may not be created
        self.testconn.delete(job.key)

        w = Worker([q])
        w.work(burst=True)
        assert q.count == 0

        # Should not have created evidence of execution
        self.assertEqual(os.path.exists(SENTINEL_FILE), False)

    @slow  # noqa
    def test_timeouts(self):
        """Worker kills jobs after timeout."""
        sentinel_file = '/tmp/.rq_sentinel'

        q = Queue()
        w = Worker([q])

        # Put it on the queue with a timeout value
        res = q.enqueue(create_file_after_timeout,
                        args=(sentinel_file, 4),
                        job_timeout=1)

        try:
            os.unlink(sentinel_file)
        except OSError as e:
            if e.errno == 2:
                pass

        self.assertEqual(os.path.exists(sentinel_file), False)
        w.work(burst=True)
        self.assertEqual(os.path.exists(sentinel_file), False)

        # TODO: Having to do the manual refresh() here is really ugly!
        res.refresh()
        self.assertIn('JobTimeoutException', as_text(res.exc_info))

    def test_worker_sets_result_ttl(self):
        """Ensure that Worker properly sets result_ttl for individual jobs."""
        q = Queue()
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w = Worker([q])
        self.assertIn(job.get_id().encode('utf-8'), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertNotEqual(self.testconn.ttl(job.key), 0)
        self.assertNotIn(job.get_id().encode('utf-8'), self.testconn.lrange(q.key, 0, -1))

        # Job with -1 result_ttl don't expire
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=-1)
        w = Worker([q])
        self.assertIn(job.get_id().encode('utf-8'), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertEqual(self.testconn.ttl(job.key), -1)
        self.assertNotIn(job.get_id().encode('utf-8'), self.testconn.lrange(q.key, 0, -1))

        # Job with result_ttl = 0 gets deleted immediately
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=0)
        w = Worker([q])
        self.assertIn(job.get_id().encode('utf-8'), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertEqual(self.testconn.get(job.key), None)
        self.assertNotIn(job.get_id().encode('utf-8'), self.testconn.lrange(q.key, 0, -1))

    def test_worker_sets_job_status(self):
        """Ensure that worker correctly sets job status."""
        q = Queue()
        w = Worker([q])

        job = q.enqueue(say_hello)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.is_queued, True)
        self.assertEqual(job.is_finished, False)
        self.assertEqual(job.is_failed, False)

        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(job.is_queued, False)
        self.assertEqual(job.is_finished, True)
        self.assertEqual(job.is_failed, False)

        # Failed jobs should set status to "failed"
        job = q.enqueue(div_by_zero, args=(1,))
        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertEqual(job.is_queued, False)
        self.assertEqual(job.is_finished, False)
        self.assertEqual(job.is_failed, True)

    def test_job_dependency(self):
        """Enqueue dependent jobs only if their parents don't fail"""
        q = Queue()
        w = Worker([q])
        parent_job = q.enqueue(say_hello, result_ttl=0)
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        parent_job = q.enqueue(div_by_zero)
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertNotEqual(job.get_status(), JobStatus.FINISHED)

    def test_get_current_job(self):
        """Ensure worker.get_current_job() works properly"""
        q = Queue()
        worker = Worker([q])
        job = q.enqueue_call(say_hello)

        self.assertEqual(self.testconn.hget(worker.key, 'current_job'), None)
        worker.set_current_job_id(job.id)
        self.assertEqual(
            worker.get_current_job_id(),
            as_text(self.testconn.hget(worker.key, 'current_job'))
        )
        self.assertEqual(worker.get_current_job(), job)

    def test_custom_job_class(self):
        """Ensure Worker accepts custom job class."""
        q = Queue()
        worker = Worker([q], job_class=CustomJob)
        self.assertEqual(worker.job_class, CustomJob)

    def test_custom_queue_class(self):
        """Ensure Worker accepts custom queue class."""
        q = CustomQueue()
        worker = Worker([q], queue_class=CustomQueue)
        self.assertEqual(worker.queue_class, CustomQueue)

    def test_custom_queue_class_is_not_global(self):
        """Ensure Worker custom queue class is not global."""
        q = CustomQueue()
        worker_custom = Worker([q], queue_class=CustomQueue)
        q_generic = Queue()
        worker_generic = Worker([q_generic])
        self.assertEqual(worker_custom.queue_class, CustomQueue)
        self.assertEqual(worker_generic.queue_class, Queue)
        self.assertEqual(Worker.queue_class, Queue)

    def test_custom_job_class_is_not_global(self):
        """Ensure Worker custom job class is not global."""
        q = Queue()
        worker_custom = Worker([q], job_class=CustomJob)
        q_generic = Queue()
        worker_generic = Worker([q_generic])
        self.assertEqual(worker_custom.job_class, CustomJob)
        self.assertEqual(worker_generic.job_class, Job)
        self.assertEqual(Worker.job_class, Job)

    def test_work_via_simpleworker(self):
        """Worker processes work, with forking disabled,
        then returns."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = SimpleWorker([fooq, barq])
        self.assertEqual(w.work(burst=True), False,
                         'Did not expect any work on the queue.')

        job = fooq.enqueue(say_pid)
        self.assertEqual(w.work(burst=True), True,
                         'Expected at least some work done.')
        self.assertEqual(job.result, os.getpid(),
                         'PID mismatch, fork() is not supposed to happen here')

    def test_simpleworker_heartbeat_ttl(self):
        """SimpleWorker's key must last longer than job.timeout when working"""
        queue = Queue('foo')

        worker = SimpleWorker([queue])
        job_timeout = 300
        job = queue.enqueue(save_key_ttl, worker.key, job_timeout=job_timeout)
        worker.work(burst=True)
        job.refresh()
        self.assertGreater(job.meta['ttl'], job_timeout)

    def test_prepare_job_execution(self):
        """Prepare job execution does the necessary bookkeeping."""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)
        worker = Worker([queue])
        worker.prepare_job_execution(job)

        # Updates working queue
        registry = StartedJobRegistry(connection=self.testconn)
        self.assertEqual(registry.get_job_ids(), [job.id])

        # Updates worker statuses
        self.assertEqual(worker.get_state(), 'busy')
        self.assertEqual(worker.get_current_job_id(), job.id)

    def test_prepare_job_execution_inf_timeout(self):
        """Prepare job execution handles infinite job timeout"""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(long_running_job,
                            args=(1,),
                            job_timeout=-1)
        worker = Worker([queue])
        worker.prepare_job_execution(job)

        # Updates working queue
        registry = StartedJobRegistry(connection=self.testconn)
        self.assertEqual(registry.get_job_ids(), [job.id])

        # Score in queue is +inf
        self.assertEqual(self.testconn.zscore(registry.key, job.id), float('Inf'))

    def test_work_unicode_friendly(self):
        """Worker processes work with unicode description, then quits."""
        q = Queue('foo')
        w = Worker([q])
        job = q.enqueue('tests.fixtures.say_hello', name='Adam',
                        description='你好 世界!')
        self.assertEqual(w.work(burst=True), True,
                         'Expected at least some work done.')
        self.assertEqual(job.result, 'Hi there, Adam!')
        self.assertEqual(job.description, '你好 世界!')

    def test_work_log_unicode_friendly(self):
        """Worker process work with unicode or str other than pure ascii content,
        logging work properly"""
        q = Queue("foo")
        w = Worker([q])

        job = q.enqueue('tests.fixtures.say_hello', name='阿达姆',
                        description='你好 世界!')
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        job = q.enqueue('tests.fixtures.say_hello_unicode', name='阿达姆',
                        description='你好 世界!')
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_suspend_worker_execution(self):
        """Test Pause Worker Execution"""

        SENTINEL_FILE = '/tmp/rq-tests.txt'  # noqa

        try:
            # Remove the sentinel if it is leftover from a previous test run
            os.remove(SENTINEL_FILE)
        except OSError as e:
            if e.errno != 2:
                raise

        q = Queue()
        q.enqueue(create_file, SENTINEL_FILE)

        w = Worker([q])

        suspend(self.testconn)

        w.work(burst=True)
        assert q.count == 1

        # Should not have created evidence of execution
        self.assertEqual(os.path.exists(SENTINEL_FILE), False)

        resume(self.testconn)
        w.work(burst=True)
        assert q.count == 0
        self.assertEqual(os.path.exists(SENTINEL_FILE), True)

    @slow
    def test_suspend_with_duration(self):
        q = Queue()
        for _ in range(5):
            q.enqueue(do_nothing)

        w = Worker([q])

        # This suspends workers for working for 2 second
        suspend(self.testconn, 2)

        # So when this burst of work happens the queue should remain at 5
        w.work(burst=True)
        assert q.count == 5

        sleep(3)

        # The suspension should be expired now, and a burst of work should now clear the queue
        w.work(burst=True)
        assert q.count == 0

    def test_worker_hash_(self):
        """Workers are hashed by their .name attribute"""
        q = Queue('foo')
        w1 = Worker([q], name="worker1")
        w2 = Worker([q], name="worker2")
        w3 = Worker([q], name="worker1")
        worker_set = set([w1, w2, w3])
        self.assertEqual(len(worker_set), 2)

    def test_worker_sets_birth(self):
        """Ensure worker correctly sets worker birth date."""
        q = Queue()
        w = Worker([q])

        w.register_birth()

        birth_date = w.birth_date
        self.assertIsNotNone(birth_date)
        self.assertEqual(type(birth_date).__name__, 'datetime')

    def test_worker_sets_death(self):
        """Ensure worker correctly sets worker death date."""
        q = Queue()
        w = Worker([q])

        w.register_death()

        death_date = w.death_date
        self.assertIsNotNone(death_date)
        self.assertIsInstance(death_date, datetime)

    def test_clean_queue_registries(self):
        """worker.clean_registries sets last_cleaned_at and cleans registries."""
        foo_queue = Queue('foo', connection=self.testconn)
        foo_registry = StartedJobRegistry('foo', connection=self.testconn)
        self.testconn.zadd(foo_registry.key, {'foo': 1})
        self.assertEqual(self.testconn.zcard(foo_registry.key), 1)

        bar_queue = Queue('bar', connection=self.testconn)
        bar_registry = StartedJobRegistry('bar', connection=self.testconn)
        self.testconn.zadd(bar_registry.key, {'bar': 1})
        self.assertEqual(self.testconn.zcard(bar_registry.key), 1)

        worker = Worker([foo_queue, bar_queue])
        self.assertEqual(worker.last_cleaned_at, None)
        worker.clean_registries()
        self.assertNotEqual(worker.last_cleaned_at, None)
        self.assertEqual(self.testconn.zcard(foo_registry.key), 0)
        self.assertEqual(self.testconn.zcard(bar_registry.key), 0)

        # worker.clean_registries() only runs once every 15 minutes
        # If we add another key, calling clean_registries() should do nothing
        self.testconn.zadd(bar_registry.key, {'bar': 1})
        worker.clean_registries()
        self.assertEqual(self.testconn.zcard(bar_registry.key), 1)

    def test_should_run_maintenance_tasks(self):
        """Workers should run maintenance tasks on startup and every hour."""
        queue = Queue(connection=self.testconn)
        worker = Worker(queue)
        self.assertTrue(worker.should_run_maintenance_tasks)

        worker.last_cleaned_at = utcnow()
        self.assertFalse(worker.should_run_maintenance_tasks)
        worker.last_cleaned_at = utcnow() - timedelta(seconds=3700)
        self.assertTrue(worker.should_run_maintenance_tasks)

    def test_worker_calls_clean_registries(self):
        """Worker calls clean_registries when run."""
        queue = Queue(connection=self.testconn)
        registry = StartedJobRegistry(connection=self.testconn)
        self.testconn.zadd(registry.key, {'foo': 1})

        worker = Worker(queue, connection=self.testconn)
        worker.work(burst=True)
        self.assertEqual(self.testconn.zcard(registry.key), 0)

    def test_job_dependency_race_condition(self):
        """Dependencies added while the job gets finished shouldn't get lost."""

        # This patches the enqueue_dependents to enqueue a new dependency AFTER
        # the original code was executed.
        orig_enqueue_dependents = Queue.enqueue_dependents

        def new_enqueue_dependents(self, job, *args, **kwargs):
            orig_enqueue_dependents(self, job, *args, **kwargs)
            if hasattr(Queue, '_add_enqueue') and Queue._add_enqueue is not None and Queue._add_enqueue.id == job.id:
                Queue._add_enqueue = None
                Queue().enqueue_call(say_hello, depends_on=job)

        Queue.enqueue_dependents = new_enqueue_dependents

        q = Queue()
        w = Worker([q])
        with mock.patch.object(Worker, 'execute_job', wraps=w.execute_job) as mocked:
            parent_job = q.enqueue(say_hello, result_ttl=0)
            Queue._add_enqueue = parent_job
            job = q.enqueue_call(say_hello, depends_on=parent_job)
            w.work(burst=True)
            job = Job.fetch(job.id)
            self.assertEqual(job.get_status(), JobStatus.FINISHED)

            # The created spy checks two issues:
            # * before the fix of #739, 2 of the 3 jobs where executed due
            #   to the race condition
            # * during the development another issue was fixed:
            #   due to a missing pipeline usage in Queue.enqueue_job, the job
            #   which was enqueued before the "rollback" was executed twice.
            #   So before that fix the call count was 4 instead of 3
            self.assertEqual(mocked.call_count, 3)

    def test_self_modification_persistence(self):
        """Make sure that any meta modification done by
        the job itself persists completely through the
        queue/worker/job stack."""
        q = Queue()
        # Also make sure that previously existing metadata
        # persists properly
        job = q.enqueue(modify_self, meta={'foo': 'bar', 'baz': 42},
                        args=[{'baz': 10, 'newinfo': 'waka'}])

        w = Worker([q])
        w.work(burst=True)

        job_check = Job.fetch(job.id)
        self.assertEqual(job_check.meta['foo'], 'bar')
        self.assertEqual(job_check.meta['baz'], 10)
        self.assertEqual(job_check.meta['newinfo'], 'waka')

    def test_self_modification_persistence_with_error(self):
        """Make sure that any meta modification done by
        the job itself persists completely through the
        queue/worker/job stack -- even if the job errored"""
        q = Queue()
        # Also make sure that previously existing metadata
        # persists properly
        job = q.enqueue(modify_self_and_error, meta={'foo': 'bar', 'baz': 42},
                        args=[{'baz': 10, 'newinfo': 'waka'}])

        w = Worker([q])
        w.work(burst=True)

        # Postconditions
        self.assertEqual(q.count, 0)
        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(w.get_current_job_id(), None)

        job_check = Job.fetch(job.id)
        self.assertEqual(job_check.meta['foo'], 'bar')
        self.assertEqual(job_check.meta['baz'], 10)
        self.assertEqual(job_check.meta['newinfo'], 'waka')

    @mock.patch('rq.worker.logger.info')
    def test_log_result_lifespan_true(self, mock_logger_info):
        """Check that log_result_lifespan True causes job lifespan to be logged."""
        q = Queue()

        w = Worker([q])
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.perform_job(job, q)
        mock_logger_info.assert_called_with('Result is kept for %s seconds', 10)
        self.assertIn('Result is kept for %s seconds', [c[0][0] for c in mock_logger_info.call_args_list])

    @mock.patch('rq.worker.logger.info')
    def test_log_result_lifespan_false(self, mock_logger_info):
        """Check that log_result_lifespan False causes job lifespan to not be logged."""
        q = Queue()

        class TestWorker(Worker):
            log_result_lifespan = False

        w = TestWorker([q])
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.perform_job(job, q)
        self.assertNotIn('Result is kept for 10 seconds', [c[0][0] for c in mock_logger_info.call_args_list])

    @mock.patch('rq.worker.logger.info')
    def test_log_job_description_true(self, mock_logger_info):
        """Check that log_job_description True causes job lifespan to be logged."""
        q = Queue()
        w = Worker([q])
        q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.dequeue_job_and_maintain_ttl(10)
        self.assertIn("Frank", mock_logger_info.call_args[0][2])

    @mock.patch('rq.worker.logger.info')
    def test_log_job_description_false(self, mock_logger_info):
        """Check that log_job_description False causes job lifespan to not be logged."""
        q = Queue()
        w = Worker([q], log_job_description=False)
        q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w.dequeue_job_and_maintain_ttl(10)
        self.assertNotIn("Frank", mock_logger_info.call_args[0][2])

    def test_worker_version(self):
        q = Queue()
        w = Worker([q])
        w.version = '0.0.0'
        w.register_birth()
        self.assertEqual(w.version, '0.0.0')
        w.refresh()
        self.assertEqual(w.version, '0.0.0')
        # making sure that version is preserved when worker is retrieved by key
        worker = Worker.find_by_key(w.key)
        self.assertEqual(worker.version, '0.0.0')

    def test_python_version(self):
        python_version = sys.version
        q = Queue()
        w = Worker([q])
        w.register_birth()
        self.assertEqual(w.python_version, python_version)
        # now patching version
        python_version = 'X.Y.Z.final'  # dummy version
        self.assertNotEqual(python_version, sys.version)  # otherwise tests are pointless
        w2 = Worker([q])
        w2.python_version = python_version
        w2.register_birth()
        self.assertEqual(w2.python_version, python_version)
        # making sure that version is preserved when worker is retrieved by key
        worker = Worker.find_by_key(w2.key)
        self.assertEqual(worker.python_version, python_version)


def wait_and_kill_work_horse(pid, time_to_wait=0.0):
    time.sleep(time_to_wait)
    os.kill(pid, signal.SIGKILL)


class TimeoutTestCase:
    def setUp(self):
        # we want tests to fail if signal are ignored and the work remain
        # running, so set a signal to kill them after X seconds
        self.killtimeout = 15
        signal.signal(signal.SIGALRM, self._timeout)
        signal.alarm(self.killtimeout)

    def _timeout(self, signal, frame):
        raise AssertionError(
            "test still running after %i seconds, likely the worker wasn't shutdown correctly" % self.killtimeout
        )


class WorkerShutdownTestCase(TimeoutTestCase, RQTestCase):
    @slow
    def test_idle_worker_warm_shutdown(self):
        """worker with no ongoing job receiving single SIGTERM signal and shutting down"""
        w = Worker('foo')
        self.assertFalse(w._stop_requested)
        p = Process(target=kill_worker, args=(os.getpid(), False))
        p.start()

        w.work()

        p.join(1)
        self.assertFalse(w._stop_requested)

    @slow
    def test_working_worker_warm_shutdown(self):
        """worker with an ongoing job receiving single SIGTERM signal, allowing job to finish then shutting down"""
        fooq = Queue('foo')
        w = Worker(fooq)

        sentinel_file = '/tmp/.rq_sentinel_warm'
        fooq.enqueue(create_file_after_timeout, sentinel_file, 2)
        self.assertFalse(w._stop_requested)
        p = Process(target=kill_worker, args=(os.getpid(), False))
        p.start()

        w.work()

        p.join(2)
        self.assertFalse(p.is_alive())
        self.assertTrue(w._stop_requested)
        self.assertTrue(os.path.exists(sentinel_file))

        self.assertIsNotNone(w.shutdown_requested_date)
        self.assertEqual(type(w.shutdown_requested_date).__name__, 'datetime')

    @slow
    def test_working_worker_cold_shutdown(self):
        """Busy worker shuts down immediately on double SIGTERM signal"""
        fooq = Queue('foo')
        w = Worker(fooq)
        sentinel_file = '/tmp/.rq_sentinel_cold'
        fooq.enqueue(create_file_after_timeout, sentinel_file, 2)
        self.assertFalse(w._stop_requested)
        p = Process(target=kill_worker, args=(os.getpid(), True))
        p.start()

        self.assertRaises(SystemExit, w.work)

        p.join(1)
        self.assertTrue(w._stop_requested)
        self.assertFalse(os.path.exists(sentinel_file))

        shutdown_requested_date = w.shutdown_requested_date
        self.assertIsNotNone(shutdown_requested_date)
        self.assertEqual(type(shutdown_requested_date).__name__, 'datetime')

    @slow
    def test_work_horse_death_sets_job_failed(self):
        """worker with an ongoing job whose work horse dies unexpectadly (before
        completing the job) should set the job's status to FAILED
        """
        fooq = Queue('foo')
        self.assertEqual(fooq.count, 0)
        w = Worker(fooq)
        sentinel_file = '/tmp/.rq_sentinel_work_horse_death'
        if os.path.exists(sentinel_file):
            os.remove(sentinel_file)
        fooq.enqueue(create_file_after_timeout, sentinel_file, 100)
        job, queue = w.dequeue_job_and_maintain_ttl(5)
        w.fork_work_horse(job, queue)
        p = Process(target=wait_and_kill_work_horse, args=(w._horse_pid, 0.5))
        p.start()
        w.monitor_work_horse(job)
        job_status = job.get_status()
        p.join(1)
        self.assertEqual(job_status, JobStatus.FAILED)
        failed_job_registry = FailedJobRegistry(queue=fooq)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(fooq.count, 0)

    @slow
    def test_work_horse_force_death(self):
        """Simulate a frozen worker that doesn't observe the timeout properly.
        Fake it by artificially setting the timeout of the parent process to
        something much smaller after the process is already forked.
        """
        fooq = Queue('foo')
        self.assertEqual(fooq.count, 0)
        w = Worker(fooq)
        sentinel_file = '/tmp/.rq_sentinel_work_horse_death'
        if os.path.exists(sentinel_file):
            os.remove(sentinel_file)
        fooq.enqueue(create_file_after_timeout, sentinel_file, 100)
        job, queue = w.dequeue_job_and_maintain_ttl(5)
        w.fork_work_horse(job, queue)
        job.timeout = 5
        w.job_monitoring_interval = 1
        now = utcnow()
        w.monitor_work_horse(job)
        fudge_factor = 1
        total_time = w.job_monitoring_interval + 65 + fudge_factor
        self.assertTrue((utcnow() - now).total_seconds() < total_time)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        failed_job_registry = FailedJobRegistry(queue=fooq)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(fooq.count, 0)


def schedule_access_self():
    q = Queue('default', connection=get_current_connection())
    q.enqueue(access_self)


@pytest.mark.skipif(sys.platform == 'darwin', reason='Fails on OS X')
class TestWorkerSubprocess(RQTestCase):
    def setUp(self):
        super(TestWorkerSubprocess, self).setUp()
        db_num = self.testconn.connection_pool.connection_kwargs['db']
        self.redis_url = 'redis://127.0.0.1:6379/%d' % db_num

    def test_run_empty_queue(self):
        """Run the worker in its own process with an empty queue"""
        subprocess.check_call(['rqworker', '-u', self.redis_url, '-b'])

    def test_run_access_self(self):
        """Schedule a job, then run the worker as subprocess"""
        q = Queue()
        job = q.enqueue(access_self)
        subprocess.check_call(['rqworker', '-u', self.redis_url, '-b'])
        registry = FinishedJobRegistry(queue=q)
        self.assertTrue(job in registry)
        assert q.count == 0

    @skipIf('pypy' in sys.version.lower(), 'often times out with pypy')
    def test_run_scheduled_access_self(self):
        """Schedule a job that schedules a job, then run the worker as subprocess"""
        q = Queue()
        job = q.enqueue(schedule_access_self)
        subprocess.check_call(['rqworker', '-u', self.redis_url, '-b'])
        registry = FinishedJobRegistry(queue=q)
        self.assertTrue(job in registry)
        assert q.count == 0


@pytest.mark.skipif(sys.platform == 'darwin', reason='requires Linux signals')
@skipIf('pypy' in sys.version.lower(), 'these tests often fail on pypy')
class HerokuWorkerShutdownTestCase(TimeoutTestCase, RQTestCase):
    def setUp(self):
        super(HerokuWorkerShutdownTestCase, self).setUp()
        self.sandbox = '/tmp/rq_shutdown/'
        os.makedirs(self.sandbox)

    def tearDown(self):
        shutil.rmtree(self.sandbox, ignore_errors=True)

    @slow
    def test_immediate_shutdown(self):
        """Heroku work horse shutdown with immediate (0 second) kill"""
        p = Process(target=run_dummy_heroku_worker, args=(self.sandbox, 0))
        p.start()
        time.sleep(0.5)

        os.kill(p.pid, signal.SIGRTMIN)

        p.join(2)
        self.assertEqual(p.exitcode, 1)
        self.assertTrue(os.path.exists(os.path.join(self.sandbox, 'started')))
        self.assertFalse(os.path.exists(os.path.join(self.sandbox, 'finished')))
        with open(os.path.join(self.sandbox, 'stderr.log')) as f:
            stderr = f.read().strip('\n')
            err = 'ShutDownImminentException: shut down imminent (signal: SIGRTMIN)'
            self.assertTrue(stderr.endswith(err), stderr)

    @slow
    def test_1_sec_shutdown(self):
        """Heroku work horse shutdown with 1 second kill"""
        p = Process(target=run_dummy_heroku_worker, args=(self.sandbox, 1))
        p.start()
        time.sleep(0.5)

        os.kill(p.pid, signal.SIGRTMIN)
        time.sleep(0.1)
        self.assertEqual(p.exitcode, None)
        p.join(2)
        self.assertEqual(p.exitcode, 1)

        self.assertTrue(os.path.exists(os.path.join(self.sandbox, 'started')))
        self.assertFalse(os.path.exists(os.path.join(self.sandbox, 'finished')))
        with open(os.path.join(self.sandbox, 'stderr.log')) as f:
            stderr = f.read().strip('\n')
            err = 'ShutDownImminentException: shut down imminent (signal: SIGALRM)'
            self.assertTrue(stderr.endswith(err), stderr)

    @slow
    def test_shutdown_double_sigrtmin(self):
        """Heroku work horse shutdown with long delay but SIGRTMIN sent twice"""
        p = Process(target=run_dummy_heroku_worker, args=(self.sandbox, 10))
        p.start()
        time.sleep(0.5)

        os.kill(p.pid, signal.SIGRTMIN)
        # we have to wait a short while otherwise the second signal wont bet processed.
        time.sleep(0.1)
        os.kill(p.pid, signal.SIGRTMIN)
        p.join(2)
        self.assertEqual(p.exitcode, 1)

        self.assertTrue(os.path.exists(os.path.join(self.sandbox, 'started')))
        self.assertFalse(os.path.exists(os.path.join(self.sandbox, 'finished')))
        with open(os.path.join(self.sandbox, 'stderr.log')) as f:
            stderr = f.read().strip('\n')
            err = 'ShutDownImminentException: shut down imminent (signal: SIGRTMIN)'
            self.assertTrue(stderr.endswith(err), stderr)

    @mock.patch('rq.worker.logger.info')
    def test_handle_shutdown_request(self, mock_logger_info):
        """Mutate HerokuWorker so _horse_pid refers to an artificial process
        and test handle_warm_shutdown_request"""
        w = HerokuWorker('foo')

        path = os.path.join(self.sandbox, 'shouldnt_exist')
        p = Process(target=create_file_after_timeout, args=(path, 2))
        p.start()
        self.assertEqual(p.exitcode, None)

        w._horse_pid = p.pid
        w.handle_warm_shutdown_request()
        p.join(2)
        # would expect p.exitcode to be -34 but for some reason os.waitpid is setting it to None, even though
        # the process has ended
        self.assertEqual(p.exitcode, None)
        self.assertFalse(os.path.exists(path))
        mock_logger_info.assert_called_with('Killed horse pid %s', p.pid)

    def test_handle_shutdown_request_no_horse(self):
        """Mutate HerokuWorker so _horse_pid refers to non existent process
        and test handle_warm_shutdown_request"""
        w = HerokuWorker('foo')

        w._horse_pid = 19999
        w.handle_warm_shutdown_request()


class TestExceptionHandlerMessageEncoding(RQTestCase):

    def setUp(self):
        super(TestExceptionHandlerMessageEncoding, self).setUp()
        self.worker = Worker("foo")
        self.worker._exc_handlers = []
        # Mimic how exception info is actually passed forwards
        try:
            raise Exception(u"💪")
        except Exception:
            self.exc_info = sys.exc_info()

    def test_handle_exception_handles_non_ascii_in_exception_message(self):
        """worker.handle_exception doesn't crash on non-ascii in exception message."""
        self.worker.handle_exception(Mock(), *self.exc_info)
