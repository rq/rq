# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import shutil
from datetime import timedelta
from time import sleep
import signal
import time
from multiprocessing import Process
import subprocess
import sys

import pytest
import mock
from mock import Mock

from tests import RQTestCase, slow
from tests.fixtures import (create_file, create_file_after_timeout,
                            div_by_zero, do_nothing, say_hello, say_pid,
                            run_dummy_heroku_worker, access_self,
                            modify_self, modify_self_and_error)
from tests.helpers import strip_microseconds

from rq import (get_failed_queue, Queue, SimpleWorker, Worker,
                get_current_connection)
from rq.compat import as_text, PY2
from rq.job import Job, JobStatus
from rq.registry import StartedJobRegistry
from rq.suspension import resume, suspend
from rq.utils import utcnow
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
        w.register_death()

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
        """Unreadable jobs are put on the failed queue."""
        q = Queue()
        failed_q = get_failed_queue()

        self.assertEqual(failed_q.count, 0)
        self.assertEqual(q.count, 0)

        # NOTE: We have to fake this enqueueing for this test case.
        # What we're simulating here is a call to a function that is not
        # importable from the worker process.
        job = Job.create(func=div_by_zero, args=(3,))
        job.save()
        data = self.testconn.hget(job.key, 'data')
        invalid_data = data.replace(b'div_by_zero', b'nonexisting')
        assert data != invalid_data
        self.testconn.hset(job.key, 'data', invalid_data)

        # We use the low-level internal function to enqueue any data (bypassing
        # validity checks)
        q.push_job_id(job.id)

        self.assertEqual(q.count, 1)

        # All set, we're going to process it
        w = Worker([q])
        w.work(burst=True)   # should silently pass
        self.assertEqual(q.count, 0)
        self.assertEqual(failed_q.count, 1)

    def test_work_fails(self):
        """Failing jobs are put on the failed queue."""
        q = Queue()
        failed_q = get_failed_queue()

        # Preconditions
        self.assertEqual(failed_q.count, 0)
        self.assertEqual(q.count, 0)

        # Action
        job = q.enqueue(div_by_zero)
        self.assertEqual(q.count, 1)

        # keep for later
        enqueued_at_date = strip_microseconds(job.enqueued_at)

        w = Worker([q])
        w.work(burst=True)  # should silently pass

        # Postconditions
        self.assertEqual(q.count, 0)
        self.assertEqual(failed_q.count, 1)
        self.assertEqual(w.get_current_job_id(), None)

        # Check the job
        job = Job.fetch(job.id)
        self.assertEqual(job.origin, q.name)

        # Should be the original enqueued_at date, not the date of enqueueing
        # to the failed queue
        self.assertEqual(job.enqueued_at, enqueued_at_date)
        self.assertIsNotNone(job.exc_info)  # should contain exc_info

    def test_custom_exc_handling(self):
        """Custom exception handling."""
        def black_hole(job, *exc_info):
            # Don't fall through to default behaviour (moving to failed queue)
            return False

        q = Queue()
        failed_q = get_failed_queue()

        # Preconditions
        self.assertEqual(failed_q.count, 0)
        self.assertEqual(q.count, 0)

        # Action
        job = q.enqueue(div_by_zero)
        self.assertEqual(q.count, 1)

        w = Worker([q], exception_handlers=black_hole)
        w.work(burst=True)  # should silently pass

        # Postconditions
        self.assertEqual(q.count, 0)
        self.assertEqual(failed_q.count, 0)

        # Check the job
        job = Job.fetch(job.id)
        self.assertEqual(job.is_failed, True)

    def test_cancelled_jobs_arent_executed(self):  # noqa
        """Cancelling jobs."""

        SENTINEL_FILE = '/tmp/rq-tests.txt'

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
                        timeout=1)

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
        self.assertNotEqual(self.testconn._ttl(job.key), 0)
        self.assertNotIn(job.get_id().encode('utf-8'), self.testconn.lrange(q.key, 0, -1))

        # Job with -1 result_ttl don't expire
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=-1)
        w = Worker([q])
        self.assertIn(job.get_id().encode('utf-8'), self.testconn.lrange(q.key, 0, -1))
        w.work(burst=True)
        self.assertEqual(self.testconn._ttl(job.key), -1)
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
        self.assertEqual(w.work(burst=True), True,
                         'Expected at least some work done.')
        job = q.enqueue('tests.fixtures.say_hello_unicode', name='阿达姆',
                        description='你好 世界!')
        self.assertEqual(w.work(burst=True), True,
                         'Expected at least some work done.')

    def test_suspend_worker_execution(self):
        """Test Pause Worker Execution"""

        SENTINEL_FILE = '/tmp/rq-tests.txt'

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
        self.assertEqual(type(death_date).__name__, 'datetime')

    def test_clean_queue_registries(self):
        """worker.clean_registries sets last_cleaned_at and cleans registries."""
        foo_queue = Queue('foo', connection=self.testconn)
        foo_registry = StartedJobRegistry('foo', connection=self.testconn)
        self.testconn.zadd(foo_registry.key, 1, 'foo')
        self.assertEqual(self.testconn.zcard(foo_registry.key), 1)

        bar_queue = Queue('bar', connection=self.testconn)
        bar_registry = StartedJobRegistry('bar', connection=self.testconn)
        self.testconn.zadd(bar_registry.key, 1, 'bar')
        self.assertEqual(self.testconn.zcard(bar_registry.key), 1)

        worker = Worker([foo_queue, bar_queue])
        self.assertEqual(worker.last_cleaned_at, None)
        worker.clean_registries()
        self.assertNotEqual(worker.last_cleaned_at, None)
        self.assertEqual(self.testconn.zcard(foo_registry.key), 0)
        self.assertEqual(self.testconn.zcard(bar_registry.key), 0)

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
        self.testconn.zadd(registry.key, 1, 'foo')

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
        self.assertEqual(set(job_check.meta.keys()),
                         set(['foo', 'baz', 'newinfo']))
        self.assertEqual(job_check.meta['foo'], 'bar')
        self.assertEqual(job_check.meta['baz'], 10)
        self.assertEqual(job_check.meta['newinfo'], 'waka')

    def test_self_modification_persistence_with_error(self):
        """Make sure that any meta modification done by
        the job itself persists completely through the
        queue/worker/job stack -- even if the job errored"""
        q = Queue()
        failed_q = get_failed_queue()
        # Also make sure that previously existing metadata
        # persists properly
        job = q.enqueue(modify_self_and_error, meta={'foo': 'bar', 'baz': 42},
                        args=[{'baz': 10, 'newinfo': 'waka'}])

        w = Worker([q])
        w.work(burst=True)

        # Postconditions
        self.assertEqual(q.count, 0)
        self.assertEqual(failed_q.count, 1)
        self.assertEqual(w.get_current_job_id(), None)

        job_check = Job.fetch(job.id)
        self.assertEqual(set(job_check.meta.keys()),
                         set(['foo', 'baz', 'newinfo']))
        self.assertEqual(job_check.meta['foo'], 'bar')
        self.assertEqual(job_check.meta['baz'], 10)
        self.assertEqual(job_check.meta['newinfo'], 'waka')


def kill_worker(pid, double_kill):
    # wait for the worker to be started over on the main process
    time.sleep(0.5)
    os.kill(pid, signal.SIGTERM)
    if double_kill:
        # give the worker time to switch signal handler
        time.sleep(0.5)
        os.kill(pid, signal.SIGTERM)


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
        """worker with an ongoing job receiving double SIGTERM signal and shutting down immediately"""
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
        failed_q = get_failed_queue()
        self.assertEqual(failed_q.count, 0)
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
        self.assertEqual(failed_q.count, 1)
        self.assertEqual(fooq.count, 0)


def schedule_access_self():
    q = Queue('default', connection=get_current_connection())
    q.enqueue(access_self)


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
        q.enqueue(access_self)
        subprocess.check_call(['rqworker', '-u', self.redis_url, '-b'])
        assert get_failed_queue().count == 0
        assert q.count == 0

    # @skipIf('pypy' in sys.version.lower(), 'often times out with pypy')
    def test_run_scheduled_access_self(self):
        """Schedule a job that schedules a job, then run the worker as subprocess"""
        if 'pypy' in sys.version.lower():
            # horrible bodge until we drop 2.6 support and can use skipIf
            return
        q = Queue()
        q.enqueue(schedule_access_self)
        subprocess.check_call(['rqworker', '-u', self.redis_url, '-b'])
        assert get_failed_queue().count == 0
        assert q.count == 0


@pytest.mark.skipif(sys.platform == 'darwin', reason='requires Linux signals')
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

    def test_handle_shutdown_request(self):
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
        self.assertEqual(p.exitcode, -34)
        self.assertFalse(os.path.exists(path))

    def test_handle_shutdown_request_no_horse(self):
        """Mutate HerokuWorker so _horse_pid refers to non existent process
        and test handle_warm_shutdown_request"""
        w = HerokuWorker('foo')

        w._horse_pid = 19999
        w.handle_warm_shutdown_request()        


class TestExceptionHandlerMessageEncoding(RQTestCase):
    def test_handle_exception_handles_non_ascii_in_exception_message(self):
        """Test that handle_exception doesn't crash on non-ascii in exception message."""
        self.worker.handle_exception(Mock(), *self.exc_info)

    def test_move_to_failed_queue_handles_non_ascii_in_exception_message(self):
        """Test that move_to_failed_queue doesn't crash on non-ascii in exception message."""
        self.worker.move_to_failed_queue(Mock(), *self.exc_info)

    def setUp(self):
        super(TestExceptionHandlerMessageEncoding, self).setUp()
        self.worker = Worker("foo")
        self.worker._exc_handlers = []
        self.worker.failed_queue = Mock()
        # Mimic how exception info is actually passed forwards
        try:
            raise Exception(u"💪")
        except:
            self.exc_info = sys.exc_info()
