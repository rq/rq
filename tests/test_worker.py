# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os

from rq import get_failed_queue, Queue, Worker
from rq.compat import as_text
from rq.job import Job, Status

from tests import RQTestCase, slow
from tests.fixtures import (create_file, create_file_after_timeout, div_by_zero,
                            say_hello)
from tests.helpers import strip_microseconds


class CustomJob(Job):
    pass


class TestWorker(RQTestCase):
    def test_create_worker(self):
        """Worker creation."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = Worker([fooq, barq])
        self.assertEquals(w.queues, [fooq, barq])

    def test_work_and_quit(self):
        """Worker processes work, then quits."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = Worker([fooq, barq])
        self.assertEquals(w.work(burst=True), False,
                          'Did not expect any work on the queue.')

        fooq.enqueue(say_hello, name='Frank')
        self.assertEquals(w.work(burst=True), True,
                          'Expected at least some work done.')

    def test_worker_ttl(self):
        """Worker ttl."""
        w = Worker([])
        w.register_birth()  # ugly: our test should only call public APIs
        [worker_key] = self.testconn.smembers(Worker.redis_workers_keys)
        self.assertIsNotNone(self.testconn.ttl(worker_key))
        w.register_death()

    def test_work_via_string_argument(self):
        """Worker processes work fed via string arguments."""
        q = Queue('foo')
        w = Worker([q])
        job = q.enqueue('tests.fixtures.say_hello', name='Frank')
        self.assertEquals(w.work(burst=True), True,
                          'Expected at least some work done.')
        self.assertEquals(job.result, 'Hi there, Frank!')

    def test_work_is_unreadable(self):
        """Unreadable jobs are put on the failed queue."""
        q = Queue()
        failed_q = get_failed_queue()

        self.assertEquals(failed_q.count, 0)
        self.assertEquals(q.count, 0)

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

        self.assertEquals(q.count, 1)

        # All set, we're going to process it
        w = Worker([q])
        w.work(burst=True)   # should silently pass
        self.assertEquals(q.count, 0)
        self.assertEquals(failed_q.count, 1)

    def test_work_fails(self):
        """Failing jobs are put on the failed queue."""
        q = Queue()
        failed_q = get_failed_queue()

        # Preconditions
        self.assertEquals(failed_q.count, 0)
        self.assertEquals(q.count, 0)

        # Action
        job = q.enqueue(div_by_zero)
        self.assertEquals(q.count, 1)

        # keep for later
        enqueued_at_date = strip_microseconds(job.enqueued_at)

        w = Worker([q])
        w.work(burst=True)  # should silently pass

        # Postconditions
        self.assertEquals(q.count, 0)
        self.assertEquals(failed_q.count, 1)

        # Check the job
        job = Job.fetch(job.id)
        self.assertEquals(job.origin, q.name)

        # Should be the original enqueued_at date, not the date of enqueueing
        # to the failed queue
        self.assertEquals(job.enqueued_at, enqueued_at_date)
        self.assertIsNotNone(job.exc_info)  # should contain exc_info

    def test_custom_exc_handling(self):
        """Custom exception handling."""
        def black_hole(job, *exc_info):
            # Don't fall through to default behaviour (moving to failed queue)
            return False

        q = Queue()
        failed_q = get_failed_queue()

        # Preconditions
        self.assertEquals(failed_q.count, 0)
        self.assertEquals(q.count, 0)

        # Action
        job = q.enqueue(div_by_zero)
        self.assertEquals(q.count, 1)

        w = Worker([q], exc_handler=black_hole)
        w.work(burst=True)  # should silently pass

        # Postconditions
        self.assertEquals(q.count, 0)
        self.assertEquals(failed_q.count, 0)

        # Check the job
        job = Job.fetch(job.id)
        self.assertEquals(job.is_failed, True)

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
        assert q.count == 1
        job.cancel()
        assert q.count == 1

        w = Worker([q])
        w.work(burst=True)
        assert q.count == 0

        # Should not have created evidence of execution
        self.assertEquals(os.path.exists(SENTINEL_FILE), False)

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

        self.assertEquals(os.path.exists(sentinel_file), False)
        w.work(burst=True)
        self.assertEquals(os.path.exists(sentinel_file), False)

        # TODO: Having to do the manual refresh() here is really ugly!
        res.refresh()
        self.assertIn('JobTimeoutException', as_text(res.exc_info))

    def test_worker_sets_result_ttl(self):
        """Ensure that Worker properly sets result_ttl for individual jobs."""
        q = Queue()
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w = Worker([q])
        w.work(burst=True)
        self.assertNotEqual(self.testconn.ttl(job.key), 0)

        # Job with -1 result_ttl don't expire
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=-1)
        w = Worker([q])
        w.work(burst=True)
        self.assertEqual(self.testconn.ttl(job.key), -1)

        # Job with result_ttl = 0 gets deleted immediately
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=0)
        w = Worker([q])
        w.work(burst=True)
        self.assertEqual(self.testconn.get(job.key), None)

    def test_worker_sets_job_status(self):
        """Ensure that worker correctly sets job status."""
        q = Queue()
        w = Worker([q])

        job = q.enqueue(say_hello)
        self.assertEqual(job.get_status(), Status.QUEUED)
        self.assertEqual(job.is_queued, True)
        self.assertEqual(job.is_finished, False)
        self.assertEqual(job.is_failed, False)

        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), Status.FINISHED)
        self.assertEqual(job.is_queued, False)
        self.assertEqual(job.is_finished, True)
        self.assertEqual(job.is_failed, False)

        # Failed jobs should set status to "failed"
        job = q.enqueue(div_by_zero, args=(1,))
        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), Status.FAILED)
        self.assertEqual(job.is_queued, False)
        self.assertEqual(job.is_finished, False)
        self.assertEqual(job.is_failed, True)

    def test_job_dependency(self):
        """Enqueue dependent jobs only if their parents don't fail"""
        q = Queue()
        w = Worker([q])
        parent_job = q.enqueue(say_hello)
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), Status.FINISHED)

        parent_job = q.enqueue(div_by_zero)
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        w.work(burst=True)
        job = Job.fetch(job.id)
        self.assertNotEqual(job.get_status(), Status.FINISHED)

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
