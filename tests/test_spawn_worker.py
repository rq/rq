import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import zlib
from datetime import datetime, timedelta, timezone
from multiprocessing import Process
from time import sleep
from unittest import mock, skipIf
from unittest.mock import Mock

import psutil
import pytest
import redis.exceptions
from redis import Redis

from rq import Queue, SimpleWorker, Worker
from rq.defaults import DEFAULT_MAINTENANCE_TASK_INTERVAL, DEFAULT_WORKER_TTL
from rq.job import Job, JobStatus, Retry
from rq.registry import FailedJobRegistry, FinishedJobRegistry, StartedJobRegistry
from rq.results import Result
from rq.serializers import JSONSerializer
from rq.suspension import resume, suspend
from rq.utils import as_text, get_version, now
from rq.version import VERSION
from rq.worker import HerokuWorker, RandomWorker, RoundRobinWorker, WorkerStatus, SpawnWorker
from tests import RQTestCase, find_empty_redis_database, slow
from tests.fixtures import (
    CustomJob,
    access_self,
    create_file,
    create_file_after_timeout,
    create_file_after_timeout_and_setpgrp,
    div_by_zero,
    do_nothing,
    kill_worker,
    launch_process_within_worker_and_store_pid,
    long_running_job,
    modify_self,
    modify_self_and_error,
    raise_exc_mock,
    resume_worker,
    run_dummy_heroku_worker,
    save_key_ttl,
    say_hello,
    say_pid,
)


class CustomQueue(Queue):
    pass


class TestWorker(RQTestCase):

    def test_work_and_quit(self):
        """Worker processes work, then quits."""
        queue = Queue('foo', connection=self.connection)
        worker = SpawnWorker([queue])
        self.assertEqual(worker.work(burst=True), False, 'Did not expect any work on the queue.')

        job = queue.enqueue(say_hello, name='Frank')
        worker.work(burst=True)
        result = job.latest_result()
        self.assertEqual(result.type, Result.Type.SUCCESSFUL)

    def test_work_fails(self):
        """Failing jobs are put on the failed queue."""
        q = Queue(connection=self.connection)
        self.assertEqual(q.count, 0)

        # Action
        job = q.enqueue(div_by_zero)
        self.assertEqual(q.count, 1)

        # keep for later
        enqueued_at_date = job.enqueued_at

        w = Worker([q])
        w.work(burst=True)

        # Postconditions
        self.assertEqual(q.count, 0)
        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(w.get_current_job_id(), None)

        # Check the job
        job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.origin, q.name)

        # Should be the original enqueued_at date, not the date of enqueueing
        # to the failed queue
        self.assertEqual(job.enqueued_at.replace(tzinfo=timezone.utc).timestamp(), enqueued_at_date.timestamp())
        if job.supports_redis_streams:
            result = Result.fetch_latest(job)
            self.assertTrue(result.exc_string)
            self.assertEqual(result.type, Result.Type.FAILED)

    def test_horse_fails(self):
        """Tests that job status is set to FAILED even if horse unexpectedly fails"""
        q = Queue(connection=self.connection)
        self.assertEqual(q.count, 0)

        # Action
        job = q.enqueue(say_hello)
        self.assertEqual(q.count, 1)

        # keep for later
        enqueued_at_date = job.enqueued_at

        w = Worker([q])
        with mock.patch.object(w, 'perform_job', new_callable=raise_exc_mock):
            w.work(burst=True)  # should silently pass

        # Postconditions
        self.assertEqual(q.count, 0)
        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in failed_job_registry)
        self.assertEqual(w.get_current_job_id(), None)

        # Check the job
        job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.origin, q.name)

        # Should be the original enqueued_at date, not the date of enqueueing
        # to the failed queue
        self.assertEqual(job.enqueued_at.replace(tzinfo=timezone.utc).timestamp(), enqueued_at_date.timestamp())
        self.assertTrue(job.exc_info)  # should contain exc_info


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
        w = Worker('foo', connection=self.connection)
        self.assertFalse(w._stop_requested)
        p = Process(target=kill_worker, args=(os.getpid(), False))
        p.start()

        w.work()

        p.join(1)
        self.assertFalse(w._stop_requested)

    @slow
    def test_working_worker_cold_shutdown(self):
        """Busy worker shuts down immediately on double SIGTERM signal"""
        fooq = Queue('foo', connection=self.connection)
        w = Worker(fooq)

        sentinel_file = '/tmp/.rq_sentinel_cold'
        self.assertFalse(
            os.path.exists(sentinel_file), '{sentinel_file} file should not exist yet, delete that file and try again.'
        )
        fooq.enqueue(create_file_after_timeout, sentinel_file, 5)
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
