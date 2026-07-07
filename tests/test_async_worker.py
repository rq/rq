import os
import time
from multiprocessing import Process
from unittest.mock import patch

from rq.job import JobStatus
from rq.queue import Queue
from rq.worker import AsyncWorker
from tests import RQTestCase
from tests.fixtures import kill_worker, rpush, say_hello


class TestAsyncWorker(RQTestCase):
    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)
        # Non-timing tests don't need the simulated execution to take time.
        patcher = patch.object(AsyncWorker, 'simulated_job_duration', 0)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_burst_finishes_jobs_without_performing_them(self):
        """Admitted jobs land FINISHED, but their functions never run."""
        connection_kwargs = self.connection.connection_pool.connection_kwargs
        first_job = self.queue.enqueue(rpush, 'sentinel', 'value', connection_kwargs)
        second_job = self.queue.enqueue(rpush, 'sentinel', 'value', connection_kwargs)

        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=2)
        processed = worker.work(burst=True)

        self.assertTrue(processed)
        self.assertEqual(first_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(second_job.get_status(), JobStatus.FINISHED)
        self.assertIn(first_job, self.queue.finished_job_registry)
        self.assertIn(second_job, self.queue.finished_job_registry)
        # The job function was never called: no rpush happened.
        self.assertFalse(self.connection.exists('sentinel'))
        self.assertEqual(worker.executions, {})
        self.assertIsNotNone(worker.death_date)

    def test_concurrent_executions_overlap(self):
        """Two simulated 0.5s executions at max_concurrency=2 overlap."""
        self.queue.enqueue(say_hello)
        self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=2)

        with patch.object(AsyncWorker, 'simulated_job_duration', 0.5):
            started_at = time.monotonic()
            worker.work(burst=True)
        self.assertLess(time.monotonic() - started_at, 0.95)

    def test_max_concurrency_one_runs_jobs_sequentially(self):
        """At max_concurrency=1 the same two executions run back to back."""
        self.queue.enqueue(say_hello)
        self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=1)

        with patch.object(AsyncWorker, 'simulated_job_duration', 0.5):
            started_at = time.monotonic()
            worker.work(burst=True)
        self.assertGreaterEqual(time.monotonic() - started_at, 1.0)

    def test_empty_queue_returns_false(self):
        worker = AsyncWorker([self.queue], connection=self.connection)
        self.assertFalse(worker.work(burst=True))

    def test_failed_finalization_does_not_leak_executions(self):
        """A crash while finalizing routes through handle_job_failure and
        leaves no stale entry in worker.executions."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)

        with patch.object(AsyncWorker, 'handle_job_success', side_effect=ValueError('boom')):
            worker.work(burst=True)

        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn(job, self.queue.failed_job_registry)
        self.assertEqual(worker.executions, {})

    def test_hydration_via_all_and_find_by_key(self):
        """Worker discovery constructs AsyncWorker with no queues."""
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()
        try:
            hydrated_workers = AsyncWorker.all(connection=self.connection)
            self.assertEqual([hydrated_worker.name for hydrated_worker in hydrated_workers], [worker.name])
        finally:
            worker.register_death()

    def test_warm_shutdown_drains_in_flight_execution(self):
        """First signal stops admission, lets the in-flight execution finish."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)

        kill_process = Process(target=kill_worker, args=(os.getpid(), False, 0.5))
        kill_process.start()
        with (
            patch.object(AsyncWorker, 'simulated_job_duration', 2),
            patch.object(AsyncWorker, 'blocking_dequeue_timeout', 1),
        ):
            worker.work()
        kill_process.join()

        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(worker.executions, {})

    def test_cold_shutdown_cancels_in_flight_execution(self):
        """Second signal cancels the in-flight execution; work() returns
        cleanly (no SystemExit, unlike the sync worker) and the job stays
        STARTED for the reaper to recover."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)

        kill_process = Process(target=kill_worker, args=(os.getpid(), True, 0.5))
        kill_process.start()
        started_at = time.monotonic()
        with (
            patch.object(AsyncWorker, 'simulated_job_duration', 20),
            patch.object(AsyncWorker, 'blocking_dequeue_timeout', 1),
        ):
            worker.work()
        kill_process.join()

        self.assertLess(time.monotonic() - started_at, 10)
        self.assertEqual(job.get_status(), JobStatus.STARTED)

    def test_idle_shutdown_wakes_after_dequeue_block(self):
        """A signal during an idle blocking dequeue is noticed once the block
        times out."""
        worker = AsyncWorker([self.queue], connection=self.connection)

        kill_process = Process(target=kill_worker, args=(os.getpid(), False, 0.5))
        kill_process.start()
        started_at = time.monotonic()
        with patch.object(AsyncWorker, 'blocking_dequeue_timeout', 1):
            self.assertFalse(worker.work())
        kill_process.join()

        self.assertLess(time.monotonic() - started_at, 4)

    def test_multiple_queues_raise(self):
        other_queue = Queue('other', connection=self.connection)
        with self.assertRaises(ValueError):
            AsyncWorker([self.queue, other_queue], connection=self.connection)

    def test_zero_max_concurrency_raises(self):
        with self.assertRaises(ValueError):
            AsyncWorker([self.queue], connection=self.connection, max_concurrency=0)

    def test_with_scheduler_raises(self):
        worker = AsyncWorker([self.queue], connection=self.connection)
        with self.assertRaises(NotImplementedError):
            worker.work(burst=True, with_scheduler=True)
