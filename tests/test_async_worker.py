import os
import time
from multiprocessing import Process
from unittest.mock import patch

from rq.job import JobStatus
from rq.queue import Queue
from rq.worker import AsyncWorker
from tests import RQTestCase, min_redis_version
from tests.fixtures import kill_worker, rpush, say_hello


@min_redis_version((6, 2, 0))
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
        with patch.object(AsyncWorker, 'simulated_job_duration', 2):
            worker.work()
        kill_process.join()

        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(worker.executions, {})

    def test_cold_shutdown_cancels_in_flight_execution(self):
        """Second signal cancels the in-flight execution; work() returns
        cleanly (no SystemExit, unlike the sync worker) and the job stays
        STARTED until StartedJobRegistry cleanup eventually fails it as
        abandoned."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)

        kill_process = Process(target=kill_worker, args=(os.getpid(), True, 0.5))
        kill_process.start()
        started_at = time.monotonic()
        with patch.object(AsyncWorker, 'simulated_job_duration', 20):
            worker.work()
        kill_process.join()

        self.assertLess(time.monotonic() - started_at, 10)
        self.assertEqual(job.get_status(), JobStatus.STARTED)

    def test_idle_shutdown_cancels_blocking_dequeue(self):
        """A signal during an idle blocking dequeue interrupts it immediately —
        the default block is ~worker_ttl seconds, so a fast return proves the
        BLMOVE await was cancelled rather than timing out."""
        worker = AsyncWorker([self.queue], connection=self.connection)

        kill_process = Process(target=kill_worker, args=(os.getpid(), False, 0.5))
        kill_process.start()
        started_at = time.monotonic()
        self.assertFalse(worker.work())
        kill_process.join()

        self.assertLess(time.monotonic() - started_at, 3)

    def test_admission_exception_drains_in_flight_execution(self):
        """A crash on the admission path (here: preparing the second job's
        execution) still drains in-flight executions before work() raises."""
        first_job = self.queue.enqueue(say_hello)
        self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)

        original_prepare_execution = AsyncWorker.prepare_execution

        def prepare_or_raise(worker_self, job):
            if job.id == first_job.id:
                return original_prepare_execution(worker_self, job)
            raise ValueError('boom')

        with (
            patch.object(AsyncWorker, 'simulated_job_duration', 1),
            patch.object(AsyncWorker, 'prepare_execution', prepare_or_raise),
        ):
            with self.assertRaises(ValueError):
                worker.work(burst=True)

        self.assertEqual(first_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(worker.executions, {})

    def test_missing_job_is_skipped(self):
        """A dequeued id whose job hash no longer exists is skipped; the next
        job still runs."""
        missing_job = self.queue.enqueue(say_hello)
        surviving_job = self.queue.enqueue(say_hello)
        self.connection.delete(missing_job.key)

        worker = AsyncWorker([self.queue], connection=self.connection)
        self.assertTrue(worker.work(burst=True))
        self.assertEqual(surviving_job.get_status(), JobStatus.FINISHED)

    def test_old_redis_server_raises(self):
        with patch('rq.worker.async_worker.get_version', return_value=(6, 0, 0)):
            with self.assertRaises(RuntimeError):
                AsyncWorker([self.queue], connection=self.connection)

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
