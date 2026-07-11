import os
import time
from multiprocessing import Process
from unittest.mock import patch

from rq.command import handle_stop_job_command
from rq.job import JobStatus
from rq.queue import Queue
from rq.utils import current_timestamp
from rq.worker import AsyncWorker, WorkerStatus
from tests import RQTestCase, min_redis_version
from tests.fixtures import (
    current_job_id_after_sleep,
    kill_worker,
    raise_async,
    say_hello,
    say_hello_async,
    sleep_async,
)


@min_redis_version((6, 2, 0))
class TestAsyncWorker(RQTestCase):
    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)

    def test_burst_performs_jobs_and_persists_results(self):
        first_job = self.queue.enqueue(say_hello_async, 'Alice')
        second_job = self.queue.enqueue(say_hello_async, 'Bob')
        sync_job = self.queue.enqueue(say_hello)

        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=2)
        processed = worker.work(burst=True)

        self.assertTrue(processed)
        self.assertEqual(first_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(second_job.get_status(), JobStatus.FINISHED)
        self.assertIn(first_job, self.queue.finished_job_registry)
        self.assertIn(second_job, self.queue.finished_job_registry)
        self.assertEqual(first_job.latest_result().return_value, 'Hi there, Alice!')
        self.assertEqual(second_job.latest_result().return_value, 'Hi there, Bob!')
        self.assertEqual(sync_job.get_status(), JobStatus.FAILED)
        self.assertIn(sync_job, self.queue.failed_job_registry)
        self.assertEqual(worker.executions, {})
        self.assertIsNotNone(worker.death_date)

    def test_concurrent_executions_overlap(self):
        self.queue.enqueue(sleep_async, 0.5)
        self.queue.enqueue(sleep_async, 0.5)
        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=2)

        started_at = time.monotonic()
        worker.work(burst=True)
        self.assertLess(time.monotonic() - started_at, 0.95)

    def test_max_concurrency_one_runs_jobs_sequentially(self):
        self.queue.enqueue(sleep_async, 0.5)
        self.queue.enqueue(sleep_async, 0.5)
        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=1)

        started_at = time.monotonic()
        worker.work(burst=True)
        self.assertGreaterEqual(time.monotonic() - started_at, 1.0)

    def test_empty_queue_returns_false(self):
        worker = AsyncWorker([self.queue], connection=self.connection)
        self.assertFalse(worker.work(burst=True))

    def test_failed_finalization_does_not_leak_executions(self):
        """A crash while finalizing routes through handle_job_failure and
        leaves no stale entry in worker.executions."""
        job = self.queue.enqueue(say_hello_async)
        worker = AsyncWorker([self.queue], connection=self.connection)

        with patch.object(AsyncWorker, 'handle_job_success', side_effect=ValueError):
            worker.work(burst=True)

        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn(job, self.queue.failed_job_registry)
        self.assertEqual(worker.executions, {})

    def test_failed_job_records_ended_at_and_working_time(self):
        """The failure path sets job.ended_at before finalizing, so the
        failure result gets an end timestamp and working time is counted."""
        job = self.queue.enqueue(raise_async)
        worker = AsyncWorker([self.queue], connection=self.connection)

        worker.work(burst=True)

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIsNotNone(job.ended_at)
        worker.refresh()
        self.assertGreater(worker.total_working_time, 0)

    def test_heartbeat_tick_maintains_executions(self):
        """_heartbeat_tick refreshes the execution TTL and StartedJobRegistry
        score, so an execution outliving its initial TTL isn't reaped as
        abandoned while still running."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()
        execution = worker.prepare_execution(job)
        registry = self.queue.started_job_registry

        # Simulate staleness: execution key about to expire, registry score in the past.
        self.connection.expire(execution.key, 1)
        self.connection.zadd(registry.key, {execution.composite_key: 1})

        worker._heartbeat_tick()

        self.assertGreater(self.connection.ttl(execution.key), 30)
        self.assertGreater(self.connection.zscore(registry.key, execution.composite_key), current_timestamp())
        worker.register_death()

    def test_heartbeat_tick_converges_worker_state(self):
        """Every tick writes the truthful state, healing a stale write from a
        tick/admission interleave."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()

        execution = worker.prepare_execution(job)
        worker.set_state(WorkerStatus.IDLE)
        worker._heartbeat_tick()
        self.assertEqual(worker.get_state(), WorkerStatus.BUSY)

        with self.connection.pipeline() as pipeline:
            worker.cleanup_execution(job, pipeline=pipeline, execution=execution)
            pipeline.execute()
        worker.set_state(WorkerStatus.BUSY)
        worker._heartbeat_tick()
        self.assertEqual(worker.get_state(), WorkerStatus.IDLE)
        worker.register_death()

    def test_current_job_is_local_to_each_coroutine(self):
        first_job = self.queue.enqueue(current_job_id_after_sleep, 0.1)
        second_job = self.queue.enqueue(current_job_id_after_sleep, 0.1)

        AsyncWorker([self.queue], connection=self.connection, max_concurrency=2).work(burst=True)

        self.assertEqual(first_job.latest_result().return_value, first_job.id)
        self.assertEqual(second_job.latest_result().return_value, second_job.id)

    def test_stop_job_command_does_not_raise(self):
        """A raw stop-job payload resolves against worker.executions, so it
        can't kill the pub/sub thread on a multi-execution worker."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        execution = worker.prepare_execution(job)
        try:
            handle_stop_job_command(worker, {'command': 'stop-job', 'job_id': job.id})
            self.assertEqual(worker._stopped_job_id, job.id)
        finally:
            with self.connection.pipeline() as pipeline:
                worker.cleanup_execution(job, pipeline=pipeline, execution=execution)
                pipeline.execute()

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
        job = self.queue.enqueue(sleep_async, 2)
        worker = AsyncWorker([self.queue], connection=self.connection)

        kill_process = Process(target=kill_worker, args=(os.getpid(), False, 0.5))
        kill_process.start()
        worker.work()
        kill_process.join()

        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(worker.executions, {})

    def test_cold_shutdown_cancels_in_flight_execution(self):
        """Second signal cancels the in-flight execution; work() returns
        cleanly (no SystemExit, unlike the sync worker) and the job stays
        STARTED until StartedJobRegistry cleanup eventually fails it as
        abandoned."""
        job = self.queue.enqueue(sleep_async, 20)
        worker = AsyncWorker([self.queue], connection=self.connection)

        kill_process = Process(target=kill_worker, args=(os.getpid(), True, 0.5))
        kill_process.start()
        started_at = time.monotonic()
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
        """An admission-loop error does not abandon an execution already in flight.

        The first job starts normally, then preparing the second execution raises.
        The admission loop must wait for the first task to finish and clean up its
        execution before propagating the error from worker.work().
        """
        first_job = self.queue.enqueue(sleep_async, 1)
        self.queue.enqueue(say_hello_async)
        worker = AsyncWorker([self.queue], connection=self.connection)

        original_prepare_execution = AsyncWorker.prepare_execution

        def prepare_or_raise(worker_self, job):
            if job.id == first_job.id:
                return original_prepare_execution(worker_self, job)
            # Simulate a failure after the first job has already been admitted.
            raise ValueError

        with patch.object(AsyncWorker, 'prepare_execution', prepare_or_raise):
            with self.assertRaises(ValueError):
                worker.work(burst=True)

        self.assertEqual(first_job.get_status(), JobStatus.FINISHED)
        # The admission loop's finally block drained and cleaned up the first task.
        self.assertEqual(worker.executions, {})

    def test_missing_job_is_skipped(self):
        """A dequeued id whose job hash no longer exists is skipped; the next
        job still runs."""
        missing_job = self.queue.enqueue(say_hello)
        surviving_job = self.queue.enqueue(say_hello_async)
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
