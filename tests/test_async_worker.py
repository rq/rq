import os
import sys
import time
import zlib
from multiprocessing import Process
from unittest import skipIf
from unittest.mock import ANY, patch

from rq.command import handle_stop_job_command
from rq.defaults import UNSERIALIZABLE_RETURN_VALUE_PAYLOAD
from rq.job import Callback, JobStatus, Retry
from rq.queue import Queue
from rq.results import Result
from rq.utils import current_timestamp, now
from rq.webhook import Webhook
from rq.worker import AsyncWorker, WorkerStatus
from tests import RQTestCase, min_redis_version
from tests.fixtures import (
    AsyncCallableObject,
    CallableObject,
    add_meta,
    async_failure_callback,
    async_success_callback,
    current_job_id_after_sleep,
    erroneous_callback,
    fail_async_while_retries_remain,
    kill_worker,
    raise_async,
    raise_timeout_async,
    return_retry_async,
    return_unserializable,
    save_exception,
    save_result,
    say_hello,
    say_hello_async,
    sleep_async,
    slow_success_callback,
)


@skipIf(sys.version_info < (3, 11), 'AsyncWorker requires Python >= 3.11')
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

    def test_undeserializable_job_fails(self):
        job = self.queue.enqueue(say_hello_async, on_failure=Callback(save_exception))
        corrupt_data = job.data.replace(b'say_hello_async', b'')
        self.connection.hset(job.key, 'data', zlib.compress(corrupt_data))

        AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn(job, self.queue.failed_job_registry)
        self.assertIn('DeserializationError', job.latest_result().exc_string)
        self.assertIsNotNone(self.connection.get(f'failure_callback:{job.id}'))

    def test_unserializable_return_value_finishes_with_placeholder(self):
        job = self.queue.enqueue(return_unserializable)

        AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(job.latest_result().return_value, UNSERIALIZABLE_RETURN_VALUE_PAYLOAD)

    def test_callable_object_jobs(self):
        async_callable_job = self.queue.enqueue(AsyncCallableObject())
        sync_callable_job = self.queue.enqueue(CallableObject())

        AsyncWorker([self.queue], connection=self.connection, max_concurrency=2).work(burst=True)

        self.assertEqual(async_callable_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(async_callable_job.latest_result().return_value, 'called asynchronously')
        self.assertEqual(sync_callable_job.get_status(), JobStatus.FAILED)
        self.assertIn('AsyncWorker only supports async functions', sync_callable_job.latest_result().exc_string)

    def test_success_callback(self):
        job = self.queue.enqueue(say_hello_async, on_success=Callback(save_result))

        AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        self.assertEqual(self.connection.get(f'success_callback:{job.id}'), b'Hi there, Stranger!')
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_failure_callback(self):
        job = self.queue.enqueue(raise_async, on_failure=Callback(save_exception))

        AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        self.assertEqual(self.connection.get(f'failure_callback:{job.id}'), b'async failure')
        self.assertEqual(job.get_status(), JobStatus.FAILED)

    def test_success_callback_error_runs_failure_callback(self):
        job = self.queue.enqueue(
            say_hello_async,
            on_success=Callback(erroneous_callback),
            on_failure=Callback(save_exception),
        )

        AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        callback_error = self.connection.get(f'failure_callback:{job.id}')
        self.assertIsNotNone(callback_error)
        self.assertIn(b'erroneous_callback', callback_error)
        self.assertEqual(job.get_status(), JobStatus.FAILED)

    def test_success_callback_timeout(self):
        job = self.queue.enqueue(
            say_hello_async,
            on_success=Callback(slow_success_callback, timeout=1),
        )

        AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn('JobTimeoutException', job.latest_result().exc_string)

    def test_coroutine_callbacks_fail_clearly(self):
        success_job = self.queue.enqueue(say_hello_async, on_success=Callback(async_success_callback))
        failure_job = self.queue.enqueue(raise_async, on_failure=Callback(async_failure_callback))

        AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        self.assertEqual(success_job.get_status(), JobStatus.FAILED)
        self.assertIn('does not support coroutine success callbacks', success_job.latest_result().exc_string)
        self.assertEqual(failure_job.get_status(), JobStatus.FAILED)
        self.assertIn('does not support coroutine failure callbacks', failure_job.latest_result().exc_string)

    def test_success_webhook(self):
        webhook = Webhook('http://example.com/done', JobStatus.FINISHED)
        self.queue.enqueue(say_hello_async, webhooks=[webhook])

        with patch.object(Webhook, 'send') as send:
            AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        send.assert_called_once()

    def test_custom_exception_handler(self):
        job = self.queue.enqueue(raise_async)

        AsyncWorker([self.queue], connection=self.connection, exception_handlers=[add_meta]).work(burst=True)

        job.refresh()
        self.assertTrue(job.meta['foo'])

    def test_timeout_fails_one_job_without_interrupting_sibling(self):
        """Only expiration by asyncio.timeout is converted to JobTimeoutException.

        A TimeoutError raised by job code is a regular job failure, while an
        unrelated sibling continues running in either case.
        """
        timed_out_job = self.queue.enqueue(sleep_async, 2, job_timeout=1)
        sibling_job = self.queue.enqueue(say_hello_async)
        timeout_error_job = self.queue.enqueue(raise_timeout_async)

        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=2)
        worker.work(burst=True)

        self.assertEqual(timed_out_job.get_status(), JobStatus.FAILED)
        self.assertIn('JobTimeoutException', timed_out_job.latest_result().exc_string)
        self.assertEqual(sibling_job.get_status(), JobStatus.FINISHED)
        # This job raised TimeoutError itself; the worker timeout did not expire.
        exc_string = timeout_error_job.latest_result().exc_string
        self.assertIn('TimeoutError: job timeout error', exc_string)
        self.assertNotIn('JobTimeoutException', exc_string)
        self.assertEqual(worker.executions, {})

    def test_exception_retry(self):
        job = self.queue.enqueue(fail_async_while_retries_remain, retry=Retry(max=1))
        worker = AsyncWorker([self.queue], connection=self.connection)

        worker.work(burst=True)

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(job.latest_result().return_value, 'succeeded after failure retry')
        self.assertEqual(job.retries_left, 0)
        # Exception retries do not create a RETRIED result; only the final execution is recorded.
        self.assertEqual([result.type for result in job.results()], [Result.Type.SUCCESSFUL])
        self.assertEqual(worker.executions, {})

    def test_returned_retry(self):
        job = self.queue.enqueue(
            return_retry_async,
            on_success=Callback(save_result),
            on_failure=Callback(save_exception),
        )
        worker = AsyncWorker([self.queue], connection=self.connection)

        worker.work(burst=True)

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertEqual(job.number_of_retries, 1)
        results = job.results()
        self.assertEqual(
            [result.type for result in results],
            [Result.Type.MAX_RETRIES_EXCEEDED, Result.Type.RETRIED],
        )
        self.assertNotEqual(results[0].execution_id, results[1].execution_id)
        self.assertIsNone(self.connection.get(f'success_callback:{job.id}'))
        self.assertIsNone(self.connection.get(f'failure_callback:{job.id}'))
        self.assertEqual(worker.executions, {})

    def test_burst_runs_retry_while_sibling_is_still_running(self):
        self.queue.enqueue(sleep_async, 0.5)
        retry_job = self.queue.enqueue(return_retry_async, 0.5)
        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=3)

        started_at = time.monotonic()
        worker.work(burst=True)

        self.assertLess(time.monotonic() - started_at, 0.95)
        self.assertEqual(retry_job.get_status(), JobStatus.FAILED)

    def test_heartbeat_tick_maintains_executions(self):
        """_heartbeat_tick refreshes the execution TTL and StartedJobRegistry
        score, so an execution outliving its initial TTL isn't reaped as
        abandoned while still running."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()
        execution = worker.prepare_execution(job)
        worker.prepare_job_execution(job)
        worker.last_cleaned_at = now()
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
        worker.last_cleaned_at = now()

        execution = worker.prepare_execution(job)
        worker.prepare_job_execution(job)
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

    def test_heartbeat_tick_batches_executions(self):
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.heartbeat_batch_size = 2
        worker.register_birth()
        for _ in range(5):
            worker.prepare_execution(job)
        worker.prepare_job_execution(job)
        worker.last_cleaned_at = now()

        with (
            patch.object(worker.connection, 'pipeline', wraps=worker.connection.pipeline) as pipeline,
            patch.object(worker, 'heartbeat', wraps=worker.heartbeat) as heartbeat,
            patch.object(worker, '_heartbeat_executions', wraps=worker._heartbeat_executions) as heartbeat_batch,
        ):
            worker._heartbeat_tick()

        self.assertEqual(pipeline.call_count, 4)
        heartbeat.assert_called_once_with(worker.job_monitoring_interval + 60, pipeline=ANY)
        self.assertEqual([len(call.args[0]) for call in heartbeat_batch.call_args_list], [2, 2, 1])
        self.assertEqual(len({call.args[1] for call in heartbeat_batch.call_args_list}), 1)
        worker.register_death()

    def test_heartbeat_tick_repairs_recreated_keys_once(self):
        first_job = self.queue.enqueue(say_hello)
        second_job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()
        worker.prepare_execution(first_job)
        worker.prepare_execution(second_job)
        worker.prepare_job_execution(first_job)
        worker.prepare_job_execution(second_job)
        worker.last_cleaned_at = now()
        self.connection.delete(first_job.key, second_job.key, worker.key)

        with patch.object(worker.connection, 'pipeline', wraps=worker.connection.pipeline) as pipeline:
            worker._heartbeat_tick()

        self.assertEqual(pipeline.call_count, 2)
        self.assertFalse(self.connection.exists(first_job.key))
        self.assertFalse(self.connection.exists(second_job.key))
        self.assertEqual(self.connection.hget(worker.key, 'queues'), self.queue.name.encode())
        worker.register_death()

    def test_heartbeat_tick_runs_maintenance_after_writes(self):
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()
        execution = worker.prepare_execution(job)
        worker.prepare_job_execution(job)
        self.connection.zadd(self.queue.started_job_registry.key, {execution.composite_key: 1})

        def assert_heartbeat_was_written():
            score = self.connection.zscore(self.queue.started_job_registry.key, execution.composite_key)
            self.assertGreater(score, current_timestamp())

        with patch.object(worker, 'run_maintenance_tasks', side_effect=assert_heartbeat_was_written) as maintenance:
            worker._heartbeat_tick()

        maintenance.assert_called_once_with()
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

    def test_old_python_raises_for_working_instance(self):
        with patch('rq.worker.async_worker.sys.version_info', (3, 10)):
            with self.assertRaisesRegex(RuntimeError, 'Python >= 3.11'):
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
