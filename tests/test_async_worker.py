import asyncio
import os
import sys
import threading
import time
import zlib
from collections.abc import Callable
from datetime import timedelta
from multiprocessing import Process
from unittest import skipIf
from unittest.mock import patch

from rq.command import send_stop_execution_command, send_stop_job_command
from rq.job import Callback, Job, JobStatus, Retry
from rq.queue import Queue
from rq.results import Result
from rq.utils import current_timestamp, now
from rq.webhook import Webhook
from rq.worker import AsyncWorker, DequeueStrategy, WorkerStatus
from tests import RQTestCase, min_redis_version
from tests.fixtures import (
    AsyncCallableObject,
    CallableObject,
    async_failure_callback,
    async_success_callback,
    concurrency,
    current_job_id_after_sleep,
    fail_async_while_retries_remain,
    kill_worker,
    raise_async,
    raise_on_cancel_async,
    raise_timeout_async,
    return_on_cancel_async,
    return_retry_async,
    save_exception,
    save_result,
    save_result_if_not_stopped,
    say_hello,
    say_hello_async,
    sleep_async,
    slow_success_callback,
    track_concurrency,
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

        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=2)
        admitted_any = worker.work(burst=True)

        self.assertTrue(admitted_any)
        self.assertEqual(first_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(second_job.get_status(), JobStatus.FINISHED)
        self.assertIn(first_job, self.queue.finished_job_registry)
        self.assertIn(second_job, self.queue.finished_job_registry)
        self.assertEqual(first_job.latest_result().return_value, 'Hi there, Alice!')
        self.assertEqual(second_job.latest_result().return_value, 'Hi there, Bob!')
        self.assertEqual(worker.executions, {})

    def test_max_concurrency(self):
        """Executions overlap up to, but never beyond, max_concurrency."""
        with self.subTest(max_concurrency=2):
            concurrency.update(active=0, peak=0)
            self.queue.enqueue(track_concurrency, 0.5)
            self.queue.enqueue(track_concurrency, 0.5)
            self.queue.enqueue(track_concurrency, 0.5)

            AsyncWorker([self.queue], connection=self.connection, max_concurrency=2).work(burst=True)

            self.assertEqual(concurrency['peak'], 2)

        with self.subTest(max_concurrency=1):
            concurrency.update(active=0, peak=0)
            self.queue.enqueue(track_concurrency, 0.5)
            self.queue.enqueue(track_concurrency, 0.5)
            self.queue.enqueue(track_concurrency, 0.5)

            AsyncWorker([self.queue], connection=self.connection, max_concurrency=1).work(burst=True)

            self.assertEqual(concurrency['peak'], 1)

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

    def test_undeserializable_job_fails(self):
        job = self.queue.enqueue(say_hello_async, on_failure=Callback(save_exception))
        corrupt_data = job.data.replace(b'say_hello_async', b'')
        self.connection.hset(job.key, 'data', zlib.compress(corrupt_data))

        AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn(job, self.queue.failed_job_registry)
        self.assertIn('DeserializationError', job.latest_result().exc_string)
        self.assertIsNotNone(self.connection.get(f'failure_callback:{job.id}'))

    def test_callable_object_jobs(self):
        async_callable_job = self.queue.enqueue(AsyncCallableObject())
        sync_callable_job = self.queue.enqueue(CallableObject())

        AsyncWorker([self.queue], connection=self.connection, max_concurrency=2).work(burst=True)

        self.assertEqual(async_callable_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(async_callable_job.latest_result().return_value, 'called asynchronously')
        self.assertEqual(sync_callable_job.get_status(), JobStatus.FAILED)
        self.assertIn('Job function must be async', sync_callable_job.latest_result().exc_string)

    def test_finalization_hooks(self):
        """Success and failure finalization run the job's callbacks and webhooks."""
        webhook = Webhook('http://example.com/done', JobStatus.FINISHED)
        success_job = self.queue.enqueue(say_hello_async, on_success=Callback(save_result), webhooks=[webhook])
        failure_job = self.queue.enqueue(raise_async, on_failure=Callback(save_exception))

        with patch.object(Webhook, 'send') as send:
            AsyncWorker([self.queue], connection=self.connection).work(burst=True)

        with self.subTest('success callback'):
            self.assertEqual(self.connection.get(f'success_callback:{success_job.id}'), b'Hi there, Stranger!')
        with self.subTest('failure callback'):
            self.assertEqual(self.connection.get(f'failure_callback:{failure_job.id}'), b'async failure')
        with self.subTest('finished webhook'):
            send.assert_called_once()

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
        self.assertIn('Coroutine success callbacks are not supported', success_job.latest_result().exc_string)
        self.assertEqual(failure_job.get_status(), JobStatus.FAILED)
        self.assertIn('Coroutine failure callbacks are not supported', failure_job.latest_result().exc_string)

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
        sibling_job = self.queue.enqueue(sleep_async, 0.5)
        retry_job = self.queue.enqueue(return_retry_async, 0.5)
        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=3)

        worker.work(burst=True)

        retry_job.refresh()
        sibling_job.refresh()
        self.assertEqual(retry_job.get_status(refresh=False), JobStatus.FAILED)
        self.assertLess(retry_job.started_at, sibling_job.ended_at)

    def test_burst_repops_after_task_finishing_during_empty_pop(self):
        """A task finishing while an empty dequeue is pending may have enqueued a
        retry, so burst mode must dequeue again instead of quitting"""
        job = self.queue.enqueue(fail_async_while_retries_remain, retry=Retry(max=1))
        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=2)
        gate = asyncio.Event()
        execution_tasks = []
        run_execution = worker._run_execution
        pop_job_id = worker._pop_job_id

        async def gated_run_execution(job, queue, execution):
            execution_tasks.append(asyncio.current_task())
            await gate.wait()
            await run_execution(job, queue, execution)

        async def stale_pop_job_id(burst, async_connection):
            job_id = await pop_job_id(burst, async_connection)
            if job_id is None and not gate.is_set():
                # Queue observed empty: let the first execution finish (enqueueing
                # its retry) before handing the loop that stale result
                gate.set()
                await execution_tasks[0]
            return job_id

        with (
            patch.object(worker, '_run_execution', gated_run_execution),
            patch.object(worker, '_pop_job_id', stale_pop_job_id),
        ):
            worker.work(burst=True)

        job.refresh()
        self.assertEqual(job.get_status(refresh=False), JobStatus.FINISHED)
        self.assertEqual(job.retries_left, 0)

    def test_heartbeat_tick_maintains_executions(self):
        """_heartbeat_tick refreshes the execution TTL and StartedJobRegistry
        score before running maintenance, so an execution outliving its initial
        TTL isn't reaped as abandoned while still running."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()
        execution = worker.prepare_execution(job)
        worker.prepare_job_execution(job)
        registry = self.queue.started_job_registry

        # Simulate staleness: execution key about to expire, registry score in the past.
        self.connection.expire(execution.key, 1)
        self.connection.zadd(registry.key, {execution.composite_key: 1})

        def assert_heartbeat_was_written():
            self.assertGreater(self.connection.ttl(execution.key), 30)
            self.assertGreater(self.connection.zscore(registry.key, execution.composite_key), current_timestamp())

        with patch.object(worker, 'run_maintenance_tasks', side_effect=assert_heartbeat_was_written) as maintenance:
            worker._heartbeat_tick()

        maintenance.assert_called_once_with()
        worker.register_death()

    def test_admission_clears_previous_attempt_started_at(self):
        """Admission clears a retried job's started_at before heartbeats can see it"""
        job = self.queue.enqueue(say_hello_async, job_timeout=10)
        job.started_at = now() - timedelta(minutes=5)
        job.save()
        worker = AsyncWorker([self.queue], connection=self.connection)
        started_at_on_admission = []

        def record_started_at(job):
            started_at_on_admission.append(job.started_at)
            return AsyncWorker.prepare_execution(worker, job)

        with patch.object(worker, 'prepare_execution', side_effect=record_started_at):
            worker.work(burst=True)

        self.assertEqual(started_at_on_admission, [None])
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

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

        with patch.object(worker, '_heartbeat_executions', wraps=worker._heartbeat_executions) as heartbeat_batch:
            worker._heartbeat_tick()

        self.assertEqual([len(call.args[0]) for call in heartbeat_batch.call_args_list], [2, 2, 1])
        worker.register_death()

    def test_heartbeat_tick_restores_worker_hash(self):
        """A tick that recreates a deleted worker hash writes its full state back."""
        job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()
        worker.prepare_execution(job)
        worker.prepare_job_execution(job)
        worker.last_cleaned_at = now()
        self.connection.delete(worker.key)

        worker._heartbeat_tick()

        self.assertEqual(self.connection.hget(worker.key, 'queues'), self.queue.name.encode())
        worker.register_death()

    def test_heartbeat_tick_removes_recreated_keys(self):
        """A tick racing finalization must not resurrect deleted job or execution hashes."""
        first_job = self.queue.enqueue(say_hello)
        second_job = self.queue.enqueue(say_hello)
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()
        first_execution = worker.prepare_execution(first_job)
        second_execution = worker.prepare_execution(second_job)
        worker.prepare_job_execution(first_job)
        worker.prepare_job_execution(second_job)
        worker.last_cleaned_at = now()
        with self.connection.pipeline() as cleanup_pipeline:
            first_execution.delete(job=first_job, pipeline=cleanup_pipeline)
            second_execution.delete(job=second_job, pipeline=cleanup_pipeline)
            cleanup_pipeline.execute()
        self.connection.delete(first_job.key, second_job.key)

        worker._heartbeat_tick()

        self.assertFalse(self.connection.exists(first_job.key))
        self.assertFalse(self.connection.exists(second_job.key))
        self.assertFalse(self.connection.exists(first_execution.key))
        self.assertFalse(self.connection.exists(second_execution.key))
        worker.register_death()

    def test_current_job_is_local_to_each_coroutine(self):
        first_job = self.queue.enqueue(current_job_id_after_sleep, 0.1)
        second_job = self.queue.enqueue(current_job_id_after_sleep, 0.1)

        AsyncWorker([self.queue], connection=self.connection, max_concurrency=2).work(burst=True)

        self.assertEqual(first_job.latest_result().return_value, first_job.id)
        self.assertEqual(second_job.latest_result().return_value, second_job.id)

    def _work_while_executing(self, worker: AsyncWorker, func: Callable[[], None]) -> float:
        """Runs `worker.work(burst=True)` while `func` runs on a thread, re-raising
        the thread's error. Returns the elapsed seconds."""
        thread_errors = []

        def run_func():
            try:
                func()
            except Exception as error:
                thread_errors.append(error)

        func_thread = threading.Thread(target=run_func)
        func_thread.start()
        started_at = time.monotonic()
        try:
            worker.work(burst=True)
        finally:
            func_thread.join()
        if thread_errors:
            raise thread_errors[0]
        return time.monotonic() - started_at

    def _wait_until_started(self, job: Job, timeout: float = 5) -> None:
        deadline = time.monotonic() + timeout
        while job.get_status() != JobStatus.STARTED:
            if time.monotonic() > deadline:
                raise TimeoutError(f'job {job.id} did not start within {timeout}s')
            time.sleep(0.05)

    def _wait_until_entered(self, job: Job, timeout: float = 5) -> None:
        """Waits for a cancellation fixture to set its entry marker, proving the
        coroutine is inside its cancellation handler's `try` block."""
        deadline = time.monotonic() + timeout
        while not self.connection.exists(f'entered:{job.id}'):
            if time.monotonic() > deadline:
                raise TimeoutError(f'job {job.id} did not enter within {timeout}s')
            time.sleep(0.05)

    def test_stop_execution_command_stops_only_targeted_job(self):
        """stop-execution cancels one coroutine, runs its stopped callback and
        persists STOPPED, while a sibling execution finishes normally."""
        stopped_job = self.queue.enqueue(sleep_async, 5, on_stopped=Callback(save_result_if_not_stopped))
        sibling_job = self.queue.enqueue(sleep_async, 1)
        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=2)

        def send_stop():
            self._wait_until_started(stopped_job)
            execution = stopped_job.get_executions()[0]
            send_stop_execution_command(self.connection, stopped_job.id, execution.id)

        elapsed = self._work_while_executing(worker, send_stop)

        self.assertLess(elapsed, 3)  # the 5s sleep was interrupted
        self.assertEqual(stopped_job.get_status(), JobStatus.STOPPED)
        self.assertIn(stopped_job, self.queue.failed_job_registry)
        self.assertTrue(self.connection.exists(f'stopped_callback:{stopped_job.id}'))
        self.assertEqual(sibling_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(worker.executions, {})
        self.assertEqual(worker._stopped_execution_ids, set())

    def test_stop_finalizes_as_stopped_however_job_reacts(self):
        """A stopped job is finalized as stopped whether it swallows the
        cancellation or raises from its cancellation handler."""
        swallowing_job = self.queue.enqueue(
            return_on_cancel_async,
            5,
            on_stopped=Callback(save_result_if_not_stopped),
            on_failure=Callback(save_exception),
        )
        raising_job = self.queue.enqueue(
            raise_on_cancel_async,
            5,
            on_stopped=Callback(save_result_if_not_stopped),
            on_failure=Callback(save_exception),
        )
        worker = AsyncWorker([self.queue], connection=self.connection, max_concurrency=2)

        def send_stops():
            self._wait_until_entered(swallowing_job)
            self._wait_until_entered(raising_job)
            send_stop_job_command(self.connection, swallowing_job.id)
            send_stop_job_command(self.connection, raising_job.id)

        self._work_while_executing(worker, send_stops)

        with self.subTest('swallows cancellation'):
            self.assertEqual(swallowing_job.get_status(), JobStatus.STOPPED)
            self.assertTrue(self.connection.exists(f'stopped_callback:{swallowing_job.id}'))
            self.assertFalse(self.connection.exists(f'failure_callback:{swallowing_job.id}'))
        with self.subTest('raises during cancellation'):
            self.assertEqual(raising_job.get_status(), JobStatus.STOPPED)
            self.assertTrue(self.connection.exists(f'stopped_callback:{raising_job.id}'))
            self.assertFalse(self.connection.exists(f'failure_callback:{raising_job.id}'))

    def test_hydration_via_all_and_find_by_key(self):
        """Worker discovery constructs AsyncWorker with no queues."""
        worker = AsyncWorker([self.queue], connection=self.connection)
        worker.register_birth()

        hydrated_workers = AsyncWorker.all(connection=self.connection)
        self.assertEqual([hydrated_worker.name for hydrated_worker in hydrated_workers], [worker.name])
        self.assertIsInstance(AsyncWorker.find_by_key(worker.key, connection=self.connection), AsyncWorker)
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

    def test_init_validation(self):
        with self.subTest('old Redis server'):
            with patch('rq.worker.async_worker.get_version', return_value=(6, 0, 0)):
                with self.assertRaises(RuntimeError):
                    AsyncWorker([self.queue], connection=self.connection)
        with self.subTest('old Python'):
            with patch('rq.worker.async_worker.sys.version_info', (3, 10)):
                with self.assertRaisesRegex(RuntimeError, 'Python >= 3.11'):
                    AsyncWorker([self.queue], connection=self.connection)
        with self.subTest('multiple queues'):
            other_queue = Queue('other', connection=self.connection)
            with self.assertRaises(ValueError):
                AsyncWorker([self.queue, other_queue], connection=self.connection)
        with self.subTest('zero max_concurrency'):
            with self.assertRaises(ValueError):
                AsyncWorker([self.queue], connection=self.connection, max_concurrency=0)

    def test_unsupported_work_options_raise(self):
        worker = AsyncWorker([self.queue], connection=self.connection)
        with self.subTest('with_scheduler'):
            with self.assertRaises(NotImplementedError):
                worker.work(burst=True, with_scheduler=True)
        with self.subTest('max_idle_time'):
            with self.assertRaises(NotImplementedError):
                worker.work(burst=True, max_idle_time=1)
        with self.subTest('dequeue_strategy'):
            with self.assertRaises(NotImplementedError):
                worker.work(burst=True, dequeue_strategy=DequeueStrategy.ROUND_ROBIN)
