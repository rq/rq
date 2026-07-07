from __future__ import annotations

import asyncio
import signal
import sys
from typing import cast

import redis.asyncio

from ..connections import get_connection_kwargs
from ..defaults import DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT
from ..executions import Execution
from ..intermediate_queue import IntermediateQueue
from ..job import Job, JobStatus
from ..job_lifecycle import format_exc_info
from ..queue import Queue
from ..utils import as_text, get_version, now
from .base import BaseWorker, DequeueStrategy, WorkerStatus


class AsyncWorker(BaseWorker):
    """Proof-of-concept worker that admits jobs onto an asyncio event loop.

    Jobs are not actually performed: each admitted job "runs" as an
    `asyncio.sleep` of `simulated_job_duration` seconds and then finalizes
    through the normal success handlers.
    """

    simulated_job_duration: float = 5  # ponytail: stands in for performing the job, Stage 3 replaces

    def __init__(self, *args, max_concurrency: int = 100, **kwargs):
        super().__init__(*args, **kwargs)
        # Hydration (find_by_key/all) passes prepare_for_work=False and no queues.
        if kwargs.get('prepare_for_work', True):
            if len(self.queues) != 1:
                raise ValueError('AsyncWorker only supports a single queue')
            if get_version(self.connection) < (6, 2, 0):
                raise RuntimeError('AsyncWorker requires Redis server >= 6.2 (BLMOVE)')
        if max_concurrency < 1:
            raise ValueError('max_concurrency must be at least 1')
        self.max_concurrency = max_concurrency

    def work(
        self,
        burst: bool = False,
        logging_level: str | None = None,
        date_format: str = DEFAULT_LOGGING_DATE_FORMAT,
        log_format: str = DEFAULT_LOGGING_FORMAT,
        max_jobs: int | None = None,
        max_idle_time: int | None = None,
        with_scheduler: bool = False,
        dequeue_strategy: DequeueStrategy = DequeueStrategy.DEFAULT,
    ) -> bool:
        """Admits jobs onto the event loop until interrupted (or the queue is
        empty, in burst mode). Returns whether any job was admitted (a
        cold-cancelled job counts: it was admitted, not processed).

        Shutdown: the first SIGINT/SIGTERM stops admission and drains in-flight
        executions; a second signal cancels them. The blocking dequeue is a
        native `redis.asyncio` BLMOVE awaited on the event loop, so a shutdown
        signal interrupts an idle worker immediately. Unlike the sync `Worker`,
        cold shutdown returns cleanly instead of raising `SystemExit`, and
        cold-cancelled executions are not finalized — their jobs stay STARTED
        in `StartedJobRegistry`.
        """
        if with_scheduler:
            raise NotImplementedError('AsyncWorker does not support with_scheduler')
        if max_idle_time is not None:
            raise NotImplementedError('AsyncWorker does not support max_idle_time')
        if dequeue_strategy != DequeueStrategy.DEFAULT:
            raise NotImplementedError('AsyncWorker only supports the default dequeue strategy')

        self.bootstrap(logging_level, date_format, log_format)
        try:
            admitted_any = asyncio.run(self._admission_loop(burst, max_jobs))
        finally:
            self.teardown()
        return admitted_any

    async def _admission_loop(self, burst: bool, max_jobs: int | None) -> bool:
        """Dequeues jobs and admits each as a task on the event loop, never
        holding more than `max_concurrency` executions in flight.
        """
        semaphore = asyncio.Semaphore(self.max_concurrency)
        running_tasks: set[asyncio.Task] = set()
        shutdown_event = asyncio.Event()
        cold_shutdown_requested = False
        admitted_jobs = 0

        def on_task_done(task: asyncio.Task):
            semaphore.release()
            running_tasks.discard(task)
            if not task.cancelled() and task.exception():
                self.log.error('Worker %s: execution task failed', self.name, exc_info=task.exception())

        def request_shutdown():
            nonlocal cold_shutdown_requested
            if shutdown_event.is_set():
                self.log.warning(
                    'Worker %s: cold shutdown, cancelling %d in-flight executions', self.name, len(running_tasks)
                )
                cold_shutdown_requested = True
                # Cancellation only interrupts the simulated asyncio.sleep; a task
                # parked on to_thread() keeps its thread running to completion.
                for task in tuple(running_tasks):  # snapshot: guard against mutation during iteration
                    task.cancel()
            else:
                self.handle_warm_shutdown_request()
                self.log.info('Worker %s: send signal again to force quit', self.name)
                shutdown_event.set()

        loop = asyncio.get_running_loop()
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(shutdown_signal, request_shutdown)
            except (ValueError, RuntimeError, NotImplementedError):
                break  # work() off the main thread or unsupported platform: signals keep default behavior

        async_connection = redis.asyncio.Redis(**get_connection_kwargs(self.connection))
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        stop_wait_task = asyncio.create_task(shutdown_event.wait())
        try:
            while not shutdown_event.is_set():
                await semaphore.acquire()
                if shutdown_event.is_set():
                    semaphore.release()
                    break

                pop_task = asyncio.create_task(self._pop_job_id(burst, async_connection))
                await asyncio.wait({pop_task, stop_wait_task}, return_when=asyncio.FIRST_COMPLETED)
                if not pop_task.done():
                    # Shutdown while blocked on the dequeue: no job is held yet,
                    # cancelling is safe (see _pop_job_id for the residual race).
                    pop_task.cancel()
                    await asyncio.gather(pop_task, return_exceptions=True)
                    semaphore.release()
                    break

                job_id = pop_task.result()
                if job_id is None:
                    semaphore.release()
                    if burst:
                        self.log.info('Worker %s: done, quitting', self.name)
                        break
                    continue

                # The job is already in the intermediate queue: admit it even if
                # shutdown was requested mid-dequeue, dropping it here would
                # strand it.
                job_fetch_result = await asyncio.to_thread(
                    self.queue_class._fetch_dequeued_job,
                    self.connection,
                    self.queues[0].key,
                    job_id,
                    self.job_class,
                    self.serializer,
                    self.death_penalty_class,
                )
                if job_fetch_result is None:
                    semaphore.release()
                    continue  # job hash vanished, dequeue again

                job, queue = job_fetch_result
                self.log.debug('Worker %s: dequeued job %s from %s', self.name, job.id, queue.name)
                execution = await asyncio.to_thread(self.prepare_execution, job)
                task = asyncio.create_task(self._run_execution(job, queue, execution))
                running_tasks.add(task)
                task.add_done_callback(on_task_done)
                if cold_shutdown_requested:
                    # A second signal landed between the pop and this registration,
                    # so request_shutdown's cancel sweep missed this task.
                    task.cancel()
                self.log.debug(
                    'Worker %s: admitted job %s (execution %s), %d in flight',
                    self.name,
                    job.id,
                    execution.id,
                    len(self.executions),
                )

                admitted_jobs += 1
                if max_jobs is not None and admitted_jobs >= max_jobs:
                    self.log.info('Worker %s: admitted %d jobs, quitting', self.name, admitted_jobs)
                    break
        finally:
            # Drain in-flight executions first (also when an admission-path
            # exception lands here), or collect their cancellations on cold
            # shutdown; failures are already logged by on_task_done. The
            # heartbeat is cancelled only after the drain: a worker draining
            # long executions must keep heartbeating or it looks dead.
            if running_tasks:
                self.log.info('Worker %s: waiting for %d in-flight executions', self.name, len(running_tasks))
                await asyncio.gather(*running_tasks, return_exceptions=True)
            heartbeat_task.cancel()
            stop_wait_task.cancel()
            await asyncio.gather(heartbeat_task, stop_wait_task, return_exceptions=True)
            await async_connection.aclose()  # type: ignore[attr-defined]  # types-redis stubs predate aclose
        return admitted_jobs > 0

    async def _pop_job_id(self, burst: bool, async_connection: redis.asyncio.Redis) -> str | None:
        """Pops the next job id into the intermediate queue, blocking on the
        event loop (cancellable). Once this returns an id, the job is in the
        intermediate queue and must be admitted — never cancel past this point.

        If a cancel lands in the instant between the server completing the
        BLMOVE and the reply arriving, the job id parks in the intermediate
        queue, where maintenance cleanup eventually fails the job as stuck
        (it is not requeued).
        """
        queue_key = self.queues[0].key
        intermediate_key = IntermediateQueue(queue_key, self.connection).key
        while True:
            try:
                job_id = cast(
                    'bytes | str | None',
                    await (
                        async_connection.lmove(queue_key, intermediate_key)
                        if burst
                        else async_connection.blmove(queue_key, intermediate_key, self.dequeue_timeout)
                    ),
                )
            except asyncio.CancelledError:
                # A cancelled command may leave an unread reply on the socket.
                await async_connection.connection_pool.disconnect()
                raise
            if job_id is not None:
                return as_text(job_id)
            if burst:
                return None  # queue empty
            # Timeout: re-block. Heartbeats run on their own task and a
            # shutdown signal cancels this await directly.

    async def _heartbeat_loop(self):
        """Periodically heartbeats the worker and keeps its state accurate."""
        while True:
            try:
                await asyncio.to_thread(self._heartbeat_tick)
            except Exception:
                self.log.exception('Worker %s: heartbeat failed', self.name)
            await asyncio.sleep(self.job_monitoring_interval)

    def _heartbeat_tick(self):
        """Heartbeats the worker and every in-flight execution: without the
        `maintain_heartbeats` refresh, an execution outliving its initial TTL
        (~job_monitoring_interval + 60) would expire out of StartedJobRegistry
        and be failed as abandoned while still running.
        """
        if not self.executions:
            self.set_state(WorkerStatus.IDLE)
            self.heartbeat()
            return
        # TODO: batch this into a bulk heartbeat (one pipeline for all executions)
        # instead of one maintain_heartbeats() round trip per execution.
        for execution in list(self.executions.values()):  # handler threads pop entries mid-pass
            self.maintain_heartbeats(execution.job, execution)

    async def _run_execution(self, job: Job, queue: Queue, execution: Execution):
        try:
            await asyncio.to_thread(self._start_job, job)
            await asyncio.sleep(self.simulated_job_duration)
            await asyncio.to_thread(self._finish_job_without_performing, job, queue, execution)
        except Exception:
            exc_string = format_exc_info(sys.exc_info())
            await asyncio.to_thread(self._fail_job, job, queue, execution, exc_string)

    def _start_job(self, job: Job):
        self.prepare_job_execution(job, remove_from_intermediate_queue=True)
        job.started_at = now()

    def _fail_job(self, job: Job, queue: Queue, execution: Execution, exc_string: str):
        """The failure half of `perform_job`, minus callbacks and exception
        handlers (POC): sets `ended_at` before finalizing so the failure
        result and working-time accounting see a real end timestamp.
        """
        self.handle_execution_ended(job, queue, job.failure_callback_timeout)
        self.handle_job_failure(
            job=job,
            queue=queue,
            started_job_registry=queue.started_job_registry,
            exc_string=exc_string,
            execution=execution,
        )

    def _finish_job_without_performing(self, job: Job, queue: Queue, execution: Execution):
        """The success half of `perform_job`, minus performing the job (and
        minus callbacks and webhooks, which the POC skips).
        """
        self.handle_execution_ended(job, queue, job.success_callback_timeout)
        job._result = None
        job._status = JobStatus.FINISHED
        self.handle_job_success(
            job=job, queue=queue, started_job_registry=queue.started_job_registry, execution=execution
        )
