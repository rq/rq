from __future__ import annotations

import asyncio
import signal
import sys

from ..defaults import DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT
from ..exceptions import DequeueTimeout
from ..executions import Execution
from ..job import Job, JobStatus
from ..job_lifecycle import format_exc_info
from ..queue import Queue
from ..utils import now
from .base import BaseWorker, DequeueStrategy, WorkerStatus


class AsyncWorker(BaseWorker):
    """Proof-of-concept worker that admits jobs onto an asyncio event loop.

    Jobs are not actually performed: each admitted job "runs" as an
    `asyncio.sleep` of `simulated_job_duration` seconds and then finalizes
    through the normal success handlers.

    Shutdown: the first SIGINT/SIGTERM stops admission and drains in-flight
    executions; a second signal cancels them. Unlike the sync `Worker`, cold
    shutdown returns cleanly from `work()` instead of raising `SystemExit`,
    and cold-cancelled executions are not finalized — their jobs stay STARTED
    in `StartedJobRegistry`. An idle worker reacts to a shutdown signal only
    after its current blocking dequeue returns, up to `blocking_dequeue_timeout`
    seconds: the dequeue runs as a sync Redis command on an executor thread,
    which the event loop cannot cancel. A future release will fix this by
    dequeuing natively with `redis.asyncio`, awaiting `BLMOVE` on the loop so
    a shutdown cancels it immediately.
    """

    simulated_job_duration: float = 5  # ponytail: stands in for performing the job, Stage 3 replaces
    blocking_dequeue_timeout: int = 5  # seconds a non-burst dequeue blocks; bounds shutdown latency

    def __init__(self, *args, max_concurrency: int = 100, **kwargs):
        super().__init__(*args, **kwargs)
        # Hydration (find_by_key/all) passes prepare_for_work=False and no queues.
        if kwargs.get('prepare_for_work', True) and len(self.queues) != 1:
            raise ValueError('AsyncWorker only supports a single queue')
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
        empty, in burst mode). Returns whether any job was processed.
        """
        if with_scheduler:
            raise NotImplementedError('AsyncWorker does not support with_scheduler')
        if max_idle_time is not None:
            raise NotImplementedError('AsyncWorker does not support max_idle_time')
        if dequeue_strategy != DequeueStrategy.DEFAULT:
            raise NotImplementedError('AsyncWorker only supports the default dequeue strategy')

        self.bootstrap(logging_level, date_format, log_format)
        try:
            processed_any = asyncio.run(self._admission_loop(burst, max_jobs))
        finally:
            self.teardown()
        return processed_any

    async def _admission_loop(self, burst: bool, max_jobs: int | None) -> bool:
        """Dequeues jobs and admits each as a task on the event loop, never
        holding more than `max_concurrency` executions in flight.
        """
        semaphore = asyncio.Semaphore(self.max_concurrency)
        running_tasks: set[asyncio.Task] = set()
        shutdown_event = asyncio.Event()
        admitted_jobs = 0

        def on_task_done(task: asyncio.Task):
            semaphore.release()
            running_tasks.discard(task)
            if not task.cancelled() and task.exception():
                self.log.error('Worker %s: execution task failed', self.name, exc_info=task.exception())

        def request_shutdown():
            if shutdown_event.is_set():
                self.log.warning(
                    'Worker %s: cold shutdown, cancelling %d in-flight executions', self.name, len(running_tasks)
                )
                # Cancellation only interrupts the simulated asyncio.sleep; a task
                # parked on to_thread() keeps its thread running to completion.
                for task in tuple(running_tasks):  # snapshot: guard against mutation during iteration
                    task.cancel()
            else:
                self.handle_warm_shutdown_request()
                self.log.info('Worker %s: send signal again to force quit', self.name)
                shutdown_event.set()

        loop = asyncio.get_running_loop()
        installed_signals = []
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(shutdown_signal, request_shutdown)
                installed_signals.append(shutdown_signal)
            except (ValueError, RuntimeError):
                break  # work() off the main thread: no handlers, signals keep default behavior

        try:
            while not shutdown_event.is_set():
                await semaphore.acquire()
                if shutdown_event.is_set():
                    semaphore.release()
                    break
                result = await asyncio.to_thread(self._admission_tick, burst)
                if result is None:
                    semaphore.release()
                    if burst:
                        self.log.info('Worker %s: done, quitting', self.name)
                        break
                    continue

                # Admit even if shutdown was requested mid-dequeue: the job is
                # already popped, dropping it here would lose it.
                job, queue = result
                execution = await asyncio.to_thread(self.prepare_execution, job)
                task = asyncio.create_task(self._run_execution(job, queue, execution))
                running_tasks.add(task)
                task.add_done_callback(on_task_done)
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

            # Drain in-flight executions (or collect their cancellations on cold
            # shutdown); failures are already logged by on_task_done.
            if running_tasks:
                self.log.info('Worker %s: waiting for %d in-flight executions', self.name, len(running_tasks))
                await asyncio.gather(*running_tasks, return_exceptions=True)
        finally:
            for installed_signal in installed_signals:
                loop.remove_signal_handler(installed_signal)
        return admitted_jobs > 0

    def _admission_tick(self, burst: bool) -> tuple[Job, Queue] | None:
        """One synchronous admission step, run off the event loop: heartbeat,
        then dequeue with a short block so the heartbeat runs every tick.
        """
        if not self.executions:
            self.set_state(WorkerStatus.IDLE)
        self.heartbeat()
        timeout = None if burst else self.blocking_dequeue_timeout
        try:
            result = self.queue_class.dequeue_any(
                self.queues,
                timeout,
                connection=self.connection,
                job_class=self.job_class,
                serializer=self.serializer,
                death_penalty_class=self.death_penalty_class,
            )
        except DequeueTimeout:
            return None
        if result:
            job, queue = result
            self.log.debug('Worker %s: dequeued job %s from %s', self.name, job.id, queue.name)
        return result

    async def _run_execution(self, job: Job, queue: Queue, execution: Execution):
        try:
            await asyncio.to_thread(self._start_job, job)
            await asyncio.sleep(self.simulated_job_duration)
            await asyncio.to_thread(self._finish_job_without_performing, job, queue, execution)
        except Exception:
            exc_string = format_exc_info(sys.exc_info())
            await asyncio.to_thread(
                self.handle_job_failure,
                job=job,
                queue=queue,
                started_job_registry=queue.started_job_registry,
                exc_string=exc_string,
                execution=execution,
            )

    def _start_job(self, job: Job):
        self.prepare_job_execution(job, remove_from_intermediate_queue=True)
        job.started_at = now()

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
