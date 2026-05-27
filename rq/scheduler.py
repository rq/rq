from __future__ import annotations

import logging
import os
import signal
import time
import traceback
from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from multiprocessing import Process, get_context
from multiprocessing.process import BaseProcess
from secrets import token_hex

from redis import ConnectionPool, Redis

from .connections import parse_connection
from .defaults import DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT, DEFAULT_SCHEDULER_FALLBACK_PERIOD
from .job import Job
from .logutils import setup_loghandlers
from .queue import Queue
from .registry import ScheduledJobRegistry
from .scripts import delete_scheduler_locks
from .serializers import resolve_serializer
from .utils import current_timestamp, parse_names, split_list

ForkProcess: type[BaseProcess]
try:
    ForkProcess = get_context('fork').Process
except ValueError:
    ForkProcess = Process

SCHEDULER_KEY_TEMPLATE = 'rq:scheduler:%s'
SCHEDULER_LOCKING_KEY_TEMPLATE = 'rq:scheduler-lock:%s'


class SchedulerStatus(str, Enum):
    STARTED = 'started'
    WORKING = 'working'
    STOPPED = 'stopped'


class RQScheduler:
    # STARTED: scheduler has been started but sleeping
    # WORKING: scheduler is in the midst of scheduling jobs
    # STOPPED: scheduler is in stopped condition

    Status = SchedulerStatus

    def __init__(
        self,
        queues,
        connection: Redis,
        burst: bool = False,
        interval=1,
        logging_level: str | int = logging.INFO,
        date_format=DEFAULT_LOGGING_DATE_FORMAT,
        log_format=DEFAULT_LOGGING_FORMAT,
        serializer=None,
    ):
        self._ppid = os.getpid()
        self._pid: int | None = None
        self._queue_names = set(parse_names(queues))
        self._acquired_locks: set[str] = set()
        self._token = token_hex()
        self._scheduled_job_registries: list[ScheduledJobRegistry] = []
        self.lock_acquisition_time: datetime | None = None
        self._connection_class, self._pool_class, self._pool_kwargs = parse_connection(connection)
        self.serializer = resolve_serializer(serializer)

        self._connection = None
        self.burst = burst
        self.interval = interval
        self._stop_requested = False
        self._status = self.Status.STOPPED
        self._process = None
        self.log = logging.getLogger(__name__)
        setup_loghandlers(
            level=logging_level,
            name=__name__,
            log_format=log_format,
            date_format=date_format,
        )

    @property
    def connection(self):
        if self._connection:
            return self._connection
        self._connection = self._connection_class(
            connection_pool=ConnectionPool(connection_class=self._pool_class, **self._pool_kwargs)
        )
        return self._connection

    @property
    def acquired_locks(self):
        return self._acquired_locks

    @property
    def status(self):
        return self._status

    @property
    def should_reacquire_locks(self):
        """Returns True if lock_acquisition_time is longer than 10 minutes ago"""
        if self._queue_names == self.acquired_locks:
            return False
        if not self.lock_acquisition_time:
            return True
        return (datetime.now() - self.lock_acquisition_time).total_seconds() > DEFAULT_SCHEDULER_FALLBACK_PERIOD

    def acquire_locks(self, auto_start=False):
        """Returns names of queue it successfully acquires lock on"""
        self.log.debug('Acquiring scheduler lock for %s', ', '.join(self._queue_names))
        with self.connection.pipeline() as pipeline:
            for name in self._queue_names:
                pipeline.set(
                    self.get_locking_key(name),
                    f'{self._ppid}:{self._pid or ""}:{self._token}',
                    nx=True,
                    ex=self.interval + 60,
                )
            result = pipeline.execute()

        if successful_locks := {name for name, locked in zip(self._queue_names, result) if locked}:
            self.log.info('Acquired scheduler lock for %s', ', '.join(successful_locks))

        # Always reset _scheduled_job_registries when acquiring locks
        self._scheduled_job_registries = []
        self._acquired_locks |= successful_locks
        self.lock_acquisition_time = datetime.now()

        # If auto_start is requested and scheduler is not started,
        # run self.start()
        if self._acquired_locks and auto_start:
            if self.burst:
                self.enqueue_scheduled_jobs()
                self.stop()

            elif not self._process or not self._process.is_alive():
                self.start()

        return successful_locks

    def _check_and_update_locks(self) -> set[str]:
        """
        Checks initial locks against the scheduler's initialization token, removes any locks that have expired
        during start-up, updates the lock to hold the child process PID, and returns the locks that are still held.
        """
        self.log.debug('Checking scheduler lock for %s', ', '.join(self._queue_names))
        with self.connection.pipeline() as pipeline:
            for name in self._queue_names:
                pipeline.get(self.get_locking_key(name))

            result = pipeline.execute()

        self._acquired_locks = successful_locks = {
            name
            for name, value in zip(self._queue_names, result)
            if value and value.decode('ascii').rpartition(':')[-1] == self._token
        }

        if not successful_locks:
            self.log.info('Scheduler stopping; All locks have expired.')
            self._status = self.Status.STOPPED
            return successful_locks

        # update locks with scheduler PID now that it has launched
        self._pid = os.getpid()
        self.lock_acquisition_time = datetime.now()
        with self.connection.pipeline() as pipeline:
            for name in successful_locks:
                pipeline.set(
                    self.get_locking_key(name),
                    f'{self._ppid}:{self._pid}:{self._token}',
                    ex=self.interval + 60,
                    xx=True,
                )

            pipeline.execute()

        return successful_locks

    def prepare_registries(self, queue_names: Iterable[str] | None = None):
        """Prepare scheduled job registries for use"""
        self._scheduled_job_registries = []
        if not queue_names:
            queue_names = self._acquired_locks
        for name in queue_names:
            self._scheduled_job_registries.append(
                ScheduledJobRegistry(name, connection=self.connection, serializer=self.serializer)
            )

    @classmethod
    def get_locking_key(cls, name: str):
        """Returns scheduler key for a given queue name"""
        return SCHEDULER_LOCKING_KEY_TEMPLATE % name

    def enqueue_scheduled_jobs(self):
        """Enqueue jobs whose timestamp is in the past"""
        self._status = self.Status.WORKING

        if not self._scheduled_job_registries and self._acquired_locks:
            self.prepare_registries()

        for registry in self._scheduled_job_registries:
            timestamp = current_timestamp()

            # TODO: try to use Lua script to make get_jobs_to_schedule()
            # and remove_jobs() atomic
            job_ids = registry.get_jobs_to_schedule(timestamp)

            if not job_ids:
                continue

            queue = Queue(registry.name, connection=self.connection, serializer=self.serializer)

            with self.connection.pipeline() as pipeline:
                jobs = Job.fetch_many(job_ids, connection=self.connection, serializer=self.serializer)
                for job in jobs:
                    if job is not None:
                        queue._enqueue_job(job, pipeline=pipeline, at_front=job.should_enqueue_at_front())
                for job_id in job_ids:
                    registry.remove(job_id, pipeline=pipeline)
                pipeline.execute()
        self._status = self.Status.STARTED

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.
        """
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def request_stop(self, signum=None, frame=None):
        """Toggle self._stop_requested that's checked on every loop"""
        self._stop_requested = True

    def heartbeat(self):
        """Updates the TTL on scheduler keys and the locks"""
        if not (locks := self._acquired_locks):
            return

        self.log.debug('Scheduler sending heartbeat to %s', ', '.join(locks))
        with self.connection.pipeline() as pipeline:
            for name in locks:
                key = self.get_locking_key(name)
                pipeline.expire(key, self.interval + 60)
                pipeline.get(key)
            result = pipeline.execute()

        if lost := {
            name
            for name, (updated_expiration, value) in zip(locks, split_list(result, 2))
            if not updated_expiration or not value or value.decode('ascii').rpartition(':')[-1] != self._token
        }:
            self.log.info('Scheduler lost locks for %s...', ', '.join(lost))
            self._acquired_locks -= lost
            self._scheduled_job_registries = []

    def stop(self):
        if locks := self._acquired_locks:
            self.log.info('Scheduler stopping, releasing locks for %s...', ', '.join(locks))
            self.release_locks()
        else:
            self.log.info('Scheduler stopping, no locks to release')
        self._status = self.Status.STOPPED

    def release_locks(self):
        """Release acquired locks"""
        delete_scheduler_locks(
            self.connection, self._token, [self.get_locking_key(name) for name in self._acquired_locks]
        )
        self._acquired_locks = set()

    def start(self):
        self._status = self.Status.STARTED
        # Redis instance can't be pickled across processes so we need to
        # clean this up before forking
        self._connection = None
        self._process = ForkProcess(target=run, args=(self,), name='Scheduler')
        self._process.start()
        return self._process

    def work(self):
        self._install_signal_handlers()
        self._check_and_update_locks()

        while self.status != self.Status.STOPPED:
            if self._stop_requested:
                self.stop()
                break

            if self.should_reacquire_locks:
                self.acquire_locks()

            self.enqueue_scheduled_jobs()
            self.heartbeat()

            # if all locks were lost, try to re-obtain a lock or shutdown
            if not self.acquired_locks:
                if not self.acquire_locks():
                    self.stop()
                    break

            time.sleep(self.interval)


def run(scheduler):
    pid = os.getpid()
    scheduler.log.info('Scheduler for %s started with PID %s', ', '.join(scheduler._queue_names), pid)
    try:
        scheduler.work()
    except:  # noqa
        scheduler.log.error('Scheduler [PID %s] raised an exception.\n%s', pid, traceback.format_exc())
        raise
    scheduler.log.info('Scheduler with PID %d has stopped', pid)
