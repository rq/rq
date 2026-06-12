from __future__ import annotations

import logging
import os
import signal
import socket
import time
import traceback
from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from multiprocessing import Process, get_context
from multiprocessing.process import BaseProcess
from uuid import uuid4

from redis import ConnectionPool, Redis
from redis.client import Pipeline

from .connections import parse_connection
from .defaults import DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT, DEFAULT_SCHEDULER_FALLBACK_PERIOD
from .exceptions import SchedulerNotFound
from .job import Job
from .logutils import setup_loghandlers
from .queue import Queue
from .registry import ScheduledJobRegistry
from .serializers import resolve_serializer
from .utils import current_timestamp, decode_redis_hash, now, parse_names, utcformat, utcparse

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
        interval=1,
        logging_level: str | int = logging.INFO,
        date_format=DEFAULT_LOGGING_DATE_FORMAT,
        log_format=DEFAULT_LOGGING_FORMAT,
        serializer=None,
        name: str | None = None,
    ):
        self._queue_names = set(parse_names(queues))
        self._acquired_locks: set[str] = set()
        self._scheduled_job_registries: list[ScheduledJobRegistry] = []
        self.lock_acquisition_time = None
        self._connection_class, self._pool_class, self._pool_kwargs = parse_connection(connection)
        self.serializer = resolve_serializer(serializer)

        # Identity, stable across the fork (name is pickled to the child process).
        self.name: str = name or uuid4().hex
        self.hostname: str = socket.gethostname()
        self.created_at: datetime = now()
        self.pid: int = 0  # set in register_birth(), once the scheduler process has forked
        self.last_heartbeat: datetime | None = None

        self._connection = None
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
    def key(self) -> str:
        """Redis key holding this scheduler's metadata hash."""
        return SCHEDULER_KEY_TEMPLATE % self.name

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
        successful_locks = set()
        self.log.debug('Acquiring scheduler lock for %s', ', '.join(self._queue_names))
        for name in self._queue_names:
            if self.connection.set(self.get_locking_key(name), self.name, nx=True, ex=self.interval + 60):
                self.log.info('Acquired scheduler lock for %s', name)
                successful_locks.add(name)

        # Always reset _scheduled_job_registries when acquiring locks
        self._scheduled_job_registries = []
        self._acquired_locks = self._acquired_locks.union(successful_locks)
        self.lock_acquisition_time = datetime.now()

        # If auto_start is requested and scheduler is not started,
        # run self.start()
        if self._acquired_locks and auto_start:
            if not self._process or not self._process.is_alive():
                self.start()

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

    def to_dict(self) -> dict:
        """Serialize this scheduler's metadata for storage in its Redis hash."""
        assert self.last_heartbeat is not None
        return {
            'name': self.name,
            'hostname': self.hostname,
            'pid': str(self.pid),
            'queues': ','.join(self._queue_names),
            'created_at': utcformat(self.created_at),
            'last_heartbeat': utcformat(self.last_heartbeat),
        }

    def restore(self, raw_data: dict) -> None:
        """Restore this scheduler's metadata from its Redis hash."""
        obj = decode_redis_hash(raw_data, decode_values=True)
        self.name = obj['name']
        self.hostname = obj['hostname']
        self.pid = int(obj['pid'])
        self._queue_names = set(obj['queues'].split(',')) if obj.get('queues') else set()
        self.created_at = utcparse(obj['created_at'])
        self.last_heartbeat = utcparse(obj['last_heartbeat']) if obj.get('last_heartbeat') else None

    def save(self, pipeline: Pipeline | None = None) -> None:
        """Save this scheduler's metadata hash with a TTL."""
        connection = pipeline if pipeline is not None else self.connection.pipeline()
        connection.hset(self.key, mapping=self.to_dict())
        connection.expire(self.key, self.interval + 60)

        if pipeline is None:
            connection.execute()

    def register_birth(self) -> None:
        """Register this scheduler's birth by writing its metadata hash.

        Idempotent: re-registering the same name (e.g. when the worker restarts a crashed
        scheduler process) overwrites the existing hash rather than erroring.
        """
        self.log.debug('Scheduler %s: registering birth', self.name)
        self.pid = os.getpid()
        self.last_heartbeat = now()
        self.save()

    def register_death(self) -> bool:
        """Register this scheduler's death by deleting its metadata hash.

        Returns:
            True if the scheduler metadata existed and was deleted, False if it was already absent.
        """
        self.log.debug('Scheduler %s: registering death', self.name)
        return bool(self.connection.delete(self.key))

    @classmethod
    def fetch(cls, name: str, connection: Redis) -> RQScheduler:
        """Fetch a scheduler by name, restoring it from its Redis hash."""
        raw_data = connection.hgetall(SCHEDULER_KEY_TEMPLATE % name)
        if not raw_data:
            raise SchedulerNotFound(f"Scheduler with name '{name}' not found")
        scheduler = cls([], connection=connection, name=name)
        scheduler.restore(raw_data)
        return scheduler

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
        """Refresh the TTL on the scheduler's metadata hash and its locks."""
        self.log.debug('Scheduler sending heartbeat to %s', ', '.join(self.acquired_locks))
        self.last_heartbeat = now()
        with self.connection.pipeline() as pipeline:
            pipeline.hset(self.key, 'last_heartbeat', utcformat(self.last_heartbeat))
            pipeline.expire(self.key, self.interval + 60)
            for name in self._acquired_locks:
                pipeline.expire(self.get_locking_key(name), self.interval + 60)
            pipeline.execute()

    def stop(self):
        self.log.info('Scheduler stopping, releasing locks for %s...', ', '.join(self._acquired_locks))
        self.release_locks()
        self._status = self.Status.STOPPED
        self.register_death()

    def release_locks(self):
        """Release acquired locks"""
        keys = [self.get_locking_key(name) for name in self._acquired_locks]
        self.connection.delete(*keys)
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
        self.register_birth()

        while True:
            if self._stop_requested:
                self.stop()
                break

            if self.should_reacquire_locks:
                self.acquire_locks()

            self.enqueue_scheduled_jobs()
            self.heartbeat()
            time.sleep(self.interval)


def run(scheduler):
    scheduler.log.info('Scheduler for %s started with PID %s', ', '.join(scheduler._queue_names), os.getpid())
    try:
        scheduler.work()
    except:  # noqa
        scheduler.log.error('Scheduler [PID %s] raised an exception.\n%s', os.getpid(), traceback.format_exc())
        raise
    scheduler.log.info('Scheduler with PID %d has stopped', os.getpid())
