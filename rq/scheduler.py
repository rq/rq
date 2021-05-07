import logging
import os
import signal
import time
import traceback
from datetime import datetime
from enum import Enum
from multiprocessing import Process

from redis import SSLConnection, UnixDomainSocketConnection

from .defaults import DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT
from .job import Job
from .logutils import setup_loghandlers
from .queue import Queue
from .registry import ScheduledJobRegistry
from .serializers import resolve_serializer
from .utils import current_timestamp

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

    def __init__(self, queues, connection, interval=1, logging_level=logging.INFO,
                 date_format=DEFAULT_LOGGING_DATE_FORMAT,
                 log_format=DEFAULT_LOGGING_FORMAT, serializer=None):
        self._queue_names = set(parse_names(queues))
        self._acquired_locks = set()
        self._scheduled_job_registries = []
        self.lock_acquisition_time = None
        # Copy the connection kwargs before mutating them in order to not change the arguments
        # used by the current connection pool to create new connections
        self._connection_kwargs = connection.connection_pool.connection_kwargs.copy()
        # Redis does not accept parser_class argument which is sometimes present
        # on connection_pool kwargs, for example when hiredis is used
        self._connection_kwargs.pop('parser_class', None)
        self._connection_class = connection.__class__  # client
        connection_class = connection.connection_pool.connection_class
        if issubclass(connection_class, SSLConnection):
            self._connection_kwargs['ssl'] = True
        if issubclass(connection_class, UnixDomainSocketConnection):
            # The connection keyword arguments are obtained from
            # `UnixDomainSocketConnection`, which expects `path`, but passed to
            # `redis.client.Redis`, which expects `unix_socket_path`, renaming
            # the key is necessary.
            # `path` is not left in the dictionary as that keyword argument is
            # not expected by `redis.client.Redis` and would raise an exception.
            self._connection_kwargs['unix_socket_path'] = self._connection_kwargs.pop(
                'path'
            )
        self.serializer = resolve_serializer(serializer)

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
        self._connection = self._connection_class(**self._connection_kwargs)
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
        return (datetime.now() - self.lock_acquisition_time).total_seconds() > 600

    def acquire_locks(self, auto_start=False):
        """Returns names of queue it successfully acquires lock on"""
        successful_locks = set()
        pid = os.getpid()
        self.log.info("Trying to acquire locks for %s", ", ".join(self._queue_names))
        for name in self._queue_names:
            if self.connection.set(self.get_locking_key(name), pid, nx=True, ex=60):
                successful_locks.add(name)

        # Always reset _scheduled_job_registries when acquiring locks
        self._scheduled_job_registries = []
        self._acquired_locks = self._acquired_locks.union(successful_locks)
        self.lock_acquisition_time = datetime.now()

        # If auto_start is requested and scheduler is not started,
        # run self.start()
        if self._acquired_locks and auto_start:
            if not self._process:
                self.start()

        return successful_locks

    def prepare_registries(self, queue_names=None):
        """Prepare scheduled job registries for use"""
        self._scheduled_job_registries = []
        if not queue_names:
            queue_names = self._acquired_locks
        for name in queue_names:
            self._scheduled_job_registries.append(
                ScheduledJobRegistry(name, connection=self.connection)
            )

    @classmethod
    def get_locking_key(cls, name):
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
                jobs = Job.fetch_many(
                    job_ids, connection=self.connection, serializer=self.serializer
                )
                for job in jobs:
                    if job is not None:
                        queue.enqueue_job(job, pipeline=pipeline)
                        registry.remove(job, pipeline=pipeline)
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
        self.log.debug("Scheduler sending heartbeat to %s",
                       ", ".join(self.acquired_locks))
        if len(self._queue_names) > 1:
            with self.connection.pipeline() as pipeline:
                for name in self._queue_names:
                    key = self.get_locking_key(name)
                    pipeline.expire(key, self.interval + 5)
                pipeline.execute()
        else:
            key = self.get_locking_key(next(iter(self._queue_names)))
            self.connection.expire(key, self.interval + 5)

    def stop(self):
        self.log.info("Scheduler stopping, releasing locks for %s...",
                      ','.join(self._queue_names))
        self.release_locks()
        self._status = self.Status.STOPPED

    def release_locks(self):
        """Release acquired locks"""
        keys = [self.get_locking_key(name) for name in self._queue_names]
        self.connection.delete(*keys)
        self._acquired_locks = set()

    def start(self):
        self._status = self.Status.STARTED
        # Redis instance can't be pickled across processes so we need to
        # clean this up before forking
        self._connection = None
        self._process = Process(target=run, args=(self,), name='Scheduler')
        self._process.start()
        return self._process

    def work(self):
        self._install_signal_handlers()

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
    scheduler.log.info("Scheduler for %s started with PID %s",
                       ','.join(scheduler._queue_names), os.getpid())
    try:
        scheduler.work()
    except:  # noqa
        scheduler.log.error(
            'Scheduler [PID %s] raised an exception.\n%s',
            os.getpid(), traceback.format_exc()
        )
        raise
    scheduler.log.info("Scheduler with PID %s has stopped", os.getpid())


def parse_names(queues_or_names):
    """Given a list of strings or queues, returns queue names"""
    names = []
    for queue_or_name in queues_or_names:
        if isinstance(queue_or_name, Queue):
            names.append(queue_or_name.name)
        else:
            names.append(str(queue_or_name))
    return names
