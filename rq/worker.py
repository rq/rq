import contextlib
import errno
import logging
import math
import os
import random
import signal
import socket
import sys
import time
import traceback
import warnings
from datetime import datetime, timedelta
from enum import Enum
from random import shuffle
from types import FrameType
from typing import TYPE_CHECKING, Callable, Optional, Union
from uuid import uuid4

if TYPE_CHECKING:
    try:
        from resource import struct_rusage
    except ImportError:
        pass
    from redis import Redis
    from redis.client import Pipeline, PubSub, PubSubWorkerThread

from contextlib import suppress

import redis.exceptions

from . import worker_registration
from .command import PUBSUB_CHANNEL_TEMPLATE, handle_command, parse_payload
from .defaults import (
    DEFAULT_JOB_MONITORING_INTERVAL,
    DEFAULT_LOGGING_DATE_FORMAT,
    DEFAULT_LOGGING_FORMAT,
    DEFAULT_MAINTENANCE_TASK_INTERVAL,
    DEFAULT_RESULT_TTL,
    DEFAULT_WORKER_TTL,
)
from .exceptions import DequeueTimeout, DeserializationError, InvalidJobOperation, ShutDownImminentException
from .executions import Execution
from .group import Group
from .job import Job, JobStatus, Retry
from .logutils import blue, green, setup_loghandlers, yellow
from .queue import Queue
from .registry import StartedJobRegistry, clean_registries
from .scheduler import RQScheduler
from .serializers import Serializer, resolve_serializer
from .suspension import is_suspended
from .timeouts import HorseMonitorTimeoutException, JobTimeoutException, UnixSignalDeathPenalty
from .utils import (
    as_text,
    compact,
    ensure_job_list,
    get_connection_from_queues,
    get_version,
    now,
    utcformat,
    utcparse,
)
from .version import VERSION

try:
    from setproctitle import setproctitle as setprocname
except ImportError:

    def setprocname(title: str) -> None:
        pass


# Set initial level to INFO.
logger = logging.getLogger('rq.worker')
if logger.level == logging.NOTSET:
    logger.setLevel(logging.INFO)


class StopRequested(Exception):
    pass


_signames = dict(
    (getattr(signal, signame), signame) for signame in dir(signal) if signame.startswith('SIG') and '_' not in signame
)


def signal_name(signum):
    try:
        if sys.version_info[:2] >= (3, 5):
            return signal.Signals(signum).name
        else:
            return _signames[signum]

    except KeyError:
        return 'SIG_UNKNOWN'
    except ValueError:
        return 'SIG_UNKNOWN'


SHUTDOWN_SIGNAL = signal.SIGTERM if not hasattr(signal, 'SIGKILL') else signal.SIGKILL


class DequeueStrategy(str, Enum):
    DEFAULT = 'default'
    ROUND_ROBIN = 'round_robin'
    RANDOM = 'random'


class WorkerStatus(str, Enum):
    STARTED = 'started'
    SUSPENDED = 'suspended'
    BUSY = 'busy'
    IDLE = 'idle'


class BaseWorker:
    redis_worker_namespace_prefix = 'rq:worker:'
    redis_workers_keys = worker_registration.REDIS_WORKER_KEYS
    death_penalty_class = UnixSignalDeathPenalty
    queue_class = Queue
    job_class = Job

    # `log_result_lifespan` controls whether "Result is kept for XXX seconds"
    # messages are logged after every job, by default they are.
    log_result_lifespan = True
    # `log_job_description` is used to toggle logging an entire jobs description.
    log_job_description = True
    # factor to increase connection_wait_time in case of continuous connection failures.
    exponential_backoff_factor = 2.0
    # Max Wait time (in seconds) after which exponential_backoff_factor won't be applicable.
    max_connection_wait_time = 60.0

    def __init__(
        self,
        queues,
        name: Optional[str] = None,
        default_result_ttl=DEFAULT_RESULT_TTL,
        connection: Optional['Redis'] = None,
        exc_handler=None,
        exception_handlers=None,
        maintenance_interval: int = DEFAULT_MAINTENANCE_TASK_INTERVAL,
        default_worker_ttl: Optional[int] = None,  # TODO remove this arg in 3.0
        worker_ttl: Optional[int] = None,
        job_class: Optional[type[Job]] = None,
        queue_class: Optional[type[Queue]] = None,
        log_job_description: bool = True,
        job_monitoring_interval=DEFAULT_JOB_MONITORING_INTERVAL,
        disable_default_exception_handler: bool = False,
        prepare_for_work: bool = True,
        serializer: Optional[Union[Serializer, str]] = None,
        work_horse_killed_handler: Optional[Callable[[Job, int, int, 'struct_rusage'], None]] = None,
    ):  # noqa
        self.default_result_ttl = default_result_ttl

        if worker_ttl:
            self.worker_ttl = worker_ttl
        elif default_worker_ttl:
            warnings.warn('default_worker_ttl is deprecated, use worker_ttl.', DeprecationWarning, stacklevel=2)
            self.worker_ttl = default_worker_ttl
        else:
            self.worker_ttl = DEFAULT_WORKER_TTL

        self.job_monitoring_interval = job_monitoring_interval
        self.maintenance_interval = maintenance_interval

        if not connection:
            connection = get_connection_from_queues(queues)

        assert connection
        connection = self._set_connection(connection)
        self.connection = connection
        self.redis_server_version = None

        self.job_class = job_class if job_class else Job
        self.queue_class = queue_class if queue_class else Queue
        self.version: str = VERSION
        self.python_version: str = sys.version
        self.serializer = resolve_serializer(serializer)
        self.execution: Optional[Execution] = None

        queues = [
            (
                self.queue_class(
                    name=q,
                    connection=connection,
                    job_class=self.job_class,
                    serializer=self.serializer,
                    death_penalty_class=self.death_penalty_class,
                )
                if isinstance(q, str)
                else q
            )
            for q in ensure_job_list(queues)
        ]

        self.name: str = name or uuid4().hex
        self.queues = queues
        self.validate_queues()
        self._ordered_queues = self.queues[:]
        self._exc_handlers: list[Callable] = []
        self._work_horse_killed_handler = work_horse_killed_handler
        self._shutdown_requested_date: Optional[datetime] = None

        self._state: str = 'starting'
        self._is_horse: bool = False
        self._horse_pid: int = 0
        self._stop_requested: bool = False
        self._stopped_job_id = None

        self.log = logger
        self.log_job_description = log_job_description
        self.last_cleaned_at = None
        self.successful_job_count: int = 0
        self.failed_job_count: int = 0
        self.total_working_time: float = 0
        self.current_job_working_time: float = 0
        self.birth_date = None
        self.scheduler: Optional[RQScheduler] = None
        self.pubsub: Optional[PubSub] = None
        self.pubsub_thread = None
        self._dequeue_strategy: Optional[DequeueStrategy] = DequeueStrategy.DEFAULT

        self.disable_default_exception_handler = disable_default_exception_handler

        if prepare_for_work:
            self.hostname: Optional[str] = socket.gethostname()
            self.pid: Optional[int] = os.getpid()
            try:
                connection.client_setname(self.name)
            except redis.exceptions.ResponseError:
                warnings.warn('CLIENT SETNAME command not supported, setting ip_address to unknown', Warning)
                self.ip_address = 'unknown'
            else:
                client_addresses = [
                    client['addr'] for client in connection.client_list() if client.get('name') == self.name
                ]
                if len(client_addresses) > 0:
                    self.ip_address = client_addresses[0]
                else:
                    warnings.warn('CLIENT LIST command not supported, setting ip_address to unknown', Warning)
                    self.ip_address = 'unknown'
        else:
            self.hostname = None
            self.pid = None
            self.ip_address = 'unknown'

        if isinstance(exception_handlers, (list, tuple)):
            for handler in exception_handlers:
                self.push_exc_handler(handler)
        elif exception_handlers is not None:
            self.push_exc_handler(exception_handlers)

    @classmethod
    def find_by_key(
        cls,
        worker_key: str,
        connection: 'Redis',
        job_class: Optional[type['Job']] = None,
        queue_class: Optional[type['Queue']] = None,
        serializer: Optional[Union[Serializer, str]] = None,
    ) -> Optional['BaseWorker']:
        """Returns a Worker instance, based on the naming conventions for
        naming the internal Redis keys.  Can be used to reverse-lookup Workers
        by their Redis keys.

        Args:
            worker_key (str): The worker key
            connection (Optional[Redis], optional): Redis connection. Defaults to None.
            job_class (Optional[Type[Job]], optional): The job class if custom class is being used. Defaults to None.
            queue_class (Optional[Type[Queue]]): The queue class if a custom class is being used. Defaults to None.
            serializer (Optional[Union[Serializer, str]], optional): The serializer to use. Defaults to None.

        Raises:
            ValueError: If the key doesn't start with `rq:worker:`, the default worker namespace prefix.

        Returns:
            worker (Worker): The Worker instance.
        """
        prefix = cls.redis_worker_namespace_prefix
        if not worker_key.startswith(prefix):
            raise ValueError('Not a valid RQ worker key: %s' % worker_key)

        if not connection.exists(worker_key):
            connection.srem(cls.redis_workers_keys, worker_key)
            return None

        name = worker_key[len(prefix) :]
        worker = cls(
            [],
            name,
            connection=connection,
            job_class=job_class,
            queue_class=queue_class,
            prepare_for_work=False,
            serializer=serializer,
        )

        worker.refresh()
        return worker

    @classmethod
    def all(
        cls,
        connection: Optional['Redis'] = None,
        job_class: Optional[type['Job']] = None,
        queue_class: Optional[type['Queue']] = None,
        queue: Optional['Queue'] = None,
        serializer=None,
    ) -> list['BaseWorker']:
        """Returns an iterable of all Workers.

        Returns:
            workers (List[Worker]): A list of workers
        """
        if queue:
            connection = queue.connection

        assert connection
        worker_keys = worker_registration.get_keys(queue=queue, connection=connection)
        workers = [
            cls.find_by_key(
                key, connection=connection, job_class=job_class, queue_class=queue_class, serializer=serializer
            )
            for key in worker_keys
        ]
        return compact(workers)

    @classmethod
    def all_keys(cls, connection: Optional['Redis'] = None, queue: Optional['Queue'] = None) -> list[str]:
        """List of worker keys

        Args:
            connection (Optional[Redis], optional): A Redis Connection. Defaults to None.
            queue (Optional[Queue], optional): The Queue. Defaults to None.

        Returns:
            list_keys (List[str]): A list of worker keys
        """
        return [as_text(key) for key in worker_registration.get_keys(queue=queue, connection=connection)]

    @classmethod
    def count(cls, connection: Optional['Redis'] = None, queue: Optional['Queue'] = None) -> int:
        """Returns the number of workers by queue or connection.

        Args:
            connection (Optional[Redis], optional): Redis connection. Defaults to None.
            queue (Optional[Queue], optional): The queue to use. Defaults to None.

        Returns:
            length (int): The queue length.
        """
        return len(worker_registration.get_keys(queue=queue, connection=connection))

    def refresh(self):
        """Refreshes the worker data.
        It will get the data from the datastore and update the Worker's attributes
        """
        data = self.connection.hmget(
            self.key,
            'queues',
            'state',
            'current_job',
            'last_heartbeat',
            'birth',
            'failed_job_count',
            'successful_job_count',
            'total_working_time',
            'current_job_working_time',
            'hostname',
            'ip_address',
            'pid',
            'version',
            'python_version',
        )
        (
            queues,
            state,
            job_id,
            last_heartbeat,
            birth,
            failed_job_count,
            successful_job_count,
            total_working_time,
            current_job_working_time,
            hostname,
            ip_address,
            pid,
            version,
            python_version,
        ) = data
        self.hostname = as_text(hostname) if hostname else None
        self.ip_address = as_text(ip_address) if ip_address else None
        self.pid = int(pid) if pid else None
        self.version = as_text(version) if version else None
        self.python_version = as_text(python_version) if python_version else None
        self._state = as_text(state or '?')
        self._job_id = job_id or None
        if last_heartbeat:
            self.last_heartbeat = utcparse(as_text(last_heartbeat))
        else:
            self.last_heartbeat = None
        if birth:
            self.birth_date = utcparse(as_text(birth))
        else:
            self.birth_date = None
        if failed_job_count:
            self.failed_job_count = int(as_text(failed_job_count))
        if successful_job_count:
            self.successful_job_count = int(as_text(successful_job_count))
        if total_working_time:
            self.total_working_time = float(as_text(total_working_time))
        if current_job_working_time:
            self.current_job_working_time = float(as_text(current_job_working_time))

        if queues:
            queues = as_text(queues)
            self.queues = [
                self.queue_class(
                    queue, connection=self.connection, job_class=self.job_class, serializer=self.serializer
                )
                for queue in queues.split(',')
            ]

    @property
    def should_run_maintenance_tasks(self):
        """Maintenance tasks should run on first startup or every 10 minutes."""
        if self.last_cleaned_at is None:
            return True
        if (now() - self.last_cleaned_at) > timedelta(seconds=self.maintenance_interval):
            return True
        return False

    def _set_connection(self, connection: 'Redis') -> 'Redis':
        """Configures the Redis connection's socket timeout.
        This will timeout the connection in case any specific command hangs at any given time (eg. BLPOP), but
        also ensures that the timeout is long enough for those operations.
        If the connection provided already has an adequate `socket_timeout` defined, skips.

        Args:
            connection (Optional[Redis]): The Redis Connection.
        """
        current_socket_timeout = connection.connection_pool.connection_kwargs.get('socket_timeout')
        if current_socket_timeout is None or current_socket_timeout < self.connection_timeout:
            timeout_config = {'socket_timeout': self.connection_timeout}
            connection.connection_pool.connection_kwargs.update(timeout_config)
        return connection

    @property
    def dequeue_timeout(self) -> int:
        return max(1, self.worker_ttl - 15)

    @property
    def connection_timeout(self) -> int:
        return self.dequeue_timeout + 10

    def clean_registries(self):
        """Runs maintenance jobs on each Queue's registries."""
        for queue in self.queues:
            # If there are multiple workers running, we only want 1 worker
            # to run clean_registries().
            if queue.acquire_maintenance_lock():
                self.log.info('Cleaning registries for queue: %s', queue.name)
                clean_registries(queue, self._exc_handlers)
                worker_registration.clean_worker_registry(queue)
                queue.intermediate_queue.cleanup(self, queue)
                queue.release_maintenance_lock()
        self.last_cleaned_at = now()

    def get_redis_server_version(self):
        """Return Redis server version of connection"""
        if not self.redis_server_version:
            self.redis_server_version = get_version(self.connection)
        return self.redis_server_version

    def validate_queues(self):
        """Sanity check for the given queues."""
        for queue in self.queues:
            if not isinstance(queue, self.queue_class):
                raise TypeError(f'{queue} is not of type {self.queue_class} or string types')

    def queue_names(self) -> list[str]:
        """Returns the queue names of this worker's queues.

        Returns:
            List[str]: The queue names.
        """
        return [queue.name for queue in self.queues]

    def queue_keys(self) -> list[str]:
        """Returns the Redis keys representing this worker's queues.

        Returns:
            List[str]: The list of strings with queues keys
        """
        return [queue.key for queue in self.queues]

    @property
    def key(self):
        """Returns the worker's Redis hash key."""
        return self.redis_worker_namespace_prefix + self.name

    @property
    def pubsub_channel_name(self):
        """Returns the worker's Redis hash key."""
        return PUBSUB_CHANNEL_TEMPLATE % self.name

    @property
    def supports_redis_streams(self) -> bool:
        """Only supported by Redis server >= 5.0 is required."""
        return self.get_redis_server_version() >= (5, 0, 0)

    def request_stop(self, signum, frame):
        """Stops the current worker loop but waits for child processes to
        end gracefully (warm shutdown).

        Args:
            signum (Any): Signum
            frame (Any): Frame
        """
        self.log.debug('Got signal %s', signal_name(signum))
        self._shutdown_requested_date = now()

        signal.signal(signal.SIGINT, self.request_force_stop)
        signal.signal(signal.SIGTERM, self.request_force_stop)

        self.handle_warm_shutdown_request()
        self._shutdown()

    def _shutdown(self):
        """
        If shutdown is requested in the middle of a job, wait until
        finish before shutting down and save the request in redis
        """
        if self.get_state() == WorkerStatus.BUSY:
            self._stop_requested = True
            self.set_shutdown_requested_date()
            self.log.debug('Stopping after current horse is finished. Press Ctrl+C again for a cold shutdown.')
            if self.scheduler:
                self.stop_scheduler()
        else:
            if self.scheduler:
                self.stop_scheduler()
            raise StopRequested()

    def request_force_stop(self, signum: int, frame: Optional[FrameType]):
        raise NotImplementedError()

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM gracefully."""
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def execute_job(self, job: 'Job', queue: 'Queue'):
        """To be implemented by subclasses."""
        raise NotImplementedError

    def work(
        self,
        burst: bool = False,
        logging_level: Optional[str] = None,
        date_format: str = DEFAULT_LOGGING_DATE_FORMAT,
        log_format: str = DEFAULT_LOGGING_FORMAT,
        max_jobs: Optional[int] = None,
        max_idle_time: Optional[int] = None,
        with_scheduler: bool = False,
        dequeue_strategy: DequeueStrategy = DequeueStrategy.DEFAULT,
    ) -> bool:
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.
        If `max_idle_time` is provided, worker will die when it's idle for more than the provided value.

        The return value indicates whether any jobs were processed.

        Args:
            burst (bool, optional): Whether to work on burst mode. Defaults to False.
            logging_level (Optional[str], optional): Logging level to use.
                If not provided, defaults to "INFO" unless a class-level logging level is already set.
            date_format (str, optional): Date Format. Defaults to DEFAULT_LOGGING_DATE_FORMAT.
            log_format (str, optional): Log Format. Defaults to DEFAULT_LOGGING_FORMAT.
            max_jobs (Optional[int], optional): Max number of jobs. Defaults to None.
            max_idle_time (Optional[int], optional): Max seconds for worker to be idle. Defaults to None.
            with_scheduler (bool, optional): Whether to run the scheduler in a separate process. Defaults to False.
            dequeue_strategy (DequeueStrategy, optional): Which strategy to use to dequeue jobs.
                Defaults to DequeueStrategy.DEFAULT

        Returns:
            worked (bool): Will return True if any job was processed, False otherwise.
        """
        self.bootstrap(logging_level, date_format, log_format)
        self._dequeue_strategy = dequeue_strategy
        completed_jobs = 0
        if with_scheduler:
            self._start_scheduler(burst, logging_level, date_format, log_format)

        self._install_signal_handlers()
        try:
            while True:
                try:
                    self.check_for_suspension(burst)

                    if self.should_run_maintenance_tasks:
                        self.run_maintenance_tasks()

                    if self._stop_requested:
                        self.log.info('Worker %s: stopping on request', self.key)
                        break

                    timeout = None if burst else self.dequeue_timeout
                    result = self.dequeue_job_and_maintain_ttl(timeout, max_idle_time)
                    if result is None:
                        if burst:
                            self.log.info('Worker %s: done, quitting', self.key)
                        elif max_idle_time is not None:
                            self.log.info('Worker %s: idle for %d seconds, quitting', self.key, max_idle_time)
                        break

                    job, queue = result
                    self.execute_job(job, queue)
                    self.heartbeat()

                    completed_jobs += 1
                    if max_jobs is not None:
                        if completed_jobs >= max_jobs:
                            self.log.info('Worker %s: finished executing %d jobs, quitting', self.key, completed_jobs)
                            break

                except redis.exceptions.TimeoutError:
                    self.log.error('Worker %s: Redis connection timeout, quitting...', self.key)
                    break

                except StopRequested:
                    break

                except SystemExit:
                    # Cold shutdown detected
                    raise

                except:  # noqa
                    self.log.error('Worker %s: found an unhandled exception, quitting...', self.key, exc_info=True)
                    break
        finally:
            self.teardown()
        return bool(completed_jobs)

    def cleanup_execution(self, job: 'Job', pipeline: 'Pipeline'):
        """Cleans up the execution of a job.
        It will remove the job execution record from the `StartedJobRegistry` and delete the Execution object.
        """
        self.log.debug('Cleaning up execution of job %s', job.id)
        self.set_current_job_id(None, pipeline=pipeline)
        if self.execution is not None:
            self.execution.delete(job=job, pipeline=pipeline)
            self.execution = None

    def handle_warm_shutdown_request(self):
        self.log.info('Worker %s [PID %d]: warm shut down requested', self.name, self.pid)

    def reorder_queues(self, reference_queue: 'Queue'):
        """Reorder the queues according to the strategy.
        As this can be defined both in the `Worker` initialization or in the `work` method,
        it doesn't take the strategy directly, but rather uses the private `_dequeue_strategy` attribute.

        Args:
            reference_queue (Union[Queue, str]): The queues to reorder
        """
        if self._dequeue_strategy is None:
            self._dequeue_strategy = DequeueStrategy.DEFAULT

        if self._dequeue_strategy not in ('default', 'random', 'round_robin'):
            raise ValueError(
                f'Dequeue strategy {self._dequeue_strategy} is not allowed. Use `default`, `random` or `round_robin`.'
            )
        if self._dequeue_strategy == DequeueStrategy.DEFAULT:
            return
        if self._dequeue_strategy == DequeueStrategy.ROUND_ROBIN:
            pos = self._ordered_queues.index(reference_queue)
            self._ordered_queues = self._ordered_queues[pos + 1 :] + self._ordered_queues[: pos + 1]
            return
        if self._dequeue_strategy == DequeueStrategy.RANDOM:
            shuffle(self._ordered_queues)
            return

    def handle_job_failure(self, job: 'Job', queue: 'Queue', started_job_registry=None, exc_string=''):
        """
        Handles the failure or an executing job by:
            1. Setting the job status to failed
            2. Removing the job from StartedJobRegistry
            3. Setting the workers current job to None
            4. Add the job to FailedJobRegistry
        `save_exc_to_job` should only be used for testing purposes
        """
        self.log.debug('Handling failed execution of job %s', job.id)
        with self.connection.pipeline() as pipeline:
            if started_job_registry is None:
                started_job_registry = StartedJobRegistry(
                    job.origin, self.connection, job_class=self.job_class, serializer=self.serializer
                )

            # check whether a job was stopped intentionally and set the job
            # status appropriately if it was this job.
            job_is_stopped = self._stopped_job_id == job.id
            retry = job.should_retry and not job_is_stopped

            if job_is_stopped:
                job.set_status(JobStatus.STOPPED, pipeline=pipeline)
                self._stopped_job_id = None
            else:
                # Requeue/reschedule if retry is configured, otherwise
                if not retry:
                    job.set_status(JobStatus.FAILED, pipeline=pipeline)

            self.cleanup_execution(job, pipeline=pipeline)

            if not self.disable_default_exception_handler and not retry:
                job._handle_failure(exc_string, pipeline=pipeline)
                with suppress(redis.exceptions.ConnectionError):
                    pipeline.execute()

            self.increment_failed_job_count(pipeline)
            if job.started_at and job.ended_at:
                self.increment_total_working_time(job.ended_at - job.started_at, pipeline)

            if retry:
                job.retry(queue, pipeline)
                enqueue_dependents = False
            else:
                enqueue_dependents = True

            try:
                pipeline.execute()
                if enqueue_dependents:
                    queue.enqueue_dependents(job)
            except Exception:
                # Ensure that custom exception handlers are called
                # even if Redis is down
                pass

    def set_current_job_working_time(self, current_job_working_time: float, pipeline: Optional['Pipeline'] = None):
        """Sets the current job working time in seconds

        Args:
            current_job_working_time (float): The current job working time in seconds
            pipeline (Optional[Pipeline], optional): Pipeline to use. Defaults to None.
        """
        self.current_job_working_time = current_job_working_time
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, 'current_job_working_time', current_job_working_time)

    def set_current_job_id(self, job_id: Optional[str] = None, pipeline: Optional['Pipeline'] = None):
        """Sets the current job id.
        If `None` is used it will delete the current job key.

        Args:
            job_id (Optional[str], optional): The job id. Defaults to None.
            pipeline (Optional[Pipeline], optional): The pipeline to use. Defaults to None.
        """
        connection = pipeline if pipeline is not None else self.connection
        if job_id is None:
            connection.hdel(self.key, 'current_job')
        else:
            connection.hset(self.key, 'current_job', job_id)

    def get_current_job_id(self, pipeline: Optional['Pipeline'] = None) -> Optional[str]:
        """Retrieves the current job id.

        Args:
            pipeline (Optional[&#39;Pipeline&#39;], optional): The pipeline to use. Defaults to None.

        Returns:
            job_id (Optional[str): The job id
        """
        connection = pipeline if pipeline is not None else self.connection
        result = connection.hget(self.key, 'current_job')
        if result is None:
            return None
        return as_text(result)

    def get_current_job(self) -> Optional['Job']:
        """Returns the currently executing job instance.

        Returns:
            job (Job): The job instance.
        """
        job_id = self.get_current_job_id()
        if job_id is None:
            return None
        return self.job_class.fetch(job_id, self.connection, self.serializer)

    def set_state(self, state: str, pipeline: Optional['Pipeline'] = None):
        """Sets the worker's state.

        Args:
            state (str): The state
            pipeline (Optional[Pipeline], optional): The pipeline to use. Defaults to None.
        """
        self._state = state
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, 'state', state)

    def _set_state(self, state):
        """Raise a DeprecationWarning if ``worker.state = X`` is used"""
        warnings.warn('worker.state is deprecated, use worker.set_state() instead.', DeprecationWarning)
        self.set_state(state)

    def get_state(self) -> str:
        return self._state

    def _get_state(self):
        """Raise a DeprecationWarning if ``worker.state == X`` is used"""
        warnings.warn('worker.state is deprecated, use worker.get_state() instead.', DeprecationWarning)
        return self.get_state()

    state = property(_get_state, _set_state)

    def _start_scheduler(
        self,
        burst: bool = False,
        logging_level: Optional[str] = 'INFO',
        date_format: str = DEFAULT_LOGGING_DATE_FORMAT,
        log_format: str = DEFAULT_LOGGING_FORMAT,
    ):
        """Starts the scheduler process.
        This is specifically designed to be run by the worker when running the `work()` method.
        Instantiates the RQScheduler and tries to acquire a lock.
        If the lock is acquired, start scheduler.
        If worker is on burst mode just enqueues scheduled jobs and quits,
        otherwise, starts the scheduler in a separate process.

        Args:
            burst (bool, optional): Whether to work on burst mode. Defaults to False.
            logging_level (str, optional): Logging level to use. Defaults to "INFO".
            date_format (str, optional): Date Format. Defaults to DEFAULT_LOGGING_DATE_FORMAT.
            log_format (str, optional): Log Format. Defaults to DEFAULT_LOGGING_FORMAT.
        """
        self.scheduler = RQScheduler(
            self.queues,
            connection=self.connection,
            logging_level=logging_level if logging_level is not None else self.log.level,
            date_format=date_format,
            log_format=log_format,
            serializer=self.serializer,
        )
        self.scheduler.acquire_locks()
        if self.scheduler.acquired_locks:
            if burst:
                self.scheduler.enqueue_scheduled_jobs()
                self.scheduler.release_locks()
            else:
                self.scheduler.start()

    def register_birth(self):
        """Registers its own birth."""
        self.log.debug('Registering birth of worker %s', self.name)
        if self.connection.exists(self.key) and not self.connection.hexists(self.key, 'death'):
            msg = 'There exists an active worker named {0!r} already'
            raise ValueError(msg.format(self.name))
        key = self.key
        queues = ','.join(self.queue_names())
        with self.connection.pipeline() as p:
            p.delete(key)
            right_now = now()
            now_in_string = utcformat(right_now)
            self.birth_date = right_now

            mapping = {
                'birth': now_in_string,
                'last_heartbeat': now_in_string,
                'queues': queues,
                'pid': self.pid,
                'hostname': self.hostname,
                'ip_address': self.ip_address,
                'version': self.version,
                'python_version': self.python_version,
            }

            p.hset(key, mapping=mapping)
            worker_registration.register(self, p)
            p.expire(key, self.worker_ttl + 60)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        self.log.debug('Registering death')
        with self.connection.pipeline() as p:
            # We cannot use self.state = 'dead' here, because that would
            # rollback the pipeline
            worker_registration.unregister(self, p)
            p.hset(self.key, 'death', utcformat(now()))
            p.expire(self.key, 60)
            p.execute()

    @property
    def horse_pid(self):
        """The horse's process ID.  Only available in the worker.  Will return
        0 in the horse part of the fork.
        """
        return self._horse_pid

    def bootstrap(
        self,
        logging_level: Optional[str] = 'INFO',
        date_format: str = DEFAULT_LOGGING_DATE_FORMAT,
        log_format: str = DEFAULT_LOGGING_FORMAT,
    ):
        """Bootstraps the worker.
        Runs the basic tasks that should run when the worker actually starts working.
        Used so that new workers can focus on the work loop implementation rather
        than the full bootstrapping process.

        Args:
            logging_level (str, optional): Logging level to use. Defaults to "INFO".
            date_format (str, optional): Date Format. Defaults to DEFAULT_LOGGING_DATE_FORMAT.
            log_format (str, optional): Log Format. Defaults to DEFAULT_LOGGING_FORMAT.
        """
        setup_loghandlers(logging_level, date_format, log_format)
        self.register_birth()
        self.log.info('Worker %s started with PID %d, version %s', self.key, os.getpid(), VERSION)
        self.subscribe()
        self.set_state(WorkerStatus.STARTED)
        qnames = self.queue_names()
        self.log.info('*** Listening on %s...', green(', '.join(qnames)))

    def check_for_suspension(self, burst: bool):
        """Check to see if workers have been suspended by `rq suspend`"""
        before_state = None
        notified = False

        while not self._stop_requested and is_suspended(self.connection, self):
            if burst:
                self.log.info('Suspended in burst mode, exiting')
                self.log.info('Note: There could still be unfinished jobs on the queue')
                raise StopRequested

            if not notified:
                self.log.info('Worker suspended, run `rq resume` to resume')
                before_state = self.get_state()
                self.set_state(WorkerStatus.SUSPENDED)
                notified = True
            time.sleep(1)

        if before_state:
            self.set_state(before_state)

    def procline(self, message):
        """Changes the current procname for the process.

        This can be used to make `ps -ef` output more readable.
        """
        setprocname(f'rq:worker:{self.name}: {message}')

    def set_shutdown_requested_date(self):
        """Sets the date on which the worker received a (warm) shutdown request"""
        self.connection.hset(self.key, 'shutdown_requested_date', utcformat(self._shutdown_requested_date))

    @property
    def shutdown_requested_date(self):
        """Fetches shutdown_requested_date from Redis."""
        shutdown_requested_timestamp = self.connection.hget(self.key, 'shutdown_requested_date')
        if shutdown_requested_timestamp is not None:
            return utcparse(as_text(shutdown_requested_timestamp))

    @property
    def death_date(self):
        """Fetches death date from Redis."""
        death_timestamp = self.connection.hget(self.key, 'death')
        if death_timestamp is not None:
            return utcparse(as_text(death_timestamp))

    def run_maintenance_tasks(self):
        """
        Runs periodic maintenance tasks, these include:
        1. Check if scheduler should be started. This check should not be run
           on first run since worker.work() already calls
           `scheduler.enqueue_scheduled_jobs()` on startup.
        2. Cleaning registries

        No need to try to start scheduler on first run
        """
        if self.last_cleaned_at:
            if self.scheduler and (not self.scheduler._process or not self.scheduler._process.is_alive()):
                self.scheduler.acquire_locks(auto_start=True)
        self.clean_registries()
        Group.clean_registries(connection=self.connection)

    def _pubsub_exception_handler(self, exc: Exception, pubsub: 'PubSub', pubsub_thread: 'PubSubWorkerThread') -> None:
        """
        This exception handler allows the pubsub_thread to continue & retry to
        connect after a connection problem the same way the main worker loop
        indefinitely retries.
        redis-py internal mechanism will restore the channels subscriptions
        once the connection is re-established.
        """
        if isinstance(exc, (redis.exceptions.ConnectionError)):
            self.log.error(
                'Could not connect to Redis instance: %s Retrying in %d seconds...',
                exc,
                2,
            )
            time.sleep(2.0)
        else:
            self.log.warning('Pubsub thread exiting on %s', exc)
            raise

    def handle_payload(self, message):
        """Handle external commands"""
        self.log.debug('Received message: %s', message)
        payload = parse_payload(message)
        handle_command(self, payload)

    def subscribe(self):
        """Subscribe to this worker's channel"""
        self.log.info('Subscribing to channel %s', self.pubsub_channel_name)
        self.pubsub = self.connection.pubsub()
        self.pubsub.subscribe(**{self.pubsub_channel_name: self.handle_payload})
        self.pubsub_thread = self.pubsub.run_in_thread(
            sleep_time=60, daemon=True, exception_handler=self._pubsub_exception_handler
        )

    def get_heartbeat_ttl(self, job: 'Job') -> int:
        """Get's the TTL for the next heartbeat.

        Args:
            job (Job): The Job

        Returns:
            int: The heartbeat TTL.
        """
        if job.timeout and job.timeout > 0:
            remaining_execution_time = job.timeout - self.current_job_working_time
            return int(min(remaining_execution_time, self.job_monitoring_interval)) + 60
        else:
            return self.job_monitoring_interval + 60

    def prepare_execution(self, job: 'Job') -> Execution:
        """This method is called by the main `Worker` (not the horse) as it prepares for execution.
        Do not confuse this with worker.prepare_job_execution() which is called by the horse.
        """
        with self.connection.pipeline() as pipeline:
            heartbeat_ttl = self.get_heartbeat_ttl(job)
            self.execution = Execution.create(job, heartbeat_ttl, pipeline=pipeline)
            self.set_state(WorkerStatus.BUSY, pipeline=pipeline)
            pipeline.execute()
        return self.execution

    def unsubscribe(self):
        """Unsubscribe from pubsub channel"""
        if self.pubsub_thread:
            self.log.info('Unsubscribing from channel %s', self.pubsub_channel_name)
            self.pubsub.unsubscribe()
            self.pubsub_thread.stop()
            self.pubsub_thread.join(timeout=1)
            self.pubsub.close()

    def dequeue_job_and_maintain_ttl(
        self, timeout: Optional[int], max_idle_time: Optional[int] = None
    ) -> Optional[tuple['Job', 'Queue']]:
        """Dequeues a job while maintaining the TTL.

        Returns:
            result (Tuple[Job, Queue]): A tuple with the job and the queue.
        """
        result = None
        qnames = ','.join(self.queue_names())

        self.set_state(WorkerStatus.IDLE)
        self.procline('Listening on ' + qnames)
        self.log.debug('*** Listening on %s...', green(qnames))
        connection_wait_time = 1.0
        idle_since = now()
        idle_time_left = max_idle_time
        while True:
            try:
                self.heartbeat()

                if self.should_run_maintenance_tasks:
                    self.run_maintenance_tasks()

                if timeout is not None and idle_time_left is not None:
                    timeout = min(timeout, idle_time_left)

                self.log.debug('Dequeueing jobs on queues %s and timeout %s', green(qnames), timeout)
                result = self.queue_class.dequeue_any(
                    self._ordered_queues,
                    timeout,
                    connection=self.connection,
                    job_class=self.job_class,
                    serializer=self.serializer,
                    death_penalty_class=self.death_penalty_class,
                )
                if result is not None:
                    job, queue = result
                    self.reorder_queues(reference_queue=queue)
                    self.log.debug('Dequeued job %s from %s', blue(job.id), green(queue.name))
                    job.redis_server_version = self.get_redis_server_version()
                    if self.log_job_description:
                        self.log.info('%s: %s (%s)', green(queue.name), blue(job.description), job.id)
                    else:
                        self.log.info('%s: %s', green(queue.name), job.id)

                break
            except DequeueTimeout:
                if max_idle_time is not None:
                    idle_for = (now() - idle_since).total_seconds()
                    idle_time_left = math.ceil(max_idle_time - idle_for)
                    if idle_time_left <= 0:
                        break
            except redis.exceptions.ConnectionError as conn_err:
                self.log.error(
                    'Could not connect to Redis instance: %s Retrying in %d seconds...', conn_err, connection_wait_time
                )
                time.sleep(connection_wait_time)
                connection_wait_time *= self.exponential_backoff_factor
                connection_wait_time = min(connection_wait_time, self.max_connection_wait_time)

        self.heartbeat()
        return result

    def heartbeat(self, timeout: Optional[int] = None, pipeline: Optional['Pipeline'] = None):
        """Specifies a new worker timeout, typically by extending the
        expiration time of the worker, effectively making this a "heartbeat"
        to not expire the worker until the timeout passes.

        The next heartbeat should come before this time, or the worker will
        die (at least from the monitoring dashboards).

        If no timeout is given, the worker_ttl will be used to update
        the expiration time of the worker.

        Args:
            timeout (Optional[int]): Timeout
            pipeline (Optional[Redis]): A Redis pipeline
        """
        timeout = timeout or self.worker_ttl + 60
        connection: Union[Redis, Pipeline] = pipeline if pipeline is not None else self.connection
        connection.expire(self.key, timeout)
        connection.hset(self.key, 'last_heartbeat', utcformat(now()))
        self.log.debug('Sent heartbeat to prevent worker timeout. Next one should arrive in %s seconds.', timeout)

    def teardown(self):
        if not self.is_horse:
            if self.scheduler:
                self.stop_scheduler()
            self.register_death()
            self.unsubscribe()

    def stop_scheduler(self):
        """Ensure scheduler process is stopped
        Will send the kill signal to scheduler process,
        if there's an OSError, just passes and `join()`'s the scheduler process,
        waiting for the process to finish.
        """
        if self.scheduler._process and self.scheduler._process.pid:
            try:
                os.kill(self.scheduler._process.pid, signal.SIGTERM)
            except OSError:
                pass
            self.scheduler._process.join()

    def increment_failed_job_count(self, pipeline: Optional['Pipeline'] = None):
        """Used to keep the worker stats up to date in Redis.
        Increments the failed job count.

        Args:
            pipeline (Optional[Pipeline], optional): A Redis Pipeline. Defaults to None.
        """
        connection = pipeline if pipeline is not None else self.connection
        connection.hincrby(self.key, 'failed_job_count', 1)

    def increment_successful_job_count(self, pipeline: Optional['Pipeline'] = None):
        """Used to keep the worker stats up to date in Redis.
        Increments the successful job count.

        Args:
            pipeline (Optional[Pipeline], optional): A Redis Pipeline. Defaults to None.
        """
        connection = pipeline if pipeline is not None else self.connection
        connection.hincrby(self.key, 'successful_job_count', 1)

    def increment_total_working_time(self, job_execution_time: timedelta, pipeline: 'Pipeline'):
        """Used to keep the worker stats up to date in Redis.
        Increments the time the worker has been working for (in seconds).

        Args:
            job_execution_time (timedelta): A timedelta object.
            pipeline (Optional[Pipeline], optional): A Redis Pipeline. Defaults to None.
        """
        pipeline.hincrbyfloat(self.key, 'total_working_time', job_execution_time.total_seconds())

    def handle_exception(self, job: 'Job', *exc_info):
        """Walks the exception handler stack to delegate exception handling.
        If the job cannot be deserialized, it will raise when func_name or
        the other properties are accessed, which will stop exceptions from
        being properly logged, so we guard against it here.
        """
        self.log.debug('Handling exception for %s.', job.id)
        exc_string = ''.join(traceback.format_exception(*exc_info))
        try:
            extra = {'func': job.func_name, 'arguments': job.args, 'kwargs': job.kwargs}
            func_name = job.func_name
        except DeserializationError:
            extra = {}
            func_name = '<DeserializationError>'

        # the properties below should be safe however
        extra.update({'queue': job.origin, 'job_id': job.id})

        # func_name
        self.log.error(
            '[Job %s]: exception raised while executing (%s)\n%s', job.id, func_name, exc_string, extra=extra
        )

        for handler in self._exc_handlers:
            self.log.debug('Invoking exception handler %s', handler)
            fallthrough = handler(job, *exc_info)

            # Only handlers with explicit return values should disable further
            # exc handling, so interpret a None return value as True.
            if fallthrough is None:
                fallthrough = True

            if not fallthrough:
                break

    def push_exc_handler(self, handler_func):
        """Pushes an exception handler onto the exc handler stack."""
        self._exc_handlers.append(handler_func)

    def kill_horse(self, sig: signal.Signals = SHUTDOWN_SIGNAL):
        raise NotImplementedError()


class Worker(BaseWorker):
    @property
    def is_horse(self):
        """Returns whether or not this is the worker or the work horse."""
        return self._is_horse

    def kill_horse(self, sig: signal.Signals = SHUTDOWN_SIGNAL):
        """Kill the horse but catch "No such process" error has the horse could already be dead.

        Args:
            sig (signal.Signals, optional): _description_. Defaults to SIGKILL.
        """
        try:
            os.killpg(os.getpgid(self.horse_pid), sig)
            self.log.info('Killed horse pid %s', self.horse_pid)
        except OSError as e:
            if e.errno == errno.ESRCH:
                # "No such process" is fine with us
                self.log.debug('Horse already dead')
            else:
                raise

    def wait_for_horse(self) -> tuple[Optional[int], Optional[int], Optional['struct_rusage']]:
        """Waits for the horse process to complete.
        Uses `0` as argument as to include "any child in the process group of the current process".
        """
        pid = stat = rusage = None
        with contextlib.suppress(ChildProcessError):  # ChildProcessError: [Errno 10] No child processes
            pid, stat, rusage = os.wait4(self.horse_pid, 0)
        return pid, stat, rusage

    def request_force_stop(self, signum: int, frame: Optional[FrameType]):
        """Terminates the application (cold shutdown).

        Args:
            signum (Any): Signum
            frame (Any): Frame

        Raises:
            SystemExit: SystemExit
        """
        # When worker is run through a worker pool, it may receive duplicate signals
        # One is sent by the pool when it calls `pool.stop_worker()` and another is sent by the OS
        # when user hits Ctrl+C. In this case if we receive the second signal within 1 second,
        # we ignore it.
        if (now() - self._shutdown_requested_date) < timedelta(seconds=1):  # type: ignore
            self.log.debug('Shutdown signal ignored, received twice in less than 1 second')
            return

        self.log.warning('Cold shut down')

        # Take down the horse with the worker
        if self.horse_pid:
            self.log.debug('Taking down horse %s with me', self.horse_pid)
            self.kill_horse()
            self.wait_for_horse()
        raise SystemExit()

    def fork_work_horse(self, job: 'Job', queue: 'Queue'):
        """Spawns a work horse to perform the actual work and passes it a job.
        This is where the `fork()` actually happens.

        Args:
            job (Job): The Job that will be ran
            queue (Queue): The queue
        """
        child_pid = os.fork()
        os.environ['RQ_WORKER_ID'] = self.name
        os.environ['RQ_JOB_ID'] = job.id
        if child_pid == 0:
            os.setpgrp()
            self.main_work_horse(job, queue)
            os._exit(0)  # just in case
        else:
            self._horse_pid = child_pid
            self.procline(f'Forked {child_pid} at {time.time()}')

    def get_heartbeat_ttl(self, job: 'Job') -> int:
        """Get's the TTL for the next heartbeat.

        Args:
            job (Job): The Job

        Returns:
            int: The heartbeat TTL.
        """
        if job.timeout and job.timeout > 0:
            remaining_execution_time = job.timeout - self.current_job_working_time
            return int(min(remaining_execution_time, self.job_monitoring_interval)) + 60
        else:
            return self.job_monitoring_interval + 60

    def monitor_work_horse(self, job: 'Job', queue: 'Queue'):
        """The worker will monitor the work horse and make sure that it
        either executes successfully or the status of the job is set to
        failed

        Args:
            job (Job): _description_
            queue (Queue): _description_
        """
        retpid = ret_val = rusage = None
        job.started_at = now()
        while True:
            try:
                with self.death_penalty_class(self.job_monitoring_interval, HorseMonitorTimeoutException):
                    retpid, ret_val, rusage = self.wait_for_horse()
                break
            except HorseMonitorTimeoutException:
                # Horse has not exited yet and is still running.
                # Send a heartbeat to keep the worker alive.
                self.set_current_job_working_time((now() - job.started_at).total_seconds())

                # Kill the job from this side if something is really wrong (interpreter lock/etc).
                if job.timeout != -1 and self.current_job_working_time > (job.timeout + 60):  # type: ignore
                    self.heartbeat(self.job_monitoring_interval + 60)
                    self.kill_horse()
                    self.wait_for_horse()
                    break

                self.maintain_heartbeats(job)

            except OSError as e:
                # In case we encountered an OSError due to EINTR (which is
                # caused by a SIGINT or SIGTERM signal during
                # os.waitpid()), we simply ignore it and enter the next
                # iteration of the loop, waiting for the child to end.  In
                # any other case, this is some other unexpected OS error,
                # which we don't want to catch, so we re-raise those ones.
                if e.errno != errno.EINTR:
                    raise
                # Send a heartbeat to keep the worker alive.
                self.heartbeat()

        self.set_current_job_working_time(0)
        self._horse_pid = 0  # Set horse PID to 0, horse has finished working
        if ret_val == os.EX_OK:  # The process exited normally.
            return

        try:
            job_status = job.get_status()
        except InvalidJobOperation:
            return  # Job completed and its ttl has expired

        if self._stopped_job_id == job.id:
            # Work-horse killed deliberately
            self.log.warning('Job stopped by user, moving job to FailedJobRegistry')
            if job.stopped_callback:
                job.execute_stopped_callback(self.death_penalty_class)
            self.handle_job_failure(job, queue=queue, exc_string='Job stopped by user, work-horse terminated.')
        elif job_status not in [JobStatus.FINISHED, JobStatus.FAILED]:
            if not job.ended_at:
                job.ended_at = now()

            # Unhandled failure: move the job to the failed queue
            signal_msg = f' (signal {os.WTERMSIG(ret_val)})' if ret_val and os.WIFSIGNALED(ret_val) else ''
            exc_string = f'Work-horse terminated unexpectedly; waitpid returned {ret_val}{signal_msg}; '
            self.log.warning('Moving job to FailedJobRegistry (%s)', exc_string)

            self.handle_work_horse_killed(job, retpid, ret_val, rusage)
            self.handle_job_failure(job, queue=queue, exc_string=exc_string)

    def execute_job(self, job: 'Job', queue: 'Queue'):
        """Spawns a work horse to perform the actual work and passes it a job.
        The worker will wait for the work horse and make sure it executes
        within the given timeout bounds, or will end the work horse with
        SIGALRM.
        """
        self.prepare_execution(job)
        self.fork_work_horse(job, queue)
        self.monitor_work_horse(job, queue)
        self.set_state(WorkerStatus.IDLE)

    def maintain_heartbeats(self, job: 'Job'):
        """Updates worker, execution and job's last heartbeat fields."""
        with self.connection.pipeline() as pipeline:
            self.heartbeat(self.job_monitoring_interval + 60, pipeline=pipeline)
            ttl = int(self.get_heartbeat_ttl(job))

            # Also need to update execution's heartbeat
            self.execution.heartbeat(job.started_job_registry, ttl, pipeline=pipeline)  # type: ignore
            # After transition to job execution is complete, `job.heartbeat()` is no longer needed
            job.heartbeat(now(), ttl, pipeline=pipeline, xx=True)
            results = pipeline.execute()

            # If job was enqueued with `result_ttl=0` (job is deleted as soon as it finishes),
            # a race condition could happen where heartbeat arrives after job has been deleted,
            # leaving a job key that contains only `last_heartbeat` field.

            # job.heartbeat() uses hset() to update job's timestamp. This command returns 1 if a new
            # Redis key is created, 0 otherwise. So in this case we check the return of job's
            # heartbeat() command. If a new key was created, this means the job was already
            # deleted. In this case, we simply send another delete command to remove the key.
            # https://github.com/rq/rq/issues/1450

            if results[7] == 1:
                self.connection.delete(job.key)

    def main_work_horse(self, job: 'Job', queue: 'Queue'):
        """This is the entry point of the newly spawned work horse.
        After fork()'ing, always assure we are generating random sequences
        that are different from the worker.

        os._exit() is the way to exit from childs after a fork(), in
        contrast to the regular sys.exit()
        """
        random.seed()
        self.setup_work_horse_signals()
        self._is_horse = True
        self.log = logger
        try:
            self.perform_job(job, queue)
        except:  # noqa
            os._exit(1)
        os._exit(0)

    def setup_work_horse_signals(self):
        """Setup signal handing for the newly spawned work horse

        Always ignore Ctrl+C in the work horse, as it might abort the
        currently running job.

        The main worker catches the Ctrl+C and requests graceful shutdown
        after the current work is done.  When cold shutdown is requested, it
        kills the current job anyway.
        """
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def prepare_job_execution(self, job: 'Job', remove_from_intermediate_queue: bool = False) -> None:
        """Performs misc bookkeeping like updating states prior to
        job execution.
        """
        self.log.debug('Preparing for execution of Job ID %s', job.id)
        with self.connection.pipeline() as pipeline:
            self.set_current_job_id(job.id, pipeline=pipeline)
            self.set_current_job_working_time(0, pipeline=pipeline)

            heartbeat_ttl = self.get_heartbeat_ttl(job)
            self.heartbeat(heartbeat_ttl, pipeline=pipeline)
            job.heartbeat(now(), heartbeat_ttl, pipeline=pipeline)

            job.prepare_for_execution(self.name, pipeline=pipeline)
            if remove_from_intermediate_queue:
                from .queue import Queue

                queue = Queue(job.origin, connection=self.connection)
                pipeline.lrem(queue.intermediate_queue_key, 1, job.id)
            pipeline.execute()
            self.log.debug('Job preparation finished.')

        msg = 'Processing {0} from {1} since {2}'
        self.procline(msg.format(job.func_name, job.origin, time.time()))

    def handle_job_retry(self, job: 'Job', queue: 'Queue', retry: Retry, started_job_registry: StartedJobRegistry):
        """Handles the retry of certain job.
        It will remove the job from the `StartedJobRegistry` and requeue or reschedule the job.

        Args:
            job (Job): The job that will be retried.
            queue (Queue): The queue
            started_job_registry (StartedJobRegistry): The started registry
        """
        self.log.debug('Handling retry of job %s', job.id)

        # Check if job has exceeded max retries
        if job.number_of_retries and job.number_of_retries >= retry.max:
            # If max retries exceeded, treat as failure
            self.log.warning('Job %s has exceeded maximum retry attempts (%d)', job.id, retry.max)
            exc_string = f'Job failed after {retry.max} retry attempts'
            self.handle_job_failure(job, queue=queue, exc_string=exc_string)
            return

        # Calculate retry interval based on retry count
        retry_interval = Retry.get_interval(job.number_of_retries or 0, retry.intervals)

        with self.connection.pipeline() as pipeline:
            self.increment_failed_job_count(pipeline=pipeline)
            self.increment_total_working_time(job.ended_at - job.started_at, pipeline)  # type: ignore

            if retry_interval > 0:
                # Schedule job for later if there's an interval
                scheduled_time = now() + timedelta(seconds=retry_interval)
                job.set_status(JobStatus.SCHEDULED, pipeline=pipeline)
                queue.schedule_job(job, scheduled_time, pipeline=pipeline)
                self.log.debug(
                    'Job %s: scheduled for retry at %s, %s attempts remaining',
                    job.id,
                    scheduled_time,
                    retry.max - (job.number_of_retries or 0),
                )
            else:
                self.log.debug(
                    'Job %s: enqueued for retry, %s attempts remaining',
                    job.id,
                    retry.max - (job.number_of_retries or 0),
                )
                job._handle_retry_result(queue=queue, pipeline=pipeline)

            self.cleanup_execution(job, pipeline=pipeline)

            pipeline.execute()

            self.log.debug('Finished handling retry of job %s', job.id)

    def handle_job_success(self, job: 'Job', queue: 'Queue', started_job_registry: StartedJobRegistry):
        """Handles the successful execution of certain job.
        It will remove the job from the `StartedJobRegistry`, adding it to the `SuccessfulJobRegistry`,
        and run a few maintenance tasks including:
            - Resting the current job ID
            - Enqueue dependents
            - Incrementing the job count and working time
            - Handling of the job successful execution
            - If job.repeats_left > 0, it will be scheduled for the next execution.

        Runs within a loop with the `watch` method so that protects interactions
        with dependents keys.

        Args:
            job (Job): The job that was successful.
            queue (Queue): The queue
            started_job_registry (StartedJobRegistry): The started registry
        """
        self.log.debug('Handling successful execution of job %s', job.id)

        with self.connection.pipeline() as pipeline:
            while True:
                try:
                    # if dependencies are inserted after enqueue_dependents
                    # a WatchError is thrown by execute()
                    pipeline.watch(job.dependents_key)
                    # enqueue_dependents might call multi() on the pipeline
                    self.log.debug('Enqueueing dependents of job %s', job.id)
                    queue.enqueue_dependents(job, pipeline=pipeline)

                    if not pipeline.explicit_transaction:
                        # enqueue_dependents didn't call multi after all!
                        # We have to do it ourselves to make sure everything runs in a transaction
                        self.log.debug('Calling multi() on pipeline for job %s', job.id)
                        pipeline.multi()

                    self.increment_successful_job_count(pipeline=pipeline)
                    self.increment_total_working_time(job.ended_at - job.started_at, pipeline)  # type: ignore

                    result_ttl = job.get_result_ttl(self.default_result_ttl)
                    if result_ttl != 0:
                        self.log.debug("Saving job %s's successful execution result", job.id)
                        job._handle_success(result_ttl, pipeline=pipeline)

                    if job.repeats_left is not None and job.repeats_left > 0:
                        from .repeat import Repeat

                        self.log.info('Job %s scheduled to repeat (%s left)', job.id, job.repeats_left)
                        Repeat.schedule(job, queue, pipeline=pipeline)
                    else:
                        job.cleanup(result_ttl, pipeline=pipeline, remove_from_queue=False)

                    self.log.debug('Cleaning up execution of job %s', job.id)
                    self.cleanup_execution(job, pipeline=pipeline)

                    pipeline.execute()

                    assert job.started_at
                    assert job.ended_at
                    time_taken = job.ended_at - job.started_at

                    if self.log_job_description:
                        self.log.info(
                            'Successfully completed %s job in %ss on worker %s', job.description, time_taken, self.name
                        )
                    else:
                        self.log.info(
                            'Successfully completed job %s in %ss on worker %s', job.id, time_taken, self.name
                        )

                    self.log.debug('Finished handling successful execution of job %s', job.id)
                    break
                except redis.exceptions.WatchError:
                    continue

    def handle_execution_ended(self, job: 'Job', queue: 'Queue', heartbeat_ttl: int):
        """Called after job has finished execution."""
        job.ended_at = now()
        job.heartbeat(now(), heartbeat_ttl)

    def perform_job(self, job: 'Job', queue: 'Queue') -> bool:
        """Performs the actual work of a job.  Will/should only be called
        inside the work horse's process.

        Args:
            job (Job): The Job
            queue (Queue): The Queue

        Returns:
            bool: True after finished.
        """
        started_job_registry = queue.started_job_registry
        self.log.debug('Started Job Registry set.')

        try:
            remove_from_intermediate_queue = len(self.queues) == 1
            self.prepare_job_execution(job, remove_from_intermediate_queue)

            job.started_at = now()
            timeout = job.timeout or self.queue_class.DEFAULT_TIMEOUT
            with self.death_penalty_class(timeout, JobTimeoutException, job_id=job.id):
                self.log.debug('Performing Job %s ...', job.id)
                return_value = job.perform()
                self.log.debug('Finished performing Job %s', job.id)

            self.handle_execution_ended(job, queue, job.success_callback_timeout)
            # Pickle the result in the same try-except block since we need
            # to use the same exc handling when pickling fails
            job._result = return_value

            if isinstance(return_value, Retry):
                # Retry the job
                self.log.debug('Job %s returns a Retry object', job.id)
                self.handle_job_retry(
                    job=job, queue=queue, retry=return_value, started_job_registry=started_job_registry
                )
                return True
            else:
                job.execute_success_callback(self.death_penalty_class, return_value)
                self.handle_job_success(job=job, queue=queue, started_job_registry=started_job_registry)

        except:  # NOQA
            self.log.debug('Job %s raised an exception.', job.id)
            job._status = JobStatus.FAILED

            self.handle_execution_ended(job, queue, job.failure_callback_timeout)
            exc_info = sys.exc_info()
            exc_string = ''.join(traceback.format_exception(*exc_info))

            try:
                job.execute_failure_callback(self.death_penalty_class, *exc_info)
            except:  # noqa
                exc_info = sys.exc_info()
                exc_string = ''.join(traceback.format_exception(*exc_info))

            # TODO: reversing the order of handle_job_failure() and handle_exception()
            # causes Sentry test to fail
            self.handle_exception(job, *exc_info)
            self.handle_job_failure(
                job=job, exc_string=exc_string, queue=queue, started_job_registry=started_job_registry
            )

            return False

        self.log.info('%s: %s (%s)', green(job.origin), blue('Job OK'), job.id)
        if return_value is not None:
            self.log.debug('Result: %r', yellow(str(return_value)))

        if self.log_result_lifespan:
            result_ttl = job.get_result_ttl(self.default_result_ttl)
            if result_ttl == 0:
                self.log.info('Result discarded immediately')
            elif result_ttl > 0:
                self.log.info('Result is kept for %s seconds', result_ttl)
            else:
                self.log.info('Result will never expire, clean up result key manually')

        return True

    def pop_exc_handler(self):
        """Pops the latest exception handler off of the exc handler stack."""
        return self._exc_handlers.pop()

    def handle_work_horse_killed(self, job, retpid, ret_val, rusage):
        if self._work_horse_killed_handler is None:
            return

        self._work_horse_killed_handler(job, retpid, ret_val, rusage)

    def __eq__(self, other):
        """Equality does not take the database/connection into account"""
        if not isinstance(other, self.__class__):
            raise TypeError('Cannot compare workers to other types (of workers)')
        return self.name == other.name

    def __hash__(self):
        """The hash does not take the database/connection into account"""
        return hash(self.name)


class SpawnWorker(Worker):
    """Worker implementation that uses os.spawn() instead of os.fork().
    This implementation is intended for environments where `os.fork()` is not available.
    """

    def fork_work_horse(self, job: 'Job', queue: 'Queue'):
        """Spawns a work horse to perform the actual work using os.spawn()."""
        os.environ['RQ_WORKER_ID'] = self.name
        os.environ['RQ_JOB_ID'] = job.id

        redis_kwargs = self.connection.connection_pool.connection_kwargs
        if redis_kwargs.get('retry'):
            # Remove retry from connection kwargs to avoid issues with os.spawnv
            del redis_kwargs['retry']

        child_pid = os.spawnv(
            os.P_NOWAIT,
            sys.executable,
            [
                sys.executable,
                '-c',
                f"""
import os
import sys
from redis import Redis
from rq import Worker, Queue
from rq.job import Job

# Recreate worker instance
redis = Redis(**{redis_kwargs})
worker = Worker.find_by_key("{self.key}", connection=redis)
if not worker:
    sys.exit(1)

# Reconstruct job and queue
job = Job.fetch("{job.id}", connection=worker.connection)
queue = Queue("{queue.name}", connection=worker.connection)

# Set up work horse
os.setpgrp()
worker._is_horse = True
worker.main_work_horse(job, queue)
""",
            ],
        )

        self._horse_pid = child_pid
        self.procline(f'Spawned {child_pid} at {time.time()}')


class SimpleWorker(Worker):
    def execute_job(self, job: 'Job', queue: 'Queue'):
        """Execute job in same thread/process, do not fork()"""
        self.prepare_execution(job)
        self.perform_job(job, queue)
        self.set_state(WorkerStatus.IDLE)

    def get_heartbeat_ttl(self, job: 'Job') -> int:
        """-1" means that jobs never timeout. In this case, we should _not_ do -1 + 60 = 59.
        We should just stick to DEFAULT_WORKER_TTL.

        Args:
            job (Job): The Job

        Returns:
            ttl (int): TTL
        """
        if job.timeout == -1:
            return DEFAULT_WORKER_TTL
        else:
            return int(job.timeout or DEFAULT_WORKER_TTL) + 60


class HerokuWorker(Worker):
    """
    Modified version of rq worker which:
    * stops work horses getting killed with SIGTERM
    * sends SIGRTMIN to work horses on SIGTERM to the main process which in turn
    causes the horse to crash `imminent_shutdown_delay` seconds later
    """

    imminent_shutdown_delay = 6
    frame_properties = ['f_code', 'f_lasti', 'f_lineno', 'f_locals', 'f_trace']

    def setup_work_horse_signals(self):
        """Modified to ignore SIGINT and SIGTERM and only handle SIGRTMIN"""
        signal.signal(signal.SIGRTMIN, self.request_stop_sigrtmin)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def handle_warm_shutdown_request(self):
        """If horse is alive send it SIGRTMIN"""
        if self.horse_pid != 0:
            self.log.info('Worker %s: warm shut down requested, sending horse SIGRTMIN signal', self.key)
            self.kill_horse(sig=signal.SIGRTMIN)
        else:
            self.log.warning('Warm shut down requested, no horse found')

    def request_stop_sigrtmin(self, signum, frame):
        if self.imminent_shutdown_delay == 0:
            self.log.warning('Imminent shutdown, raising ShutDownImminentException immediately')
            self.request_force_stop_sigrtmin(signum, frame)
        else:
            self.log.warning(
                'Imminent shutdown, raising ShutDownImminentException in %d seconds', self.imminent_shutdown_delay
            )
            signal.signal(signal.SIGRTMIN, self.request_force_stop_sigrtmin)
            signal.signal(signal.SIGALRM, self.request_force_stop_sigrtmin)
            signal.alarm(self.imminent_shutdown_delay)

    def request_force_stop_sigrtmin(self, signum, frame):
        info = dict((attr, getattr(frame, attr)) for attr in self.frame_properties)
        self.log.warning('raising ShutDownImminentException to cancel job...')
        raise ShutDownImminentException('shut down imminent (signal: %s)' % signal_name(signum), info)


class RoundRobinWorker(Worker):
    """
    Modified version of Worker that dequeues jobs from the queues using a round-robin strategy.
    """

    def reorder_queues(self, reference_queue):
        pos = self._ordered_queues.index(reference_queue)
        self._ordered_queues = self._ordered_queues[pos + 1 :] + self._ordered_queues[: pos + 1]


class RandomWorker(Worker):
    """
    Modified version of Worker that dequeues jobs from the queues using a random strategy.
    """

    def reorder_queues(self, reference_queue):
        shuffle(self._ordered_queues)
