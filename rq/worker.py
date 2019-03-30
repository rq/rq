# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import errno
import logging
import os
import random
import signal
import socket
import sys
import time
import traceback
import warnings
from datetime import timedelta
from uuid import uuid4

try:
    from signal import SIGKILL
except ImportError:
    from signal import SIGTERM as SIGKILL

from redis import WatchError

from . import worker_registration
from .compat import PY2, as_text, string_types, text_type
from .connections import get_current_connection, push_connection, pop_connection

from .defaults import (DEFAULT_RESULT_TTL,
                       DEFAULT_WORKER_TTL, DEFAULT_JOB_MONITORING_INTERVAL,
                       DEFAULT_LOGGING_FORMAT, DEFAULT_LOGGING_DATE_FORMAT)
from .exceptions import DequeueTimeout, ShutDownImminentException
from .job import Job, JobStatus
from .logutils import setup_loghandlers
from .queue import Queue
from .registry import (FailedJobRegistry, FinishedJobRegistry,
                       StartedJobRegistry, clean_registries)
from .suspension import is_suspended
from .timeouts import JobTimeoutException, HorseMonitorTimeoutException, UnixSignalDeathPenalty
from .utils import (backend_class, ensure_list, enum,
                    make_colorizer, utcformat, utcnow, utcparse)
from .version import VERSION
from .worker_registration import clean_worker_registry, get_keys

try:
    from procname import setprocname
except ImportError:
    def setprocname(*args, **kwargs):  # noqa
        pass

green = make_colorizer('darkgreen')
yellow = make_colorizer('darkyellow')
blue = make_colorizer('darkblue')


logger = logging.getLogger(__name__)


class StopRequested(Exception):
    pass


def compact(l):
    return [x for x in l if x is not None]


_signames = dict((getattr(signal, signame), signame)
                 for signame in dir(signal)
                 if signame.startswith('SIG') and '_' not in signame)


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


WorkerStatus = enum(
    'WorkerStatus',
    STARTED='started',
    SUSPENDED='suspended',
    BUSY='busy',
    IDLE='idle'
)


class Worker(object):
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

    @classmethod
    def all(cls, connection=None, job_class=None, queue_class=None, queue=None):
        """Returns an iterable of all Workers.
        """
        if queue:
            connection = queue.connection
        elif connection is None:
            connection = get_current_connection()

        worker_keys = get_keys(queue=queue, connection=connection)
        workers = [cls.find_by_key(as_text(key),
                                   connection=connection,
                                   job_class=job_class,
                                   queue_class=queue_class)
                   for key in worker_keys]
        return compact(workers)

    @classmethod
    def all_keys(cls, connection=None, queue=None):
        return [as_text(key)
                for key in get_keys(queue=queue, connection=connection)]

    @classmethod
    def count(cls, connection=None, queue=None):
        """Returns the number of workers by queue or connection"""
        return len(get_keys(queue=queue, connection=connection))

    @classmethod
    def find_by_key(cls, worker_key, connection=None, job_class=None,
                    queue_class=None):
        """Returns a Worker instance, based on the naming conventions for
        naming the internal Redis keys.  Can be used to reverse-lookup Workers
        by their Redis keys.
        """
        prefix = cls.redis_worker_namespace_prefix
        if not worker_key.startswith(prefix):
            raise ValueError('Not a valid RQ worker key: %s' % worker_key)

        if connection is None:
            connection = get_current_connection()
        if not connection.exists(worker_key):
            connection.srem(cls.redis_workers_keys, worker_key)
            return None

        name = worker_key[len(prefix):]
        worker = cls([], name, connection=connection, job_class=job_class,
                     queue_class=queue_class, prepare_for_work=False)

        worker.refresh()

        return worker

    def __init__(self, queues, name=None, default_result_ttl=DEFAULT_RESULT_TTL,
                 connection=None, exc_handler=None, exception_handlers=None,
                 default_worker_ttl=DEFAULT_WORKER_TTL, job_class=None,
                 queue_class=None, log_job_description=True,
                 job_monitoring_interval=DEFAULT_JOB_MONITORING_INTERVAL,
                 disable_default_exception_handler=False,
                 prepare_for_work=True):  # noqa
        if connection is None:
            connection = get_current_connection()
        self.connection = connection

        if prepare_for_work:
            self.hostname = socket.gethostname()
            self.pid = os.getpid()
        else:
            self.hostname = None
            self.pid = None

        self.job_class = backend_class(self, 'job_class', override=job_class)
        self.queue_class = backend_class(self, 'queue_class', override=queue_class)

        queues = [self.queue_class(name=q,
                                   connection=connection,
                                   job_class=self.job_class)
                  if isinstance(q, string_types) else q
                  for q in ensure_list(queues)]

        self.name = name or uuid4().hex
        self.queues = queues
        self.validate_queues()
        self._exc_handlers = []

        self.default_result_ttl = default_result_ttl
        self.default_worker_ttl = default_worker_ttl
        self.job_monitoring_interval = job_monitoring_interval

        self._state = 'starting'
        self._is_horse = False
        self._horse_pid = 0
        self._stop_requested = False
        self.log = logger
        self.log_job_description = log_job_description
        self.last_cleaned_at = None
        self.successful_job_count = 0
        self.failed_job_count = 0
        self.total_working_time = 0
        self.birth_date = None

        self.disable_default_exception_handler = disable_default_exception_handler

        if isinstance(exception_handlers, list):
            for handler in exception_handlers:
                self.push_exc_handler(handler)
        elif exception_handlers is not None:
            self.push_exc_handler(exception_handlers)

    def validate_queues(self):
        """Sanity check for the given queues."""
        for queue in self.queues:
            if not isinstance(queue, self.queue_class):
                raise TypeError('{0} is not of type {1} or string types'.format(queue, self.queue_class))

    def queue_names(self):
        """Returns the queue names of this worker's queues."""
        return list(map(lambda q: q.name, self.queues))

    def queue_keys(self):
        """Returns the Redis keys representing this worker's queues."""
        return list(map(lambda q: q.key, self.queues))

    @property
    def key(self):
        """Returns the worker's Redis hash key."""
        return self.redis_worker_namespace_prefix + self.name

    @property
    def horse_pid(self):
        """The horse's process ID.  Only available in the worker.  Will return
        0 in the horse part of the fork.
        """
        return self._horse_pid

    @property
    def is_horse(self):
        """Returns whether or not this is the worker or the work horse."""
        return self._is_horse

    def procline(self, message):
        """Changes the current procname for the process.

        This can be used to make `ps -ef` output more readable.
        """
        setprocname('rq: {0}'.format(message))

    def register_birth(self):
        """Registers its own birth."""
        self.log.debug('Registering birth of worker %s', self.name)
        if self.connection.exists(self.key) and \
                not self.connection.hexists(self.key, 'death'):
            msg = 'There exists an active worker named {0!r} already'
            raise ValueError(msg.format(self.name))
        key = self.key
        queues = ','.join(self.queue_names())
        with self.connection.pipeline() as p:
            p.delete(key)
            now = utcnow()
            now_in_string = utcformat(utcnow())
            self.birth_date = now
            p.hset(key, 'birth', now_in_string)
            p.hset(key, 'last_heartbeat', now_in_string)
            p.hset(key, 'queues', queues)
            p.hset(key, 'pid', self.pid)
            p.hset(key, 'hostname', self.hostname)
            worker_registration.register(self, p)
            p.expire(key, self.default_worker_ttl)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        self.log.debug('Registering death')
        with self.connection.pipeline() as p:
            # We cannot use self.state = 'dead' here, because that would
            # rollback the pipeline
            worker_registration.unregister(self, p)
            p.hset(self.key, 'death', utcformat(utcnow()))
            p.expire(self.key, 60)
            p.execute()

    def set_shutdown_requested_date(self):
        """Sets the date on which the worker received a (warm) shutdown request"""
        self.connection.hset(self.key, 'shutdown_requested_date', utcformat(utcnow()))

    # @property
    # def birth_date(self):
    #     """Fetches birth date from Redis."""
    #     birth_timestamp = self.connection.hget(self.key, 'birth')
    #     if birth_timestamp is not None:
    #         return utcparse(as_text(birth_timestamp))

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

    def set_state(self, state, pipeline=None):
        self._state = state
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, 'state', state)

    def _set_state(self, state):
        """Raise a DeprecationWarning if ``worker.state = X`` is used"""
        warnings.warn(
            "worker.state is deprecated, use worker.set_state() instead.",
            DeprecationWarning
        )
        self.set_state(state)

    def get_state(self):
        return self._state

    def _get_state(self):
        """Raise a DeprecationWarning if ``worker.state == X`` is used"""
        warnings.warn(
            "worker.state is deprecated, use worker.get_state() instead.",
            DeprecationWarning
        )
        return self.get_state()

    state = property(_get_state, _set_state)

    def set_current_job_id(self, job_id, pipeline=None):
        connection = pipeline if pipeline is not None else self.connection

        if job_id is None:
            connection.hdel(self.key, 'current_job')
        else:
            connection.hset(self.key, 'current_job', job_id)

    def get_current_job_id(self, pipeline=None):
        connection = pipeline if pipeline is not None else self.connection
        return as_text(connection.hget(self.key, 'current_job'))

    def get_current_job(self):
        """Returns the job id of the currently executing job."""
        job_id = self.get_current_job_id()

        if job_id is None:
            return None

        return self.job_class.fetch(job_id, self.connection)

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.
        """

        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def kill_horse(self, sig=SIGKILL):
        """
        Kill the horse but catch "No such process" error has the horse could already be dead.
        """
        try:
            os.kill(self.horse_pid, sig)
        except OSError as e:
            if e.errno == errno.ESRCH:
                # "No such process" is fine with us
                self.log.debug('Horse already dead')
            else:
                raise

    def request_force_stop(self, signum, frame):
        """Terminates the application (cold shutdown).
        """
        self.log.warning('Cold shut down')

        # Take down the horse with the worker
        if self.horse_pid:
            self.log.debug('Taking down horse %s with me', self.horse_pid)
            self.kill_horse()
        raise SystemExit()

    def request_stop(self, signum, frame):
        """Stops the current worker loop but waits for child processes to
        end gracefully (warm shutdown).
        """
        self.log.debug('Got signal %s', signal_name(signum))

        signal.signal(signal.SIGINT, self.request_force_stop)
        signal.signal(signal.SIGTERM, self.request_force_stop)

        self.handle_warm_shutdown_request()

        # If shutdown is requested in the middle of a job, wait until
        # finish before shutting down and save the request in redis
        if self.get_state() == WorkerStatus.BUSY:
            self._stop_requested = True
            self.set_shutdown_requested_date()
            self.log.debug('Stopping after current horse is finished. '
                           'Press Ctrl+C again for a cold shutdown.')
        else:
            raise StopRequested()

    def handle_warm_shutdown_request(self):
        self.log.info('Warm shut down requested')

    def check_for_suspension(self, burst):
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

    def work(self, burst=False, logging_level="INFO", date_format=DEFAULT_LOGGING_DATE_FORMAT,
             log_format=DEFAULT_LOGGING_FORMAT):
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.
        """
        setup_loghandlers(logging_level, date_format, log_format)
        self._install_signal_handlers()
        did_perform_work = False
        self.register_birth()
        self.log.info("RQ worker %r started, version %s", self.key, VERSION)
        self.set_state(WorkerStatus.STARTED)
        qnames = self.queue_names()
        self.log.info('*** Listening on %s...', green(', '.join(qnames)))

        try:
            while True:
                try:
                    self.check_for_suspension(burst)

                    if self.should_run_maintenance_tasks:
                        self.clean_registries()

                    if self._stop_requested:
                        self.log.info('Stopping on request')
                        break

                    timeout = None if burst else max(1, self.default_worker_ttl - 15)

                    result = self.dequeue_job_and_maintain_ttl(timeout)
                    if result is None:
                        if burst:
                            self.log.info("RQ worker %r done, quitting", self.key)
                        break

                    job, queue = result
                    self.execute_job(job, queue)
                    self.heartbeat()

                    did_perform_work = True

                except StopRequested:
                    break

                except SystemExit:
                    # Cold shutdown detected
                    raise

                except:  # noqa
                    self.log.error(
                        'Worker %s: found an unhandled exception, quitting...',
                        self.name, exc_info=True
                    )
                    break
        finally:
            if not self.is_horse:
                self.register_death()
        return did_perform_work

    def dequeue_job_and_maintain_ttl(self, timeout):
        result = None
        qnames = ','.join(self.queue_names())

        self.set_state(WorkerStatus.IDLE)
        self.procline('Listening on ' + qnames)
        self.log.debug('*** Listening on %s...', green(qnames))

        while True:
            self.heartbeat()

            try:
                result = self.queue_class.dequeue_any(self.queues, timeout,
                                                      connection=self.connection,
                                                      job_class=self.job_class)
                if result is not None:

                    job, queue = result
                    if self.log_job_description:
                        self.log.info(
                            '%s: %s (%s)', green(queue.name),
                            blue(job.description), job.id)
                    else:
                        self.log.info('%s:%s', green(queue.name), job.id)

                break
            except DequeueTimeout:
                pass

        self.heartbeat()
        return result

    def heartbeat(self, timeout=None, pipeline=None):
        """Specifies a new worker timeout, typically by extending the
        expiration time of the worker, effectively making this a "heartbeat"
        to not expire the worker until the timeout passes.

        The next heartbeat should come before this time, or the worker will
        die (at least from the monitoring dashboards).

        If no timeout is given, the default_worker_ttl will be used to update
        the expiration time of the worker.
        """
        timeout = timeout or self.default_worker_ttl
        connection = pipeline if pipeline is not None else self.connection
        connection.expire(self.key, timeout)
        connection.hset(self.key, 'last_heartbeat', utcformat(utcnow()))
        self.log.debug('Sent heartbeat to prevent worker timeout. '
                       'Next one should arrive within %s seconds.', timeout)

    def refresh(self):
        data = self.connection.hmget(
            self.key, 'queues', 'state', 'current_job', 'last_heartbeat',
            'birth', 'failed_job_count', 'successful_job_count',
            'total_working_time', 'hostname', 'pid'
        )
        (queues, state, job_id, last_heartbeat, birth, failed_job_count,
         successful_job_count, total_working_time, hostname, pid) = data
        queues = as_text(queues)
        self.hostname = hostname
        self.pid = int(pid) if pid else None
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

        if queues:
            self.queues = [self.queue_class(queue,
                                            connection=self.connection,
                                            job_class=self.job_class)
                           for queue in queues.split(',')]

    def increment_failed_job_count(self, pipeline=None):
        connection = pipeline if pipeline is not None else self.connection
        connection.hincrby(self.key, 'failed_job_count', 1)

    def increment_successful_job_count(self, pipeline=None):
        connection = pipeline if pipeline is not None else self.connection
        connection.hincrby(self.key, 'successful_job_count', 1)

    def increment_total_working_time(self, job_execution_time, pipeline):
        pipeline.hincrbyfloat(self.key, 'total_working_time',
                              job_execution_time.total_seconds())

    def fork_work_horse(self, job, queue):
        """Spawns a work horse to perform the actual work and passes it a job.
        """
        child_pid = os.fork()
        os.environ['RQ_WORKER_ID'] = self.name
        os.environ['RQ_JOB_ID'] = job.id
        if child_pid == 0:
            self.main_work_horse(job, queue)
        else:
            self._horse_pid = child_pid
            self.procline('Forked {0} at {1}'.format(child_pid, time.time()))

    def monitor_work_horse(self, job):
        """The worker will monitor the work horse and make sure that it
        either executes successfully or the status of the job is set to
        failed
        """
        while True:
            try:
                with UnixSignalDeathPenalty(self.job_monitoring_interval, HorseMonitorTimeoutException):
                    retpid, ret_val = os.waitpid(self._horse_pid, 0)
                break
            except HorseMonitorTimeoutException:
                # Horse has not exited yet and is still running.
                # Send a heartbeat to keep the worker alive.
                self.heartbeat(self.job_monitoring_interval + 5)
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

        if ret_val == os.EX_OK:  # The process exited normally.
            return
        job_status = job.get_status()
        if job_status is None:  # Job completed and its ttl has expired
            return
        if job_status not in [JobStatus.FINISHED, JobStatus.FAILED]:

            if not job.ended_at:
                job.ended_at = utcnow()

            # Unhandled failure: move the job to the failed queue
            self.log.warning((
                'Moving job to FailedJobRegistry '
                '(work-horse terminated unexpectedly; waitpid returned {})'
            ).format(ret_val))

            exc_string = "Work-horse process was terminated unexpectedly " + "(waitpid returned %s)" % ret_val
            self.handle_job_failure(
                job,
                exc_string="Work-horse process was terminated unexpectedly "
                           "(waitpid returned %s)" % ret_val
            )

    def execute_job(self, job, queue):
        """Spawns a work horse to perform the actual work and passes it a job.
        The worker will wait for the work horse and make sure it executes
        within the given timeout bounds, or will end the work horse with
        SIGALRM.
        """
        self.set_state(WorkerStatus.BUSY)
        self.fork_work_horse(job, queue)
        self.monitor_work_horse(job)
        self.set_state(WorkerStatus.IDLE)

    def main_work_horse(self, job, queue):
        """This is the entry point of the newly spawned work horse."""
        # After fork()'ing, always assure we are generating random sequences
        # that are different from the worker.
        random.seed()

        try:
            self.setup_work_horse_signals()
            self._is_horse = True
            self.log = logger
            self.perform_job(job, queue)
        except Exception as e:  # noqa
            # Horse does not terminate properly
            raise e
            os._exit(1)

        # os._exit() is the way to exit from childs after a fork(), in
        # constrast to the regular sys.exit()
        os._exit(0)

    def setup_work_horse_signals(self):
        """Setup signal handing for the newly spawned work horse."""
        # Always ignore Ctrl+C in the work horse, as it might abort the
        # currently running job.
        # The main worker catches the Ctrl+C and requests graceful shutdown
        # after the current work is done.  When cold shutdown is requested, it
        # kills the current job anyway.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def prepare_job_execution(self, job, heartbeat_ttl=None):
        """Performs misc bookkeeping like updating states prior to
        job execution.
        """
        timeout = (job.timeout or 180) + 60

        if heartbeat_ttl is None:
            heartbeat_ttl = self.job_monitoring_interval + 5

        with self.connection.pipeline() as pipeline:
            self.set_state(WorkerStatus.BUSY, pipeline=pipeline)
            self.set_current_job_id(job.id, pipeline=pipeline)
            self.heartbeat(heartbeat_ttl, pipeline=pipeline)
            registry = StartedJobRegistry(job.origin, self.connection,
                                          job_class=self.job_class)
            registry.add(job, timeout, pipeline=pipeline)
            job.set_status(JobStatus.STARTED, pipeline=pipeline)
            pipeline.hset(job.key, 'started_at', utcformat(utcnow()))
            pipeline.execute()

        msg = 'Processing {0} from {1} since {2}'
        self.procline(msg.format(job.func_name, job.origin, time.time()))

    def handle_job_failure(self, job, started_job_registry=None,
                           exc_string=''):
        """Handles the failure or an executing job by:
            1. Setting the job status to failed
            2. Removing the job from StartedJobRegistry
            3. Setting the workers current job to None
            4. Add the job to FailedJobRegistry
        """
        with self.connection.pipeline() as pipeline:
            if started_job_registry is None:
                started_job_registry = StartedJobRegistry(
                    job.origin,
                    self.connection,
                    job_class=self.job_class
                )
            job.set_status(JobStatus.FAILED, pipeline=pipeline)
            started_job_registry.remove(job, pipeline=pipeline)

            if not self.disable_default_exception_handler:
                failed_job_registry = FailedJobRegistry(job.origin, job.connection,
                                                        job_class=self.job_class)
                failed_job_registry.add(job, ttl=job.failure_ttl,
                                        exc_string=exc_string, pipeline=pipeline)

            self.set_current_job_id(None, pipeline=pipeline)
            self.increment_failed_job_count(pipeline)
            if job.started_at and job.ended_at:
                self.increment_total_working_time(
                    job.ended_at - job.started_at,
                    pipeline
                )

            try:
                pipeline.execute()
            except Exception:
                # Ensure that custom exception handlers are called
                # even if Redis is down
                pass

    def handle_job_success(self, job, queue, started_job_registry):

        with self.connection.pipeline() as pipeline:
            while True:
                try:
                    # if dependencies are inserted after enqueue_dependents
                    # a WatchError is thrown by execute()
                    pipeline.watch(job.dependents_key)
                    # enqueue_dependents calls multi() on the pipeline!
                    queue.enqueue_dependents(job, pipeline=pipeline)

                    self.set_current_job_id(None, pipeline=pipeline)
                    self.increment_successful_job_count(pipeline=pipeline)
                    self.increment_total_working_time(
                        job.ended_at - job.started_at, pipeline
                    )

                    result_ttl = job.get_result_ttl(self.default_result_ttl)
                    if result_ttl != 0:
                        job.set_status(JobStatus.FINISHED, pipeline=pipeline)
                        # Don't clobber the user's meta dictionary!
                        job.save(pipeline=pipeline, include_meta=False)

                        finished_job_registry = FinishedJobRegistry(job.origin,
                                                                    self.connection,
                                                                    job_class=self.job_class)
                        finished_job_registry.add(job, result_ttl, pipeline)

                    job.cleanup(result_ttl, pipeline=pipeline,
                                remove_from_queue=False)
                    started_job_registry.remove(job, pipeline=pipeline)

                    pipeline.execute()
                    break
                except WatchError:
                    continue

    def perform_job(self, job, queue, heartbeat_ttl=None):
        """Performs the actual work of a job.  Will/should only be called
        inside the work horse's process.
        """
        self.prepare_job_execution(job, heartbeat_ttl)
        push_connection(self.connection)

        started_job_registry = StartedJobRegistry(job.origin,
                                                  self.connection,
                                                  job_class=self.job_class)

        try:
            job.started_at = utcnow()
            timeout = job.timeout or self.queue_class.DEFAULT_TIMEOUT
            with self.death_penalty_class(timeout, JobTimeoutException, job_id=job.id):
                rv = job.perform()

            job.ended_at = utcnow()

            # Pickle the result in the same try-except block since we need
            # to use the same exc handling when pickling fails
            job._result = rv
            self.handle_job_success(job=job,
                                    queue=queue,
                                    started_job_registry=started_job_registry)
        except:
            job.ended_at = utcnow()
            exc_info = sys.exc_info()
            exc_string = self._get_safe_exception_string(
                traceback.format_exception(*exc_info)
            )
            self.handle_job_failure(job=job, exc_string=exc_string,
                                    started_job_registry=started_job_registry)
            self.handle_exception(job, *exc_info)
            return False

        finally:
            pop_connection()

        self.log.info('%s: %s (%s)', green(job.origin), blue('Job OK'), job.id)
        if rv is not None:
            log_result = "{0!r}".format(as_text(text_type(rv)))
            self.log.debug('Result: %s', yellow(log_result))

        if self.log_result_lifespan:
            result_ttl = job.get_result_ttl(self.default_result_ttl)
            if result_ttl == 0:
                self.log.info('Result discarded immediately')
            elif result_ttl > 0:
                self.log.info('Result is kept for %s seconds', result_ttl)
            else:
                self.log.info('Result will never expire, clean up result key manually')

        return True

    def handle_exception(self, job, *exc_info):
        """Walks the exception handler stack to delegate exception handling."""
        exc_string = Worker._get_safe_exception_string(
            traceback.format_exception_only(*exc_info[:2]) + traceback.format_exception(*exc_info)
        )
        self.log.error(exc_string, exc_info=True, extra={
            'func': job.func_name,
            'arguments': job.args,
            'kwargs': job.kwargs,
            'queue': job.origin,
        })

        for handler in self._exc_handlers:
            self.log.debug('Invoking exception handler %s', handler)
            fallthrough = handler(job, *exc_info)

            # Only handlers with explicit return values should disable further
            # exc handling, so interpret a None return value as True.
            if fallthrough is None:
                fallthrough = True

            if not fallthrough:
                break

    @staticmethod
    def _get_safe_exception_string(exc_strings):
        """Ensure list of exception strings is decoded on Python 2 and joined as one string safely."""
        if sys.version_info[0] < 3:
            try:
                exc_strings = [exc.decode("utf-8") for exc in exc_strings]
            except ValueError:
                exc_strings = [exc.decode("latin-1") for exc in exc_strings]
        return ''.join(exc_strings)

    def push_exc_handler(self, handler_func):
        """Pushes an exception handler onto the exc handler stack."""
        self._exc_handlers.append(handler_func)

    def pop_exc_handler(self):
        """Pops the latest exception handler off of the exc handler stack."""
        return self._exc_handlers.pop()

    def __eq__(self, other):
        """Equality does not take the database/connection into account"""
        if not isinstance(other, self.__class__):
            raise TypeError('Cannot compare workers to other types (of workers)')
        return self.name == other.name

    def __hash__(self):
        """The hash does not take the database/connection into account"""
        return hash(self.name)

    def clean_registries(self):
        """Runs maintenance jobs on each Queue's registries."""
        for queue in self.queues:
            # If there are multiple workers running, we only want 1 worker
            # to run clean_registries().
            if queue.acquire_cleaning_lock():
                self.log.info('Cleaning registries for queue: %s', queue.name)
                clean_registries(queue)
                clean_worker_registry(queue)
        self.last_cleaned_at = utcnow()

    @property
    def should_run_maintenance_tasks(self):
        """Maintenance tasks should run on first startup or 15 minutes."""
        if self.last_cleaned_at is None:
            return True
        if (utcnow() - self.last_cleaned_at) > timedelta(minutes=15):
            return True
        return False


class SimpleWorker(Worker):
    def main_work_horse(self, *args, **kwargs):
        raise NotImplementedError("Test worker does not implement this method")

    def execute_job(self, job, queue):
        """Execute job in same thread/process, do not fork()"""
        timeout = (job.timeout or DEFAULT_WORKER_TTL) + 5
        return self.perform_job(job, queue, heartbeat_ttl=timeout)


class HerokuWorker(Worker):
    """
    Modified version of rq worker which:
    * stops work horses getting killed with SIGTERM
    * sends SIGRTMIN to work horses on SIGTERM to the main process which in turn
    causes the horse to crash `imminent_shutdown_delay` seconds later
    """
    imminent_shutdown_delay = 6

    frame_properties = ['f_code', 'f_lasti', 'f_lineno', 'f_locals', 'f_trace']
    if PY2:
        frame_properties.extend(
            ['f_exc_traceback', 'f_exc_type', 'f_exc_value', 'f_restricted']
        )

    def setup_work_horse_signals(self):
        """Modified to ignore SIGINT and SIGTERM and only handle SIGRTMIN"""
        signal.signal(signal.SIGRTMIN, self.request_stop_sigrtmin)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def handle_warm_shutdown_request(self):
        """If horse is alive send it SIGRTMIN"""
        if self.horse_pid != 0:
            self.log.info('Warm shut down requested, sending horse SIGRTMIN signal')
            self.kill_horse(sig=signal.SIGRTMIN)
        else:
            self.log.warning('Warm shut down requested, no horse found')

    def request_stop_sigrtmin(self, signum, frame):
        if self.imminent_shutdown_delay == 0:
            self.log.warning('Imminent shutdown, raising ShutDownImminentException immediately')
            self.request_force_stop_sigrtmin(signum, frame)
        else:
            self.log.warning('Imminent shutdown, raising ShutDownImminentException in %d seconds',
                             self.imminent_shutdown_delay)
            signal.signal(signal.SIGRTMIN, self.request_force_stop_sigrtmin)
            signal.signal(signal.SIGALRM, self.request_force_stop_sigrtmin)
            signal.alarm(self.imminent_shutdown_delay)

    def request_force_stop_sigrtmin(self, signum, frame):
        info = dict((attr, getattr(frame, attr)) for attr in self.frame_properties)
        self.log.warning('raising ShutDownImminentException to cancel job...')
        raise ShutDownImminentException('shut down imminent (signal: %s)' % signal_name(signum), info)
