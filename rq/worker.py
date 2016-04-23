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
from datetime import timedelta

from rq.compat import as_text, string_types, text_type

from .defaults import DEFAULT_RESULT_TTL, DEFAULT_WORKER_TTL
from .exceptions import DequeueTimeout
from .job import JobStatus
from .logutils import setup_loghandlers
from .timeouts import UnixSignalDeathPenalty
from .utils import (ensure_list, enum, make_colorizer, utcformat, utcnow,
                    utcparse, transaction)
from .version import VERSION
from .keys import worker_key_from_name, WORKERS_KEY, SUSPENDED_KEY

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


def iterable(x):
    return hasattr(x, '__iter__')


def compact(l):
    return [x for x in l if x is not None]

_signames = dict((getattr(signal, signame), signame)
                 for signame in dir(signal)
                 if signame.startswith('SIG') and '_' not in signame)


def signal_name(signum):
    # Hackety-hack-hack: is there really no better way to reverse lookup the
    # signal name?  If you read this and know a way: please provide a patch :)
    try:
        return _signames[signum]
    except KeyError:
        return 'SIG_UNKNOWN'


WorkerStatus = enum(
    'WorkerStatus',
    STARTED='started',
    SUSPENDED='suspended',
    BUSY='busy',
    IDLE='idle'
)


class Worker(object):
    death_penalty_class = UnixSignalDeathPenalty

    def __init__(self, queues, name=None, default_result_ttl=None,
                 exception_handlers=None, default_worker_ttl=None,
                 connection=None):

        from .connections import resolve_connection
        self.connection = resolve_connection(connection)

        queues = [self.connection.mkqueue(name=q)
                  if isinstance(q, string_types) else q
                  for q in ensure_list(queues)]
        self._name = name
        self.queues = queues
        self.validate_queues()
        self._exc_handlers = []

        if default_result_ttl is None:
            default_result_ttl = DEFAULT_RESULT_TTL
        self.default_result_ttl = default_result_ttl

        if default_worker_ttl is None:
            default_worker_ttl = DEFAULT_WORKER_TTL
        self.default_worker_ttl = default_worker_ttl

        self._state = 'starting'
        self._is_horse = False
        self._horse_pid = 0
        self._stop_requested = False
        self.log = logger
        self.failed_queue = self.connection.get_failed_queue()
        self.last_cleaned_at = None

        if isinstance(exception_handlers, (list, tuple)):
            for h in exception_handlers:
                self.push_exc_handler(h)
        elif exception_handlers is not None:
            self.push_exc_handler(exception_handlers)

    @property
    def rq_conn(self):
        return self.connection

    def validate_queues(self):
        """Sanity check for the given queues."""
        from rq.queue import Queue
        for queue in self.queues:
            queue_class = self.connection.queue_class
            if not isinstance(queue, queue_class):
                raise TypeError('{0} is not of type {1} or string types'.format(queue, queue_class))

    def queue_names(self):
        """Returns the queue names of this worker's queues."""
        return list(map(lambda q: q.name, self.queues))

    def queue_keys(self):
        """Returns the Redis keys representing this worker's queues."""
        return list(map(lambda q: q.key, self.queues))

    @property
    def name(self):
        """Returns the name of the worker, under which it is registered to the
        monitoring system.

        By default, the name of the worker is constructed from the current
        (short) host name and the current PID.
        """
        if self._name is None:
            hostname = socket.gethostname()
            shortname, _, _ = hostname.partition('.')
            self._name = '{0}.{1}'.format(shortname, self.pid)
        return self._name

    @property
    def key(self):
        """Returns the worker's Redis hash key."""
        return worker_key_from_name(self.name)

    @property
    def pid(self):
        """The current process ID."""
        return os.getpid()

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

    @transaction
    def register_birth(self):
        """Registers its own birth."""
        self.log.debug('Registering birth of worker {0}'.format(self.name))
        if (self.connection._exists(self.key) and not
                self.connection._hexists(self.key, 'death')):
            raise ValueError('There exists an active worker named {0!r} '
                             'already'.format(self.name))

        queues = ','.join(self.queue_names())
        self.connection._delete(self.key)
        self.connection._hset(self.key, 'birth', utcformat(utcnow()))
        self.connection._hset(self.key, 'queues', queues)
        self.connection._sadd(WORKERS_KEY, self.name)
        self.connection._expire(self.key, self.default_worker_ttl)

    @transaction
    def register_death(self):
        """Registers its own death."""
        self.log.debug('Registering death')
        self.connection._srem(WORKERS_KEY, self.name)
        self.connection._hset(self.key, 'death', utcformat(utcnow()))
        self.connection._expire(self.key, 60)

    @transaction
    def set_shutdown_requested_date(self):
        """Sets the date on which the worker received a (warm) shutdown request"""
        self.connection._hset(self.key, 'shutdown_requested_date',
                            utcformat(utcnow()))

    @property
    @transaction
    def birth_date(self):
        """Fetches birth date from Redis."""
        birth_timestamp = self.connection._hget(self.key, 'birth')
        if birth_timestamp is not None:
            return utcparse(as_text(birth_timestamp))

    @property
    @transaction
    def shutdown_requested_date(self):
        """Fetches shutdown_requested_date from Redis."""
        shutdown_requested_timestamp = self.connection._hget(self.key, 'shutdown_requested_date')
        if shutdown_requested_timestamp is not None:
            return utcparse(as_text(shutdown_requested_timestamp))

    @property
    @transaction
    def death_date(self):
        """Fetches death date from Redis."""
        death_timestamp = self.connection._hget(self.key, 'death')
        if death_timestamp is not None:
            return utcparse(as_text(death_timestamp))

    @transaction
    def set_state(self, state):
        self._state = state
        self.connection._hset(self.key, 'state', state)

    def get_state(self):
        return self._state

    @transaction
    def set_current_job_id(self, job_id):
        if job_id is None:
            self.connection._hdel(self.key, 'current_job')
        else:
            self.connection._hset(self.key, 'current_job', job_id)

    @transaction
    def get_current_job_id(self):
        return as_text(self.connection._hget(self.key, 'current_job'))

    @transaction
    def get_current_job(self):
        """Returns the job id of the currently executing job."""
        job_id = self.get_current_job_id()

        if job_id is None:
            return None

        return self.connection.get_job(job_id)

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.
        """

        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def request_force_stop(self, signum, frame):
        """Terminates the application (cold shutdown).
        """
        self.log.warning('Cold shut down')

        # Take down the horse with the worker
        if self.horse_pid:
            msg = 'Taking down horse {0} with me'.format(self.horse_pid)
            self.log.debug(msg)
            try:
                os.kill(self.horse_pid, signal.SIGKILL)
            except OSError as e:
                # ESRCH ("No such process") is fine with us
                if e.errno != errno.ESRCH:
                    self.log.debug('Horse already down')
                    raise
        raise SystemExit()

    @transaction
    def request_stop(self, signum, frame):
        """Stops the current worker loop but waits for child processes to
        end gracefully (warm shutdown).
        """
        self.log.debug('Got signal {0}'.format(signal_name(signum)))

        signal.signal(signal.SIGINT, self.request_force_stop)
        signal.signal(signal.SIGTERM, self.request_force_stop)

        msg = 'Warm shut down requested'
        self.log.warning(msg)

        # If shutdown is requested in the middle of a job, wait until
        # finish before shutting down and save the request in redis
        if self.get_state() == 'busy':
            self._stop_requested = True
            self.set_shutdown_requested_date()
            self.log.debug('Stopping after current horse is finished. '
                           'Press Ctrl+C again for a cold shutdown.')
        else:
            raise StopRequested()

    def check_for_suspension(self, burst):
        """Check to see if workers have been suspended by `rq suspend`"""

        before_state = None
        notified = False

        while (not self._stop_requested and
                self.connection._exists(SUSPENDED_KEY)):

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

    def work(self, burst=False, logging_level="INFO"):
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.
        """
        setup_loghandlers(logging_level)
        self._install_signal_handlers()

        did_perform_work = False
        self.register_birth()
        self.log.info("RQ worker {0!r} started, version {1}".format(self.key, VERSION))
        self.set_state(WorkerStatus.STARTED)

        try:
            while True:
                try:
                    self.check_for_suspension(burst)

                    if self.should_run_maintenance_tasks:
                        self.clean_registries()

                    if self._stop_requested:
                        self.log.info('Stopping on request')
                        break

                    timeout = 0 if burst else max(1, self.default_worker_ttl - 60)

                    result = self.dequeue_job_and_maintain_ttl(timeout)
                    if result is None:
                        if burst:
                            self.log.info("RQ worker {0!r} done, quitting".format(self.key))
                        break
                except StopRequested:
                    break

                job, queue = result
                self.execute_job(job)
                self.heartbeat()
                did_perform_work = True

        finally:
            self.register_death()
        return did_perform_work

    def dequeue_job_and_maintain_ttl(self, timeout):
        result = None
        qnames = self.queue_names()

        self.set_state(WorkerStatus.IDLE)
        self.procline('Listening on {0}'.format(','.join(qnames)))
        self.log.info('')
        self.log.info('*** Listening on {0}...'.format(green(', '.join(qnames))))

        while True:
            self.heartbeat()

            try:
                result = self.connection.dequeue_any(self.queues, timeout)
                if result is not None:
                    job, queue = result
                    self.log.info('{0}: {1} ({2})'.format(green(queue.name),
                                                          blue(job.description),
                                                          job.id))

                break
            except DequeueTimeout:
                pass

        self.heartbeat()
        return result

    @transaction
    def heartbeat(self, timeout=0):
        """
        Specifies a new worker timeout, typically by extending the
        expiration time of the worker, effectively making this a "heartbeat"
        to not expire the worker until the timeout passes.

        The next heartbeat should come before this time, or the worker will
        die (at least from the monitoring dashboards).

        The effective timeout can never be shorter than default_worker_ttl,
        only larger.
        """
        timeout = max(timeout, self.default_worker_ttl)
        self.connection._expire(self.key, timeout)
        self.log.debug('Sent heartbeat to prevent worker timeout. '
                       'Next one should arrive within {0} seconds.'.format(timeout))

    def execute_job(self, job):
        """
        Spawns a work horse to perform the actual work and passes it a job.
        The worker will wait for the work horse and make sure it executes
        within the given timeout bounds, or will end the work horse with
        SIGALRM.
        """
        self.set_state('busy')
        child_pid = os.fork()
        os.environ['RQ_WORKER_ID'] = self.name
        os.environ['RQ_JOB_ID'] = job.id
        if child_pid == 0:
            self.main_work_horse(job)
        else:
            self._horse_pid = child_pid
            self.procline('Forked {0} at {1}'.format(child_pid, time.time()))
            while True:
                try:
                    os.waitpid(child_pid, 0)
                    break
                except OSError as e:
                    # In case we encountered an OSError due to EINTR (which is
                    # caused by a SIGINT or SIGTERM signal during
                    # os.waitpid()), we simply ignore it and enter the next
                    # iteration of the loop, waiting for the child to end.  In
                    # any other case, this is some other unexpected OS error,
                    # which we don't want to catch, so we re-raise those ones.
                    if e.errno != errno.EINTR:
                        raise

        self.set_state('idle')

    def main_work_horse(self, job):
        """This is the entry point of the newly spawned work horse.

        Job should already have been moved to started registry when it was
        de-queued. But no other modifications should have been made
        """
        # After fork()'ing, always assure we are generating random sequences
        # that are different from the worker.
        random.seed()

        # Always ignore Ctrl+C in the work horse, as it might abort the
        # currently running job.
        # The main worker catches the Ctrl+C and requests graceful shutdown
        # after the current work is done.  When cold shutdown is requested, it
        # kills the current job anyway.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        self._is_horse = True
        self.log = logger

        success = self.perform_job(job)

        # os._exit() is the way to exit from childs after a fork(), in
        # constrast to the regular sys.exit()
        os._exit(int(not success))

    @transaction
    def prepare_job_execution(self, job):
        """Performs misc bookkeeping like updating states prior to
        job execution.
        """
        timeout = (job.timeout or 180) + 60
        started_registry = self.connection.get_started_registry(job.origin)

        self.set_state(WorkerStatus.BUSY)
        self.set_current_job_id(job.id)
        self.heartbeat(timeout)
        job.set_status(JobStatus.STARTED)
        self.connection._hset(job.key, 'started_at', utcformat(utcnow()))
        started_registry.add(job, timeout)

        msg = 'Processing {0} from {1} since {2}'
        self.procline(msg.format(job.func_name, job.origin, time.time()))

    def perform_job(self, job):
        """Performs the actual work of a job.  Will/should only be called
        inside the work horse's process.
        """
        self.prepare_job_execution(job)
        try:
            timeout = job.timeout or self.connection.queue_class.DEFAULT_TIMEOUT
            with self.death_penalty_class(timeout):
                rv = job.perform(self.default_result_ttl)

        except Exception as exc:
            self.set_current_job_id(None)
            self.handle_exception(job, *sys.exc_info())
            return False

        self.set_current_job_id(None)
        self.log.info('{0}: {1} ({2})'.format(green(job.origin), blue('Job OK'), job.id))
        if rv:
            log_result = "{0!r}".format(as_text(text_type(rv)))
            self.log.debug('Result: {0}'.format(yellow(log_result)))

        result_ttl = job.get_result_ttl(self.default_result_ttl)
        if result_ttl == 0:
            self.log.info('Result discarded immediately')
        elif result_ttl > 0:
            self.log.info('Result is kept for {0} seconds'.format(result_ttl))
        else:
            self.log.warning('Result will never expire, clean up result key manually')

        return True

    def handle_exception(self, job, *exc_info):
        """Walks the exception handler stack to delegate exception handling."""
        exc_string = ''.join(traceback.format_exception_only(*exc_info[:2]) +
                             traceback.format_exception(*exc_info))
        self.log.error(exc_string, exc_info=True, extra={
            'func': job.func_name,
            'arguments': job.args,
            'kwargs': job.kwargs,
            'queue': job.origin,
        })

        for handler in reversed(self._exc_handlers):
            self.log.debug('Invoking exception handler {0}'.format(handler))
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
            self.log.info('Cleaning registries for queue: {0}'.format(queue.name))
            queue.clean_registries()
        self.last_cleaned_at = utcnow()

    @property
    def should_run_maintenance_tasks(self):
        """Maintenance tasks should run on first startup or every hour."""
        if self.last_cleaned_at is None:
            return True
        if (utcnow() - self.last_cleaned_at) > timedelta(hours=1):
            return True
        return False


class SimpleWorker(Worker):
    def main_work_horse(self, *args, **kwargs):
        raise NotImplementedError("Test worker does not implement this method")

    def execute_job(self, *args, **kwargs):
        """Execute job in same thread/process, do not fork()"""
        return self.perform_job(*args, **kwargs)
