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

from rq.compat import as_text, string_types, text_type

from .connections import get_current_connection
from .exceptions import DequeueTimeout, NoQueueError
from .job import Job, Status
from .logutils import setup_loghandlers
from .queue import get_failed_queue, Queue
from .timeouts import UnixSignalDeathPenalty
from .utils import import_attribute, make_colorizer, utcformat, utcnow
from .version import VERSION
from .registry import FinishedJobRegistry, StartedJobRegistry

try:
    from procname import setprocname
except ImportError:
    def setprocname(*args, **kwargs):  # noqa
        pass

green = make_colorizer('darkgreen')
yellow = make_colorizer('darkyellow')
blue = make_colorizer('darkblue')

DEFAULT_WORKER_TTL = 420
DEFAULT_RESULT_TTL = 500
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


class Worker(object):
    redis_worker_namespace_prefix = 'rq:worker:'
    redis_workers_keys = 'rq:workers'
    death_penalty_class = UnixSignalDeathPenalty
    queue_class = Queue
    job_class = Job

    @classmethod
    def all(cls, connection=None):
        """Returns an iterable of all Workers.
        """
        if connection is None:
            connection = get_current_connection()
        reported_working = connection.smembers(cls.redis_workers_keys)
        workers = [cls.find_by_key(as_text(key), connection)
                   for key in reported_working]
        return compact(workers)

    @classmethod
    def find_by_key(cls, worker_key, connection=None):
        """Returns a Worker instance, based on the naming conventions for
        naming the internal Redis keys.  Can be used to reverse-lookup Workers
        by their Redis keys.
        """
        prefix = cls.redis_worker_namespace_prefix
        if not worker_key.startswith(prefix):
            raise ValueError('Not a valid RQ worker key: %s' % (worker_key,))

        if connection is None:
            connection = get_current_connection()
        if not connection.exists(worker_key):
            connection.srem(cls.redis_workers_keys, worker_key)
            return None

        name = worker_key[len(prefix):]
        worker = cls([], name, connection=connection)
        queues = as_text(connection.hget(worker.key, 'queues'))
        worker._state = connection.hget(worker.key, 'state') or '?'
        worker._job_id = connection.hget(worker.key, 'current_job') or None
        if queues:
            worker.queues = [cls.queue_class(queue, connection=connection)
                             for queue in queues.split(',')]
        return worker

    def __init__(self, queues, name=None,
                 default_result_ttl=None, connection=None,
                 exc_handler=None, default_worker_ttl=None, job_class=None):  # noqa
        if connection is None:
            connection = get_current_connection()
        self.connection = connection
        if isinstance(queues, self.queue_class):
            queues = [queues]
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
        self._stopped = False
        self.log = logger
        self.failed_queue = get_failed_queue(connection=self.connection)

        # By default, push the "move-to-failed-queue" exception handler onto
        # the stack
        self.push_exc_handler(self.move_to_failed_queue)
        if exc_handler is not None:
            self.push_exc_handler(exc_handler)

        if job_class is not None:
            if isinstance(job_class, string_types):
                job_class = import_attribute(job_class)
            self.job_class = job_class

    def validate_queues(self):
        """Sanity check for the given queues."""
        if not iterable(self.queues):
            raise ValueError('Argument queues not iterable.')
        for queue in self.queues:
            if not isinstance(queue, self.queue_class):
                raise NoQueueError('Give each worker at least one Queue.')

    def queue_names(self):
        """Returns the queue names of this worker's queues."""
        return map(lambda q: q.name, self.queues)

    def queue_keys(self):
        """Returns the Redis keys representing this worker's queues."""
        return map(lambda q: q.key, self.queues)

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
            self._name = '%s.%s' % (shortname, self.pid)
        return self._name

    @property
    def key(self):
        """Returns the worker's Redis hash key."""
        return self.redis_worker_namespace_prefix + self.name

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
        setprocname('rq: %s' % (message,))

    def register_birth(self):
        """Registers its own birth."""
        self.log.debug('Registering birth of worker %s' % (self.name,))
        if self.connection.exists(self.key) and \
                not self.connection.hexists(self.key, 'death'):
            raise ValueError('There exists an active worker named \'%s\' '
                             'already.' % (self.name,))
        key = self.key
        queues = ','.join(self.queue_names())
        with self.connection._pipeline() as p:
            p.delete(key)
            p.hset(key, 'birth', utcformat(utcnow()))
            p.hset(key, 'queues', queues)
            p.sadd(self.redis_workers_keys, key)
            p.expire(key, self.default_worker_ttl)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        self.log.debug('Registering death')
        with self.connection._pipeline() as p:
            # We cannot use self.state = 'dead' here, because that would
            # rollback the pipeline
            p.srem(self.redis_workers_keys, self.key)
            p.hset(self.key, 'death', utcformat(utcnow()))
            p.expire(self.key, 60)
            p.execute()

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

    @property
    def stopped(self):
        return self._stopped

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.
        """

        def request_force_stop(signum, frame):
            """Terminates the application (cold shutdown).
            """
            self.log.warning('Cold shut down.')

            # Take down the horse with the worker
            if self.horse_pid:
                msg = 'Taking down horse %d with me.' % self.horse_pid
                self.log.debug(msg)
                try:
                    os.kill(self.horse_pid, signal.SIGKILL)
                except OSError as e:
                    # ESRCH ("No such process") is fine with us
                    if e.errno != errno.ESRCH:
                        self.log.debug('Horse already down.')
                        raise
            raise SystemExit()

        def request_stop(signum, frame):
            """Stops the current worker loop but waits for child processes to
            end gracefully (warm shutdown).
            """
            self.log.debug('Got signal %s.' % signal_name(signum))

            signal.signal(signal.SIGINT, request_force_stop)
            signal.signal(signal.SIGTERM, request_force_stop)

            msg = 'Warm shut down requested.'
            self.log.warning(msg)

            # If shutdown is requested in the middle of a job, wait until
            # finish before shutting down
            if self.get_state() == 'busy':
                self._stopped = True
                self.log.debug('Stopping after current horse is finished. '
                               'Press Ctrl+C again for a cold shutdown.')
            else:
                raise StopRequested()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)

    def work(self, burst=False):
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.
        """
        setup_loghandlers()
        self._install_signal_handlers()

        did_perform_work = False
        self.register_birth()
        self.log.info('RQ worker started, version %s' % VERSION)
        self.set_state('starting')
        try:
            while True:
                if self.stopped:
                    self.log.info('Stopping on request.')
                    break

                timeout = None if burst else max(1, self.default_worker_ttl - 60)
                try:
                    result = self.dequeue_job_and_maintain_ttl(timeout)
                    if result is None:
                        break
                except StopRequested:
                    break

                job, queue = result
                self.execute_job(job)
                self.heartbeat()

                if job.get_status() == Status.FINISHED:
                    queue.enqueue_dependents(job)

                did_perform_work = True
        finally:
            if not self.is_horse:
                self.register_death()
        return did_perform_work

    def dequeue_job_and_maintain_ttl(self, timeout):
        result = None
        qnames = self.queue_names()

        self.set_state('idle')
        self.procline('Listening on %s' % ','.join(qnames))
        self.log.info('')
        self.log.info('*** Listening on %s...' %
                      green(', '.join(qnames)))

        while True:
            self.heartbeat()

            try:
                result = self.queue_class.dequeue_any(self.queues, timeout,
                                                      connection=self.connection)
                if result is not None:
                    job, queue = result
                    self.log.info('%s: %s (%s)' % (green(queue.name),
                                  blue(job.description), job.id))

                break
            except DequeueTimeout:
                pass

        self.heartbeat()
        return result

    def heartbeat(self, timeout=0, pipeline=None):
        """Specifies a new worker timeout, typically by extending the
        expiration time of the worker, effectively making this a "heartbeat"
        to not expire the worker until the timeout passes.

        The next heartbeat should come before this time, or the worker will
        die (at least from the monitoring dashboards).

        The effective timeout can never be shorter than default_worker_ttl,
        only larger.
        """
        timeout = max(timeout, self.default_worker_ttl)
        connection = pipeline if pipeline is not None else self.connection
        connection.expire(self.key, timeout)
        self.log.debug('Sent heartbeat to prevent worker timeout. '
                       'Next one should arrive within {0} seconds.'.format(timeout))

    def execute_job(self, job):
        """Spawns a work horse to perform the actual work and passes it a job.
        The worker will wait for the work horse and make sure it executes
        within the given timeout bounds, or will end the work horse with
        SIGALRM.
        """
        child_pid = os.fork()
        if child_pid == 0:
            self.main_work_horse(job)
        else:
            self._horse_pid = child_pid
            self.procline('Forked %d at %d' % (child_pid, time.time()))
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

    def main_work_horse(self, job):
        """This is the entry point of the newly spawned work horse."""
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

    def prepare_job_execution(self, job):
        """Performs misc bookkeeping like updating states prior to
        job execution.
        """
        timeout = (job.timeout or 180) + 60

        with self.connection._pipeline() as pipeline:
            self.set_state('busy', pipeline=pipeline)
            self.set_current_job_id(job.id, pipeline=pipeline)
            self.heartbeat(timeout, pipeline=pipeline)
            registry = StartedJobRegistry(job.origin, self.connection)
            registry.add(job, timeout, pipeline=pipeline)
            job.set_status(Status.STARTED, pipeline=pipeline)
            pipeline.execute()

        self.procline('Processing %s from %s since %s' % (
            job.func_name,
            job.origin, time.time()))

    def perform_job(self, job):
        """Performs the actual work of a job.  Will/should only be called
        inside the work horse's process.
        """
        self.prepare_job_execution(job)

        with self.connection._pipeline() as pipeline:
            started_job_registry = StartedJobRegistry(job.origin)

            try:
                with self.death_penalty_class(job.timeout or self.queue_class.DEFAULT_TIMEOUT):
                    rv = job.perform()

                # Pickle the result in the same try-except block since we need
                # to use the same exc handling when pickling fails
                job._result = rv

                self.set_current_job_id(None, pipeline=pipeline)

                result_ttl = job.get_ttl(self.default_result_ttl)
                if result_ttl != 0:
                    job.ended_at = utcnow()
                    job._status = Status.FINISHED
                    job.save(pipeline=pipeline)

                    finished_job_registry = FinishedJobRegistry(job.origin)
                    finished_job_registry.add(job, result_ttl, pipeline)

                job.cleanup(result_ttl, pipeline=pipeline)
                started_job_registry.remove(job, pipeline=pipeline)

                pipeline.execute()

            except Exception:
                job.set_status(Status.FAILED, pipeline=pipeline)
                started_job_registry.remove(job, pipeline=pipeline)
                pipeline.execute()
                self.handle_exception(job, *sys.exc_info())
                return False

        if rv is None:
            self.log.info('Job OK')
        else:
            self.log.info('Job OK, result = %s' % (yellow(text_type(rv)),))

        if result_ttl == 0:
            self.log.info('Result discarded immediately.')
        elif result_ttl > 0:
            self.log.info('Result is kept for %d seconds.' % result_ttl)
        else:
            self.log.warning('Result will never expire, clean up result key manually.')

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
            self.log.debug('Invoking exception handler %s' % (handler,))
            fallthrough = handler(job, *exc_info)

            # Only handlers with explicit return values should disable further
            # exc handling, so interpret a None return value as True.
            if fallthrough is None:
                fallthrough = True

            if not fallthrough:
                break

    def move_to_failed_queue(self, job, *exc_info):
        """Default exception handler: move the job to the failed queue."""
        exc_string = ''.join(traceback.format_exception(*exc_info))
        self.log.warning('Moving job to %s queue.' % self.failed_queue.name)
        self.failed_queue.quarantine(job, exc_info=exc_string)

    def push_exc_handler(self, handler_func):
        """Pushes an exception handler onto the exc handler stack."""
        self._exc_handlers.append(handler_func)

    def pop_exc_handler(self):
        """Pops the latest exception handler off of the exc handler stack."""
        return self._exc_handlers.pop()


class SimpleWorker(Worker):
    def _install_signal_handlers(self, *args, **kwargs):
        """Signal handlers are useless for test worker, as it
        does not have fork() ability"""
        pass

    def main_work_horse(self, *args, **kwargs):
        raise NotImplementedError("Test worker does not implement this method")

    def execute_job(self, *args, **kwargs):
        """Execute job in same thread/process, do not fork()"""
        return self.perform_job(*args, **kwargs)
