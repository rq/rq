import sys
import os
import errno
import random
import time
import times
import procname
import socket
import signal
import traceback
from cPickle import dumps
try:
    from logbook import Logger
    Logger = Logger   # Does nothing except it shuts up pyflakes annoying error
except ImportError:
    from logging import Logger
from .queue import Queue
from .proxy import conn
from .utils import make_colorizer
from .exceptions import NoQueueError, UnpickleError

green = make_colorizer('darkgreen')
yellow = make_colorizer('darkyellow')
red = make_colorizer('darkred')
blue = make_colorizer('darkblue')


def iterable(x):
    return hasattr(x, '__iter__')


def compact(l):
    return [x for x in l if x is not None]

_signames = dict((getattr(signal, signame), signame) \
                    for signame in dir(signal) \
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

    @classmethod
    def all(cls):
        """Returns an iterable of all Workers.
        """
        reported_working = conn.smembers(cls.redis_workers_keys)
        return compact(map(cls.find_by_key, reported_working))

    @classmethod
    def find_by_key(cls, worker_key):
        """Returns a Worker instance, based on the naming conventions for
        naming the internal Redis keys.  Can be used to reverse-lookup Workers
        by their Redis keys.
        """
        prefix = cls.redis_worker_namespace_prefix
        name = worker_key[len(prefix):]
        if not worker_key.startswith(prefix):
            raise ValueError('Not a valid RQ worker key: %s' % (worker_key,))

        if not conn.exists(worker_key):
            return None

        name = worker_key[len(prefix):]
        worker = Worker([], name)
        queues = conn.hget(worker.key, 'queues')
        worker._state = conn.hget(worker.key, 'state') or '?'
        if queues:
            worker.queues = map(Queue, queues.split(','))
        return worker


    def __init__(self, queues, name=None, rv_ttl=500):  # noqa
        if isinstance(queues, Queue):
            queues = [queues]
        self._name = name
        self.queues = queues
        self.validate_queues()
        self.rv_ttl = rv_ttl
        self._state = 'starting'
        self._is_horse = False
        self._horse_pid = 0
        self._stopped = False
        self.log = Logger('worker')
        self.failed_queue = Queue('failed')


    def validate_queues(self):  # noqa
        """Sanity check for the given queues."""
        if not iterable(self.queues):
            raise ValueError('Argument queues not iterable.')
        for queue in self.queues:
            if not isinstance(queue, Queue):
                raise NoQueueError('Give each worker at least one Queue.')

    def queue_names(self):
        """Returns the queue names of this worker's queues."""
        return map(lambda q: q.name, self.queues)

    def queue_keys(self):
        """Returns the Redis keys representing this worker's queues."""
        return map(lambda q: q.key, self.queues)


    @property  # noqa
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
        procname.setprocname('rq: %s' % (message,))


    def register_birth(self):  # noqa
        """Registers its own birth."""
        self.log.debug('Registering birth of worker %s' % (self.name,))
        if conn.exists(self.key) and not conn.hexists(self.key, 'death'):
            raise ValueError(
                    'There exists an active worker named \'%s\' '
                    'already.' % (self.name,))
        key = self.key
        now = time.time()
        queues = ','.join(self.queue_names())
        with conn.pipeline() as p:
            p.delete(key)
            p.hset(key, 'birth', now)
            p.hset(key, 'queues', queues)
            p.sadd(self.redis_workers_keys, key)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        self.log.debug('Registering death')
        with conn.pipeline() as p:
            # We cannot use self.state = 'dead' here, because that would
            # rollback the pipeline
            p.srem(self.redis_workers_keys, self.key)
            p.hset(self.key, 'death', time.time())
            p.expire(self.key, 60)
            p.execute()

    def set_state(self, new_state):
        self._state = new_state
        conn.hset(self.key, 'state', new_state)

    def get_state(self):
        return self._state

    state = property(get_state, set_state)

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
            self.log.debug('Got %s signal.' % signal_name(signum))

            signal.signal(signal.SIGINT, request_force_stop)
            signal.signal(signal.SIGTERM, request_force_stop)

            if self.is_horse:
                self.log.debug('Ignoring signal %s.' % signal_name(signum))
                return

            msg = 'Warm shut down. Press Ctrl+C again for a cold shutdown.'
            self.log.warning(msg)
            self._stopped = True
            self.log.debug('Stopping after current horse is finished.')

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)


    def work(self, burst=False):  # noqa
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.
        """
        self._install_signal_handlers()

        did_perform_work = False
        self.register_birth()
        self.state = 'starting'
        try:
            while True:
                if self.stopped:
                    self.log.info('Stopping on request.')
                    break
                self.state = 'idle'
                qnames = self.queue_names()
                self.procline('Listening on %s' % ','.join(qnames))
                self.log.info('')
                self.log.info('*** Listening on %s...' % \
                        green(', '.join(qnames)))
                wait_for_job = not burst
                try:
                    result = Queue.dequeue_any(self.queues, wait_for_job)
                    if result is None:
                        break
                except UnpickleError as e:
                    msg = '*** Ignoring unpickleable data on %s.' % \
                            green(e.queue.name)
                    self.log.warning(msg)
                    self.log.debug('Data follows:')
                    self.log.debug(e.raw_data)
                    self.log.debug('End of unreadable data.')
                    self.failed_queue.push_job_id(e.job_id)
                    continue

                job, queue = result
                self.log.info('%s: %s (%s)' % (green(queue.name),
                    blue(job.description), job.id))

                self.state = 'busy'
                self.fork_and_perform_job(job)

                did_perform_work = True
        finally:
            if not self.is_horse:
                self.register_death()
        return did_perform_work

    def fork_and_perform_job(self, job):
        child_pid = os.fork()
        if child_pid == 0:
            self._is_horse = True
            random.seed()
            self.log = Logger('horse')

            success = self.perform_job(job)
            sys.exit(int(not success))
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

    def perform_job(self, job):
        self.procline('Processing %s from %s since %s' % (
            job.func.__name__,
            job.origin, time.time()))
        try:
            rv = job.perform()
        except Exception as e:
            fq = self.failed_queue
            self.log.exception(red(str(e)))
            self.log.warning('Moving job to %s queue.' % fq.name)

            # Store the exception information...
            job.ended_at = times.now()
            job.exc_info = traceback.format_exc()

            fq.enqueue_job(job, to_failed=True)
            return False
        else:
            if rv is None:
                self.log.info('Job OK')
            else:
                self.log.info('Job OK, result = %s' % (yellow(unicode(rv)),))
        if rv is not None:
            p = conn.pipeline()
            p.hset(job.key, 'result', dumps(rv))
            p.expire(job.key, self.rv_ttl)
            p.execute()
        return True
