import sys
import os
import random
import time
import procname
import socket
from pickle import dumps
try:
    from logbook import Logger
    Logger = Logger    # Does nothing except it shuts up pyflakes annoying error
except ImportError:
    from logging import Logger
from .queue import Queue
from .proxy import conn
from .exceptions import NoQueueError

def iterable(x):
    return hasattr(x, '__iter__')

class Worker(object):
    redis_worker_namespace_prefix = 'rq:worker:'
    redis_workers_keys = 'rq:workers'

    @classmethod
    def all(cls):
        """Returns an iterable of all Workers.
        """
        reported_working = conn.smembers(cls.redis_workers_keys)
        return map(cls.from_worker_key, reported_working)

    @classmethod
    def from_worker_key(cls, worker_key):
        """Returns a Worker instance, based on the naming conventions for naming
        the internal Redis keys.  Can be used to reverse-lookup Workers by their
        Redis keys.
        """
        prefix = cls.redis_worker_namespace_prefix
        name = worker_key[len(prefix):]
        if not worker_key.startswith(prefix):
            raise ValueError('Not a valid RQ worker key: %s' % (worker_key,))
        name = worker_key[len(prefix):]
        return Worker([], name)


    def __init__(self, queues, name=None, rv_ttl=500):
        if isinstance(queues, Queue):
            queues = [queues]
        self._name = name
        self.queues = queues
        self.validate_queues()
        self.rv_ttl = rv_ttl
        self._state = 'starting'
        self._is_horse = False
        self.log = Logger('worker')


    def validate_queues(self):
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
    def is_horse(self):
        """Returns whether or not this is the worker or the work horse."""
        return self._is_horse

    def procline(self, message):
        """Changes the current procname for the process.

        This can be used to make `ps -ef` output more readable.
        """
        self.log.debug(message)
        procname.setprocname('rq: %s' % (message,))


    def register_birth(self):
        """Registers its own birth."""
        if conn.exists(self.key) and not conn.hexists(self.key, 'death'):
            raise ValueError('There exists an active worker named \'%s\' alread.' % (self.name,))
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
        self.log.error('Registering death')
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

    def _work(self, quit_when_done=False):
        """This method starts the work loop.
        """
        did_work = False
        self.register_birth()
        self.state = 'starting'
        try:
            while True:
                self.state = 'idle'
                self.procline('Waiting on %s' % (', '.join(self.queue_names()),))
                wait_for_job = not quit_when_done
                job = Queue.dequeue_any(self.queues, wait_for_job)
                if job is None:
                    break
                self.state = 'busy'
                self.fork_and_perform_job(job)
                did_work = True
        finally:
            if not self._is_horse:
                self.register_death()
        return did_work

    def work(self):
        """Pop and perform all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues.
        """
        self._work(False)

    def work_burst(self):
        """Pop and perform all jobs on the current list of queues.  When all
        queues are empty, return.

        The return value indicates whether any jobs were processed.
        """
        return self._work(True)

    def fork_and_perform_job(self, job):
        child_pid = os.fork()
        if child_pid == 0:
            self._is_horse = True
            random.seed()
            self.log = Logger('horse')
            try:
                self.procline('Processing work since %d' % (time.time(),))
                self.perform_job(job)
            except Exception, e:
                self.log.exception(e)
                sys.exit(1)
            sys.exit(0)
        else:
            self.procline('Forked %d at %d' % (child_pid, time.time()))
            os.waitpid(child_pid, 0)

    def perform_job(self, job):
        self.procline('Processing %s from %s since %s' % (job.func.__name__, job.origin.name, time.time()))
        try:
            rv = job.perform()
        except Exception, e:
            rv = e
            self.log.exception(e)
        else:
            if rv is not None:
                self.log.info('Job result = %s' % (rv,))
            else:
                self.log.info('Job ended normally without result')
        if rv is not None:
            p = conn.pipeline()
            p.set(job.rv_key, dumps(rv))
            p.expire(job.rv_key, self.rv_ttl)
            p.execute()
