import sys
import os
import random
import time
import procname
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
    def __init__(self, queues, rv_ttl=500):
        if isinstance(queues, Queue):
            queues = [queues]
        self.queues = queues
        self.validate_queues()
        self.rv_ttl = rv_ttl
        self._working = False
        self.log = Logger('worker')

    def validate_queues(self):
        if not iterable(self.queues):
            raise ValueError('Argument queues not iterable.')
        for queue in self.queues:
            if not isinstance(queue, Queue):
                raise NoQueueError('Give each worker at least one Queue.')

    def queue_names(self):
        return map(lambda q: q.name, self.queues)

    def queue_keys(self):
        return map(lambda q: q.key, self.queues)


    def is_idle(self):
        return not self.is_working()

    def is_working(self):
        return self._working


    @property
    def pid(self):
        return os.getpid()

    def procline(self, message):
        self.log.debug(message)
        procname.setprocname('rq: %s' % (message,))


    def _work(self, quit_when_done=False):
        did_work = False
        while True:
            self.procline('Waiting on %s' % (', '.join(self.queue_names()),))
            wait_for_job = not quit_when_done
            job = Queue.dequeue_any(self.queues, wait_for_job)
            if job is None:
                break
            did_work = True
            self.fork_and_perform_job(job)
        return did_work

    def work(self):
        self._work(False)

    def work_burst(self):
        return self._work(True)

    def fork_and_perform_job(self, job):
        child_pid = os.fork()
        if child_pid == 0:
            random.seed()
            self.log = Logger('horse')
            try:
                self.procline('Processing work since %d' % (time.time(),))
                self._working = True
                self.perform_job(job)
            except Exception, e:
                self.log.exception(e)
                sys.exit(1)
            sys.exit(0)
        else:
            self.procline('Forked %d at %d' % (child_pid, time.time()))
            os.waitpid(child_pid, 0)
            self._working = False

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
