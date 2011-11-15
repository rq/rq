import sys
import os
import random
import time
import procname
try:
    from logbook import Logger
except ImportError:
    from logging import Logger
from pickle import loads, dumps
from .queue import Queue
from .proxy import conn

def iterable(x):
    return hasattr(x, '__iter__')

class NoQueueError(Exception): pass
class NoMoreWorkError(Exception): pass

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


    def multi_lpop(self, queues):
        # Redis' BLPOP command takes multiple queue arguments, but LPOP can
        # only take a single queue.  Therefore, we need to loop over all
        # queues manually, in order, and raise an exception is no more work
        # is available
        for queue in queues:
            value = conn.lpop(queue)
            if value is not None:
                return (queue, value)
        return None

    def pop_next_job(self, blocking):
        queues = self.queue_keys()
        if blocking:
            queue, msg = conn.blpop(queues)
        else:
            value = self.multi_lpop(queues)
            if value is None:
                raise NoMoreWorkError('No more work.')
            queue, msg = value
        return (queue, msg)

    def _work(self, quit_when_done=False):
        while True:
            self.procline('Waiting on %s' % (', '.join(self.queue_names()),))
            try:
                wait_for_job = not quit_when_done
                queue, msg = self.pop_next_job(wait_for_job)
            except NoMoreWorkError:
                break
            self.fork_and_perform_job(queue, msg)

    def work_forever(self):
        return self._work(False)

    def work(self):
        return self._work(True)

    def fork_and_perform_job(self, queue, msg):
        child_pid = os.fork()
        if child_pid == 0:
            random.seed()
            self.log = Logger('horse')
            try:
                self.procline('Processing work since %d' % (time.time(),))
                self._working = True
                self.perform_job(queue, msg)
            except Exception, e:
                self.log.exception(e)
                sys.exit(1)
            sys.exit(0)
        else:
            self.procline('Forked %d at %d' % (child_pid, time.time()))
            os.waitpid(child_pid, 0)
            self._working = False

    def perform_job(self, queue, msg):
        func, args, kwargs, rv_key = loads(msg)
        self.procline('Processing %s from %s since %s' % (func.__name__, queue, time.time()))
        try:
            rv = func(*args, **kwargs)
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
            p.set(rv_key, dumps(rv))
            p.expire(rv_key, self.rv_ttl)
            p.execute()
