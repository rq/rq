import sys
import os
import random
import time
import procname
from logbook import Logger
from pickle import loads, dumps
from .queue import Queue
from .proxy import conn

class NoQueueError(Exception): pass

class Worker(object):
    def __init__(self, queue_names, rv_ttl=500):
        self.queues = map(Queue, queue_names)
        self.rv_ttl = rv_ttl
        self._working = False
        self.log = Logger('worker')
        self.validate_queues()

    def validate_queues(self):
        if not self.queues:
            raise NoQueueError('Give each worker at least one queue.')

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

    def work(self):
        while True:
            self.procline('Waiting on %s' % (', '.join(self.queue_names()),))
            queue, msg = conn.blpop(self.queue_keys())
            self.fork_and_perform_job(queue, msg)

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
        func, key, args, kwargs = loads(msg)
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
            p.set(key, dumps(rv))
            p.expire(key, self.rv_ttl)
            p.execute()
