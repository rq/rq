# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from redis.client import StrictRedis, Redis, StrictPipeline, Pipeline
from contextlib import contextmanager
import time
import math

from .worker import Worker
from .queue import Queue
from .job import Job
from .local import LocalStack
from .compat import string_types, as_text
from .utils import import_attribute, compact, transaction
from .keys import (QUEUES_KEY, queue_name_from_key, worker_key_from_name,
                   WORKERS_KEY, queue_key_from_name, SUSPENDED_KEY)
from .exceptions import NoSuchJobError

class NoRedisConnectionException(Exception):
    pass


class RQConnection(object):
    worker_class = Worker
    queue_class = Queue
    job_class = Job

    def __init__(self, redis_conn=None, worker_class=None,
                 job_class=None, queue_class=None):

        if worker_class is not None:
            if isinstance(worker_class, string_types):
                worker_class = import_attribute(worker_class)
            self.worker_class = worker_class

        if job_class is not None:
            if isinstance(job_class, string_types):
                job_class = import_attribute(job_class)
            self.job_class = job_class

        if queue_class is not None:
            if isinstance(queue_class, string_types):
                queue_class = import_attribute(queue_class)
            self.queue_class = queue_class

        if isinstance(redis_conn, StrictRedis):
            self._redis_conn = redis_conn
        elif isinstance(redis_conn, Redis):
            connection_pool = redis_conn.connection_pool
            self._redis_conn = StrictRedis(connection_pool=connection_pool)
        else:
            raise ValueError("redis_conn must be either Redis or StrictRedis")

        self._pipe = None

    def __enter__(self):
        push_connection(self)

    def __exit__(self, tupe, value, traceback):
        pop_connection()

    @property
    def rq_conn(self):
        return self

    @transaction
    def get_all_queues(self):
        """
        Returns an iterable of all Queues.
        """
        return [self.mkqueue(queue_name_from_key(rq_key)) for rq_key in
                self._smembers(QUEUES_KEY) if rq_key]

    def mkqueue(self, name='default', default_timeout=None, async=True):
        """
        Get a queue by name, note: this does not actually do a remote check. Do
        that call sync().
        """
        return self.queue_class(name, default_timeout, async=async,
                                connection=self)

    def mkworker(self, queues, name=None, default_result_ttl=None,
                 exception_handlers=None, default_worker_ttl=None,
                 queue_class=None, job_class=None):

        if job_class is None:
            job_class = self.job_class

        if queue_class is None:
            queue_class = self.queue_class

        return self.worker_class(queues, name=name,
                                 default_result_ttl=default_result_ttl,
                                 exception_handlers=exception_handlers,
                                 default_worker_ttl=default_worker_ttl,
                                 connection=self)

    def get_deferred_registry(self, name='default'):
        """

        Note: no database interaction takes place here
        """
        from .registry import DeferredJobRegistry
        return DeferredJobRegistry(name, connection=self)

    def get_started_registry(self, name='default'):
        """

        Note: no database interaction takes place here
        """
        from .registry import StartedJobRegistry
        return StartedJobRegistry(name, connection=self)

    def get_finished_registry(self, name='default'):
        """

        Note: no database interaction takes place here
        """
        from .registry import FinishedJobRegistry
        return FinishedJobRegistry(name, connection=self)

    def get_failed_queue(self):
        """Returns a handle to the special failed queue."""
        from .queue import FailedQueue
        return FailedQueue(connection=self)

    @transaction
    def get_worker(self, name):
        """
        Returns a Worker instance
        """
        worker_key = worker_key_from_name(name)
        if not self._exists(worker_key):
            self._srem(WORKERS_KEY, worker_key)
            return None

        worker_dict = self._hgetall(worker_key)
        queues = as_text(worker_dict['queues']).split(',')

        worker = self.worker_class(queues, name=name, connection=self)
        worker._state = as_text(self._hget(worker.key, 'state') or '?')
        worker._job_id = self._hget(worker.key, 'current_job') or None

        return worker

    @transaction
    def get_all_workers(self):
        """
        Returns an iterable of all Workers.
        """
        reported_working = self._smembers(WORKERS_KEY)
        workers = [self.get_worker(name) for name in reported_working]
        return compact(workers)

    @transaction
    def get_job(self, job_id):
        job = self.job_class(job_id, connection=self)
        job.refresh()
        return job

    def mkjob(self, func, args=None, kwargs=None, result_ttl=None, ttl=None,
              status=None, description=None, depends_on=None, timeout=None,
              origin='default', meta=None):
        """
        Createa job
        """
        return self.job_class.create(func, args=args, kwargs=kwargs,
                                     result_ttl=result_ttl, ttl=ttl,
                                     status=status, description=description,
                                     depends_on=depends_on, timeout=timeout,
                                     origin=origin, meta=meta, connection=self)

    def job_exists(self, job_id):
        try:
            return self.get_job(job_id)
        except NoSuchJobError:
            return None

    def clean_registries(self, queue):
        """Cleans StartedJobRegistry and FinishedJobRegistry of a queue."""
        from rq.queue import Queue
        if isinstance(queue, Queue):
            name = queue.name
        else:
            name = queue

        registry = self.get_finished_registry(name)
        registry.cleanup()
        registry = self.get_started_registry(name)
        registry.cleanup()

    @transaction
    def is_suspended(self):
        return self._exists(SUSPENDED_KEY)

    @transaction
    def suspend(self, ttl=None):
        """
        :param conn:
        :param ttl: time to live in seconds.  Default is no expiration
               Note: If you pass in 0 it will invalidate right away
        """
        self._set(SUSPENDED_KEY, 1)
        if ttl is not None:
            self._expire(SUSPENDED_KEY, ttl)

    @transaction
    def resume(self):
        return self._delete(SUSPENDED_KEY)

    @transaction
    def _pop_job_id_no_wait(self, queues):
        """
        Non-blocking dequeue of the next job. Job is atomically moved to running
        registry for the queue

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        """
        job_id = None
        ret_queue_name = None
        for ii, queue in enumerate(queues):
            if not isinstance(queue, self.queue_class):
                queue = self.mkqueue(queue)

            if queue.count > 0:
                reg = self.get_started_registry(queue.name)
                job_id = as_text(self._lpop(queue.key))
                ret_queue_name = queue.name
                reg.add(job_id)
                break

        return job_id, ret_queue_name

    def _pop_job_id(self, queues, timeout=0):
        """
        Non-blocking dequeue of the next job. Job is moved to running registry
        for the queue.

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        :return (job_id, queue_name)
        """
        if timeout == 0:
            return self._pop_job_id_no_wait(queues)

        # Since timeout is > 0, we can use blplop. Unfortunately there is no way
        # to atomically move from a queue to a set (maybe StartedJobRegistry
        # should be a simple list and we could use BRPOPLPUSH). So there is a
        # *tiny* window of time that this job will be only stored locally in
        # memory. Generally this sort of circumstance is to be avoided since it
        # is technically possible to lose data, but in this case there doesn't
        # seem a way around it, other than busy waiting
        keys = []
        for queue in queues:
            if isinstance(queue, self.queue_class):
                keys.append(queue_key_from_name(queue.name))
            else:
                keys.append(queue_key_from_name(queue))

        # Convert None to blpop's infinite timeout (0)
        if timeout is None:
            timeout = 0
        result = self._redis_conn.blpop(keys, int(math.ceil(timeout)))

        if result:
            queue_key, job_id = result
            reg = self.get_started_registry(queue_name_from_key(queue_key))
            reg.add(job_id)
            return job_id, queue_name_from_key(queue_key)
        else:
            return None, None

    def dequeue_any(self, queues, timeout=0):
        """
        Dequeue a single job. Unlike RQ we expect all jobs to exist

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        """
        while True:
            start_time = time.time()
            job_id, queue_name = self._pop_job_id(queues, timeout=timeout)

            if job_id is not None:
                try:
                    return self.get_job(job_id), self.mkqueue(queue_name)
                except NoSuchJobError:
                    # If we find a job that doesn't exist, try again with timeout
                    # reduced
                    if timeout is not None:
                        timeout = max(0, timeout - (time.time() - start_time))
            else:
                return None

    ### Redis Pipe / Connection Management ###
    def _active_transaction(self):
        return self._pipe is not None

    @property
    def redis_write(self):
        if self._active_transaction():
            if not self._pipe.explicit_transaction:
                self._pipe.multi()
            return self._pipe
        else:
            return self._redis_conn

    @property
    def redis_read(self):
        if self._active_transaction():
            return self._pipe
        else:
            return self._redis_conn

    @property
    def redis_rw(self):
        assert not self._active_transaction()
        return self._redis_conn

    @contextmanager
    def _pipeline(self):
        with self._redis_conn.pipeline() as pipe:
            yield pipe

    ### Redis Write ###
    def _hset(self, name, key, value):
        self.redis_write.hset(name, key, value)

    def _setex(self, name, time, value):
        """
        Use keyword arguments so that non-strict version acts the same
        """
        self.redis_write.setex(name=name, time=time, value=value)

    def _set(self, name, value, ex=None, px=None, nx=False, xx=False):
        """
        Set key
        """
        self.redis_write.set(name, value, ex, px, nx, xx)

    def _lrem(self, name, count, value):
        """
        Patched count->num for non-strict version
        """
        if (isinstance(self.redis_write, StrictPipeline) or
                isinstance(self.redis_write, StrictRedis)):
            # name, count, value
            self.redis_write.lrem(name, count, value)

        elif (isinstance(self.redis_write, Pipeline) or
                isinstance(self.redis_write, Redis)):
            # name, value, num
            self.redis_write.lrem(name, num=count, value=value)

    def _zadd(self, name, *args, **kwargs):
        """
        Patched to handle [score0, name0, score1, name1, ... ] in args for
        non-strict version
        """
        self.redis_write.zadd(name, *args, **kwargs)

    def _zrem(self, name, value):
        self.redis_write.zrem(name, value)

    def _lpush(self, name, *values):
        if len(values) > 0:
            self.redis_write.lpush(name, *values)

    def _rpush(self, name, *values):
        self.redis_write.rpush(name, *values)

    def _delete(self, name):
        self.redis_write.delete(name)

    def _srem(self, name, *values):
        self.redis_write.srem(name, *values)

    def _hmset(self, name, mapping):
        self.redis_write.hmset(name, mapping)

    def _expire(self, name, ttl):
        self.redis_write.expire(name, ttl)

    def _zremrangebyscore(self, *args, **kwargs):
        self.redis_write.zremrangebyscore(*args, **kwargs)

    def _sadd(self, name, *args, **kwargs):
        self.redis_write.sadd(name, *args, **kwargs)

    def _persist(self, name):
        self.redis_write.persist(name)

    def _hdel(self, name, key):
        self.redis_write.hdel(name, key)

    ### Read ###
    def _ttl(self, name):
        """
        Return the strict -1 if no ttl exists for the key
        """
        self.redis_read.watch(name)
        out = self.redis_read.ttl(name)
        if out is None:
            return -1
        return out

    def _pttl(self, name):
        """
        Return the strict -1 if no ttl exists for the key
        """
        self.redis_read.watch(name)
        out = self.redis_read.pttl(name)
        if out is None:
            return -1
        return out

    def _smembers(self, name):
        self.redis_read.watch(name)
        return self.redis_read.smembers(name)

    def _llen(self, name):
        self.redis_read.watch(name)
        return self.redis_read.llen(name)

    def _lrange(self, name, start, end):
        self.redis_read.watch(name)
        return self.redis_read.lrange(name, start, end)

    def _zcard(self, name):
        self.redis_read.watch(name)
        return self.redis_read.zcard(name)

    def _zrangebyscore(self, name, *args, **kwargs):
        self.redis_read.watch(name)
        return self.redis_read.zrangebyscore(name, *args, **kwargs)

    def _zrange(self, name, *args, **kwargs):
        self.redis_read.watch(name)
        return self.redis_read.zrange(name, *args, **kwargs)

    def _scard(self, name):
        self.redis_read.watch(name)
        return self.redis_read.scard(name)

    def _hgetall(self, name):
        self.redis_read.watch(name)
        return self.redis_read.hgetall(name)

    def _hget(self, name, key):
        self.redis_read.watch(name)
        return self.redis_read.hget(name, key)

    def _exists(self, name):
        self.redis_read.watch(name)
        return self.redis_read.exists(name)

    def _hexists(self, name, key):
        self.redis_read.watch(name)
        return self.redis_read.hexists(name, key)

    # Read / Then Write. BE WARY THIS CAN ONLY BE USED 1 TIME PER PIPE
    def _lpop(self, name):
        self.redis_read.watch(name)
        out = self.redis_read.lindex(name, 0)
        self.redis_write.lpop(name)
        return out


def push_connection(redis):
    """Pushes the given connection on the stack."""
    _connection_stack.push(resolve_connection(redis))


def pop_connection():
    """Pops the topmost connection from the stack."""
    return _connection_stack.pop()


def use_connection(redis=None):
    """Clears the stack and uses the given connection.  Protects against mixed
    use of use_connection() and stacked connection contexts.
    """
    assert len(_connection_stack) <= 1, \
        'You should not mix Connection contexts with use_connection()'
    release_local(_connection_stack)

    if redis is None:
        redis = StrictRedis()
    push_connection(redis)


def get_current_connection():
    """Returns the current Redis connection (i.e. the topmost on the
    connection stack).
    """
    return _connection_stack.top


def resolve_connection(connection=None):
    """Convenience function to resolve the given or the current connection.
    Raises an exception if it cannot resolve a connection now.
    """
    if connection is None:
        connection = get_current_connection()

    if isinstance(connection, RQConnection):
        return connection
    return RQConnection(redis_conn=connection)


_connection_stack = LocalStack()


__all__ = ['RQConnection', 'get_current_connection', 'push_connection',
           'pop_connection', 'use_connection']
