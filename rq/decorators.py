# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from functools import wraps
import warnings

from rq.compat import string_types

from .defaults import DEFAULT_RESULT_TTL
from .config import DEFAULT_CONFIG, Config
from .utils import overwrite_config_connection


class job:  # noqa
    def __init__(self, queue, config=DEFAULT_CONFIG, connection=None, timeout=None,
                 result_ttl=DEFAULT_RESULT_TTL, ttl=None,
                 queue_class=None, depends_on=None, at_front=None, meta=None,
                 description=None, failure_ttl=None, retry=None):
        """A decorator that adds a ``delay`` method to the decorated function,
        which in turn creates a RQ job when called. Accepts a required
        ``queue`` argument that can be either a ``Queue`` instance or a string
        denoting the queue name.  For example:

            @job(queue='default')
            def simple_add(x, y):
                return x + y

            simple_add.delay(1, 2) # Puts simple_add function into queue
        """

        if queue_class is not None:
            warnings.warn('queue_class argument of job decorator is deprecated and will be removed in RQ 2. '
                          'Use @job(config=Config(queue_class=queue_class)) instead.', DeprecationWarning)
            config = Config(template=config, queue_class=queue_class)

        self.queue = queue
        self.config = overwrite_config_connection(config, connection)
        self.timeout = timeout
        self.result_ttl = result_ttl
        self.ttl = ttl
        self.meta = meta
        self.depends_on = depends_on
        self.at_front = at_front
        self.description = description
        self.failure_ttl = failure_ttl
        self.retry = retry

    def __call__(self, f):
        @wraps(f)
        def delay(*args, **kwargs):
            if isinstance(self.queue, string_types):
                queue = self.config.queue_class(name=self.queue, config=self.config)
            else:
                queue = self.queue

            depends_on = kwargs.pop('depends_on', None)
            job_id = kwargs.pop('job_id', None)
            at_front = kwargs.pop('at_front', False)

            if not depends_on:
                depends_on = self.depends_on

            if not at_front:
                at_front = self.at_front

            return queue.enqueue_call(f, args=args, kwargs=kwargs,
                                      timeout=self.timeout, result_ttl=self.result_ttl,
                                      ttl=self.ttl, depends_on=depends_on, job_id=job_id, at_front=at_front,
                                      meta=self.meta, description=self.description, failure_ttl=self.failure_ttl,
                                      retry=self.retry)
        f.delay = delay
        return f
