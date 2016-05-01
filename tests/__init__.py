# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging
from uuid import uuid4

from redis import StrictRedis
from rq.job import Job
from rq.compat import is_python_version
from rq.connections import RQConnection, pop_connection

if is_python_version((2, 7), (3, 2)):
    import unittest
else:
    import unittest2 as unittest  # noqa


def find_empty_redis_database():
    """Tries to connect to a random Redis database (starting from 4), and
    will use/connect it when no keys are in there.
    """
    for dbnum in range(4, 17):
        testconn = StrictRedis(db=dbnum)
        empty = len(testconn.keys('*')) == 0
        if empty:
            return testconn
    assert False, 'No empty Redis database found to run tests in.'


def slow(f):
    import os
    from functools import wraps

    @wraps(f)
    def _inner(*args, **kwargs):
        if os.environ.get('RUN_SLOW_TESTS_TOO'):
            f(*args, **kwargs)

    return _inner


class RQTestCase(unittest.TestCase):
    """Base class to inherit test cases from for RQ.

    It sets up the Redis connection (available via self.testconn), turns off
    logging to the terminal and flushes the Redis database before and after
    running each test.

    Also offers assertQueueContains(queue, that_func) assertion method.
    """

    @classmethod
    def setUpClass(cls):
        # Set up connection to Redis
        testconn = find_empty_redis_database()

        # Store the connection (for sanity checking)
        cls.testconn = testconn

        # Shut up logging
        logging.disable(logging.ERROR)

    def setUp(self):
        # Flush beforewards (we like our hygiene)
        self.testconn.flushdb()
        self.conn = RQConnection(self.testconn)
        while pop_connection():
            pass

    def tearDown(self):
        # Flush afterwards
        self.testconn.flushdb()

    # Implement assertIsNotNone for Python runtimes < 2.7 or < 3.1
    if not hasattr(unittest.TestCase, 'assertIsNotNone'):
        def assertIsNotNone(self, value, *args):
            self.assertNotEqual(value, None, *args)

    @classmethod
    def tearDownClass(cls):
        logging.disable(logging.NOTSET)

    def create_job(self, *args, **kwargs):
        if 'origin' not in kwargs:
            kwargs['origin'] = str(uuid4())
        if 'func' not in kwargs and len(args) == 0:
            kwargs['func'] = 'tests.fixtures.say_hello'

        job = Job.create(connection=self.conn, *args, **kwargs)
        return job

