# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging

from redis import StrictRedis
from rq import pop_connection, push_connection
from rq.compat import is_python_version

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
        push_connection(testconn)

        # Store the connection (for sanity checking)
        cls.testconn = testconn

        # Shut up logging
        logging.disable(logging.ERROR)

    def setUp(self):
        # Flush beforewards (we like our hygiene)
        self.testconn.flushdb()

    def tearDown(self):
        # Flush afterwards
        self.testconn.flushdb()

    # Implement assertIsNotNone for Python runtimes < 2.7 or < 3.1
    if not hasattr(unittest.TestCase, 'assertIsNotNone'):
        def assertIsNotNone(self, value, *args):  # noqa
            self.assertNotEqual(value, None, *args)

    @classmethod
    def tearDownClass(cls):
        logging.disable(logging.NOTSET)

        # Pop the connection to Redis
        testconn = pop_connection()
        assert testconn == cls.testconn, \
            'Wow, something really nasty happened to the Redis connection stack. Check your setup.'
