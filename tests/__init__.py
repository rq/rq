import unittest
from redis import Redis
from logbook import NullHandler
from rq import conn

def find_empty_redis_database():
    """Tries to connect to a random Redis database (starting from 4), and
    will use/connect it when no keys are in there.
    """
    for dbnum in range(4, 17):
        testconn = Redis(db=dbnum)
        empty = len(testconn.keys('*')) == 0
        if empty:
            return testconn
    assert False, 'No empty Redis database found to run tests in.'


def slow(f):
    import os
    from functools import wraps

    @wraps(f)
    def _inner(*args, **kwargs):
        if os.environ.get('ONLY_RUN_FAST_TESTS'):
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
        conn.push(testconn)

        # Store the connection (for sanity checking)
        cls.testconn = testconn

        # Shut up logbook
        cls.log_handler = NullHandler()
        cls.log_handler.push_thread()

    def setUp(self):
        # Flush beforewards (we like our hygiene)
        conn.flushdb()

    def tearDown(self):
        # Flush afterwards
        conn.flushdb()

    @classmethod
    def tearDownClass(cls):
        cls.log_handler.pop_thread()

        # Pop the connection to Redis
        testconn = conn.pop()
        assert testconn == cls.testconn, 'Wow, something really nasty happened to the Redis connection stack. Check your setup.'

