import unittest
from redis import Redis
from logbook import NullHandler
from rq import conn

# Test data
def testjob(name=None):
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)

def failing_job(x):
    # Will throw a division-by-zero error
    return x / 0


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
        testconn = Redis()
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


    def assertQueueContains(self, queue, that_func):
        # Do a queue scan (this is O(n), but we're in a test, so hey)
        for job in queue.jobs:
            if job.func == that_func:
                return
        self.fail('Queue %s does not contain message for function %s' %
                (queue.key, that_func))

