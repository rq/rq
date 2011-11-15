import unittest
from pickle import loads
from blinker import signal
from redis import Redis
from rq import conn, Queue

# Test data
def testjob(name=None):
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


class RQTestCase(unittest.TestCase):
    def setUp(self):
        super(RQTestCase, self).setUp()

        # Set up connection to Redis
        testconn = Redis()
        conn.push(testconn)

        # Flush beforewards (we like our hygiene)
        conn.flushdb()
        signal('setup').send(self)

        # Store the connection (for sanity checking)
        self.testconn = testconn

    def tearDown(self):
        signal('teardown').send(self)

        # Flush afterwards
        conn.flushdb()

        # Pop the connection to Redis
        testconn = conn.pop()
        assert testconn == self.testconn, 'Wow, something really nasty happened to the Redis connection stack. Check your setup.'

        super(RQTestCase, self).tearDown()


    def assertQueueContains(self, queue, that_func):
        # Do a queue scan (this is O(n), but we're in a test, so hey)
        for message in queue.messages:
            f, _, args, kwargs = loads(message)
            if f == that_func:
                return
        self.fail('Queue %s does not contain message for function %s' %
                (queue.key, that_func))


class TestQueue(RQTestCase):
    def test_create_queue(self):
        """Creating queues."""
        q = Queue('my-queue')
        self.assertEquals(q.name, 'my-queue')

    def test_create_default_queue(self):
        """Instantiating the default queue."""
        q = Queue()
        self.assertEquals(q.name, 'default')

    def test_queue_empty(self):
        """Detecting empty queues."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        conn.rpush('rq:my-queue', 'some val')
        self.assertEquals(q.empty, False)


    def test_enqueue(self):
        """Putting work on queues."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        # testjob spec holds which queue this is sent to
        q.enqueue(testjob, 'Nick', foo='bar')
        self.assertEquals(q.empty, False)
        self.assertQueueContains(q, testjob)

    def test_dequeue(self):
        """Fetching work from specific queue."""
        q = Queue('foo')
        q.enqueue(testjob, 'Rick', foo='bar')

        # Pull it off the queue (normally, a worker would do this)
        f, args, kwargs, rv_key = q.dequeue()
        self.assertEquals(f, testjob)
        self.assertEquals(args[0], 'Rick')
        self.assertEquals(kwargs['foo'], 'bar')


if __name__ == '__main__':
    unittest.main()
