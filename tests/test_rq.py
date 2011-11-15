import unittest
from pickle import loads
from blinker import signal
from redis import Redis
from rq import conn, Queue, job

@job('my-queue')
def testjob():
    return 'hi there'


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

    def test_queue_empty(self):
        """Detecting empty queues."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        conn.rpush('rq:my-queue', 'some val')
        self.assertEquals(q.empty, False)

    def test_put_work_on_queue(self):
        """Putting work on queues."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        testjob.delay()
        self.assertEquals(q.empty, False)
        self.assertQueueContains(q, testjob)


if __name__ == '__main__':
    unittest.main()
