import unittest
from pickle import loads
from blinker import signal
from redis import Redis
from rq import conn, Queue, job

# Test data
@job('my-queue')
def testjob(name=None):
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)

@job() # no queue spec'ed
def queueless_job(name=None):
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

    def test_queue_empty(self):
        """Detecting empty queues."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        conn.rpush('rq:my-queue', 'some val')
        self.assertEquals(q.empty, False)


    def test_enqueue(self):
        """Putting work on queues using delay."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        # testjob spec holds which queue this is sent to
        testjob.delay()
        self.assertEquals(q.empty, False)
        self.assertQueueContains(q, testjob)

    def test_enqueue_to_different_queue(self):
        """Putting work on alternative queues using enqueue."""

        # Override testjob spec holds which queue
        q = Queue('different-queue')
        self.assertEquals(q.empty, True)
        testjob.enqueue(q, 'Nick')
        self.assertEquals(q.empty, False)
        self.assertQueueContains(q, testjob)

    def test_enqueue_to_different_queue_reverse(self):
        """Putting work on specific queues using the Queue object."""

        q = Queue('alt-queue')
        self.assertEquals(q.empty, True)
        q.enqueue(testjob)
        self.assertEquals(q.empty, False)
        self.assertQueueContains(q, testjob)


    def test_dequeue(self):
        """Fetching work from specific queue."""
        q = Queue('foo')
        testjob.enqueue(q, 'Rick')

        # Pull it off the queue (normally, a worker would do this)
        f, rv_key, args, kwargs = q.dequeue()
        self.assertEquals(f, testjob)
        self.assertEquals(args[0], 'Rick')


class TestJob(RQTestCase):
    def test_job_methods(self):
        """Jobs have methods to enqueue them."""
        self.assertTrue(hasattr(testjob, 'delay'))
        self.assertTrue(hasattr(testjob, 'enqueue'))
        self.assertTrue(hasattr(queueless_job, 'delay'))
        self.assertTrue(hasattr(queueless_job, 'enqueue'))

    def test_queue_empty(self):
        """Detecting empty queues."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        conn.rpush('rq:my-queue', 'some val')
        self.assertEquals(q.empty, False)

    def test_put_work_on_queue(self):
        """Putting work on queues using delay."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        # testjob spec holds which queue this is sent to
        testjob.delay()
        self.assertEquals(q.empty, False)
        self.assertQueueContains(q, testjob)

    def test_put_work_on_queue_fails_for_queueless_jobs(self):
        """Putting work on queues using delay fails for queueless jobs."""
        self.assertRaises(ValueError, queueless_job.delay, 'Rick')

    def test_put_work_on_different_queue(self):
        """Putting work on alternative queues using enqueue."""

        # Override testjob spec holds which queue
        q = Queue('different-queue')
        self.assertEquals(q.empty, True)
        testjob.enqueue(q)
        self.assertEquals(q.empty, False)
        self.assertQueueContains(q, testjob)

    def test_put_work_on_different_queue_reverse(self):
        """Putting work on specific queues using the Queue object."""

        q = Queue('alt-queue')
        self.assertEquals(q.empty, True)
        q.enqueue(testjob)
        self.assertEquals(q.empty, False)
        self.assertQueueContains(q, testjob)


if __name__ == '__main__':
    unittest.main()
