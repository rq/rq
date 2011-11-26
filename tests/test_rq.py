import unittest
from pickle import loads
from redis import Redis
from logbook import NullHandler
from rq import conn, Queue, Worker

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

        # Store the connection (for sanity checking)
        self.testconn = testconn

        # Shut up logbook
        self.log_handler = NullHandler()
        self.log_handler.push_thread()

    def tearDown(self):
        self.log_handler.pop_thread()

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


    def test_equality(self):
        """Mathematical equality of queues."""
        q1 = Queue('foo')
        q2 = Queue('foo')
        q3 = Queue('bar')

        self.assertEquals(q1, q2)
        self.assertEquals(q2, q1)
        self.assertNotEquals(q1, q3)
        self.assertNotEquals(q2, q3)


    def test_queue_empty(self):
        """Detecting empty queues."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        conn.rpush('rq:queue:my-queue', 'some val')
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
        job = q.dequeue()
        self.assertEquals(job.func, testjob)
        self.assertEquals(job.origin, q)
        self.assertEquals(job.args[0], 'Rick')
        self.assertEquals(job.kwargs['foo'], 'bar')


    def test_dequeue_any(self):
        """Fetching work from any given queue."""
        fooq = Queue('foo')
        barq = Queue('bar')

        self.assertEquals(Queue.dequeue_any([fooq, barq], False), None)

        # Enqueue a single item
        barq.enqueue(testjob)
        job = Queue.dequeue_any([fooq, barq], False)
        self.assertEquals(job.func, testjob)

        # Enqueue items on both queues
        barq.enqueue(testjob, 'for Bar')
        fooq.enqueue(testjob, 'for Foo')

        job = Queue.dequeue_any([fooq, barq], False)
        self.assertEquals(job.func, testjob)
        self.assertEquals(job.origin, fooq)
        self.assertEquals(job.args[0], 'for Foo', 'Foo should be dequeued first.')

        job = Queue.dequeue_any([fooq, barq], False)
        self.assertEquals(job.func, testjob)
        self.assertEquals(job.origin, barq)
        self.assertEquals(job.args[0], 'for Bar', 'Bar should be dequeued second.')


class TestWorker(RQTestCase):
    def test_create_worker(self):
        """Worker creation."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = Worker([fooq, barq])
        self.assertEquals(w.queues, [fooq, barq])

    def test_work_and_quit(self):
        """Worker processes work, then quits."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = Worker([fooq, barq])
        self.assertEquals(w.work(burst=True), False, 'Did not expect any work on the queue.')

        fooq.enqueue(testjob, name='Frank')
        self.assertEquals(w.work(burst=True), True, 'Expected at least some work done.')


if __name__ == '__main__':
    unittest.main()
