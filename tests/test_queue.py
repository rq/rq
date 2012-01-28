from tests import RQTestCase
from tests import testjob
from pickle import dumps
from rq import Queue
from rq.exceptions import DequeueError


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

        self.testconn.rpush('rq:queue:my-queue', 'some val')
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


    def test_dequeue_unpicklable_data(self):
        """Error handling of invalid pickle data."""

        # Push non-pickle data on the queue
        q = Queue('foo')
        blob = 'this is nothing like pickled data'
        self.testconn.rpush(q._key, blob)

        with self.assertRaises(DequeueError):
            q.dequeue()  # error occurs when perform()'ing

        # Push value pickle data, but not representing a job tuple
        q = Queue('foo')
        blob = dumps('this is not a job tuple')
        self.testconn.rpush(q._key, blob)

        with self.assertRaises(DequeueError):
            q.dequeue()  # error occurs when perform()'ing

        # Push slightly incorrect pickled data onto the queue (simulate
        # a function that can't be imported from the worker)
        q = Queue('foo')

        job_tuple = dumps((testjob, [], dict(name='Frank'), 'unused'))
        blob = job_tuple.replace('testjob', 'fooobar')
        self.testconn.rpush(q._key, blob)

        with self.assertRaises(DequeueError):
            q.dequeue()  # error occurs when dequeue()'ing

