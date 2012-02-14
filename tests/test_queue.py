from tests import RQTestCase
from tests import testjob
from rq import Queue
from rq.job import Job


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


    def test_empty_queue(self):
        """Emptying queues."""
        q = Queue('example')

        self.testconn.rpush('rq:queue:example', 'foo')
        self.testconn.rpush('rq:queue:example', 'bar')
        self.assertEquals(q.is_empty(), False)

        q.empty()

        self.assertEquals(q.is_empty(), True)
        self.assertIsNone(self.testconn.lpop('rq:queue:example'))

    def test_queue_is_empty(self):
        """Detecting empty queues."""
        q = Queue('example')
        self.assertEquals(q.is_empty(), True)

        self.testconn.rpush('rq:queue:example', 'sentinel message')
        self.assertEquals(q.is_empty(), False)


    def test_enqueue(self):
        """Enqueueing job onto queues."""
        q = Queue()
        self.assertEquals(q.is_empty(), True)

        # testjob spec holds which queue this is sent to
        job = q.enqueue(testjob, 'Nick', foo='bar')
        job_id = job.id

        # Inspect data inside Redis
        q_key = 'rq:queue:default'
        self.assertEquals(self.testconn.llen(q_key), 1)
        self.assertEquals(self.testconn.lrange(q_key, 0, -1)[0], job_id)

    def test_enqueue_sets_metadata(self):
        """Enqueueing job onto queues modifies meta data."""
        q = Queue()
        job = Job.for_call(testjob, 'Nick', foo='bar')

        # Preconditions
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)

        # Action
        q.enqueue_job(job)

        # Postconditions
        self.assertEquals(job.origin, q.name)
        self.assertIsNotNone(job.enqueued_at)


    def test_pop_job_id(self):
        """Popping job IDs from queues."""
        # Set up
        q = Queue()
        uuid = '112188ae-4e9d-4a5b-a5b3-f26f2cb054da'
        q.push_job_id(uuid)

        # Pop it off the queue...
        self.assertEquals(q.count, 1)
        self.assertEquals(q.pop_job_id(), uuid)

        # ...and assert the queue count when down
        self.assertEquals(q.count, 0)

    def test_dequeue(self):
        """Dequeueing jobs from queues."""
        # Set up
        q = Queue()
        result = q.enqueue(testjob, 'Rick', foo='bar')

        # Dequeue a job (not a job ID) off the queue
        self.assertEquals(q.count, 1)
        job = q.dequeue()
        self.assertEquals(job.id, result.id)
        self.assertEquals(job.func, testjob)
        self.assertEquals(job.origin, q.name)
        self.assertEquals(job.args[0], 'Rick')
        self.assertEquals(job.kwargs['foo'], 'bar')

        # ...and assert the queue count when down
        self.assertEquals(q.count, 0)

    def test_dequeue_ignores_nonexisting_jobs(self):
        """Dequeuing silently ignores non-existing jobs."""

        q = Queue()
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)
        q.push_job_id(uuid)
        result = q.enqueue(testjob, 'Nick', foo='bar')
        q.push_job_id(uuid)

        # Dequeue simply ignores the missing job and returns None
        self.assertEquals(q.count, 4)
        self.assertEquals(q.dequeue().id, result.id)
        self.assertIsNone(q.dequeue())
        self.assertEquals(q.count, 0)

    def test_dequeue_any(self):
        """Fetching work from any given queue."""
        fooq = Queue('foo')
        barq = Queue('bar')

        self.assertEquals(Queue.dequeue_any([fooq, barq], False), None)

        # Enqueue a single item
        barq.enqueue(testjob)
        job, queue = Queue.dequeue_any([fooq, barq], False)
        self.assertEquals(job.func, testjob)
        self.assertEquals(queue, barq)

        # Enqueue items on both queues
        barq.enqueue(testjob, 'for Bar')
        fooq.enqueue(testjob, 'for Foo')

        job, queue = Queue.dequeue_any([fooq, barq], False)
        self.assertEquals(queue, fooq)
        self.assertEquals(job.func, testjob)
        self.assertEquals(job.origin, fooq.name)
        self.assertEquals(job.args[0], 'for Foo', 'Foo should be dequeued first.')

        job, queue = Queue.dequeue_any([fooq, barq], False)
        self.assertEquals(queue, barq)
        self.assertEquals(job.func, testjob)
        self.assertEquals(job.origin, barq.name)
        self.assertEquals(job.args[0], 'for Bar', 'Bar should be dequeued second.')

    def test_dequeue_any_ignores_nonexisting_jobs(self):
        """Dequeuing (from any queue) silently ignores non-existing jobs."""

        q = Queue('low')
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)

        # Dequeue simply ignores the missing job and returns None
        self.assertEquals(q.count, 1)
        self.assertEquals(Queue.dequeue_any([Queue(), Queue('low')], False), None)
        self.assertEquals(q.count, 0)

