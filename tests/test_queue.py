from datetime import timedelta

from tests import RQTestCase
from tests.fixtures import Number, div_by_zero, say_hello, some_calculation
from rq import Queue, get_failed_queue
from rq.job import Job, Status
from rq.exceptions import InvalidJobOperationError
from rq.scheduler import Scheduler

import times


class TestQueue(RQTestCase):
    def test_create_queue(self):
        """Creating queues."""
        q = Queue('my-queue')
        self.assertEquals(q.name, 'my-queue')

    def test_create_default_queue(self):
        """Instantiating the default queue."""
        q = Queue()
        self.assertEquals(q.name, 'default')


    def test_equality(self):  # noqa
        """Mathematical equality of queues."""
        q1 = Queue('foo')
        q2 = Queue('foo')
        q3 = Queue('bar')

        self.assertEquals(q1, q2)
        self.assertEquals(q2, q1)
        self.assertNotEquals(q1, q3)
        self.assertNotEquals(q2, q3)


    def test_empty_queue(self):  # noqa
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

    def test_remove(self):
        """Ensure queue.remove properly removes Job from queue."""
        q = Queue('example')
        job = q.enqueue(say_hello)
        self.assertIn(job.id, q.job_ids)
        q.remove(job)
        self.assertNotIn(job.id, q.job_ids)

        job = q.enqueue(say_hello)
        self.assertIn(job.id, q.job_ids)
        q.remove(job.id)
        self.assertNotIn(job.id, q.job_ids)

    def test_jobs(self):
        """Getting jobs out of a queue."""
        q = Queue('example')
        self.assertEqual(q.jobs, [])
        job = q.enqueue(say_hello)
        self.assertEqual(q.jobs, [job])

        # Fetching a deleted removes it from queue
        job.delete()
        self.assertEqual(q.job_ids, [job.id])
        q.jobs
        self.assertEqual(q.job_ids, [])

    def test_compact(self):
        """Compacting queueus."""
        q = Queue()

        q.enqueue(say_hello, 'Alice')
        bob = q.enqueue(say_hello, 'Bob')
        q.enqueue(say_hello, 'Charlie')
        debrah = q.enqueue(say_hello, 'Debrah')

        bob.delete()
        debrah.delete()

        self.assertEquals(q.count, 4)

        q.compact()

        self.assertEquals(q.count, 2)


    def test_enqueue(self):  # noqa
        """Enqueueing job onto queues."""
        q = Queue()
        self.assertEquals(q.is_empty(), True)

        # say_hello spec holds which queue this is sent to
        job = q.enqueue(say_hello, 'Nick', foo='bar')
        job_id = job.id

        # Inspect data inside Redis
        q_key = 'rq:queue:default'
        self.assertEquals(self.testconn.llen(q_key), 1)
        self.assertEquals(self.testconn.lrange(q_key, 0, -1)[0], job_id)

    def test_enqueue_sets_metadata(self):
        """Enqueueing job onto queues modifies meta data."""
        q = Queue()
        job = Job.create(func=say_hello, args=('Nick',), kwargs=dict(foo='bar'))

        # Preconditions
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)

        # Action
        q.enqueue_job(job)

        # Postconditions
        self.assertEquals(job.origin, q.name)
        self.assertIsNotNone(job.enqueued_at)


    def test_pop_job_id(self):  # noqa
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
        result = q.enqueue(say_hello, 'Rick', foo='bar')

        # Dequeue a job (not a job ID) off the queue
        self.assertEquals(q.count, 1)
        job = q.dequeue()
        self.assertEquals(job.id, result.id)
        self.assertEquals(job.func, say_hello)
        self.assertEquals(job.origin, q.name)
        self.assertEquals(job.args[0], 'Rick')
        self.assertEquals(job.kwargs['foo'], 'bar')

        # ...and assert the queue count when down
        self.assertEquals(q.count, 0)

    def test_dequeue_instance_method(self):
        """Dequeueing instance method jobs from queues."""
        q = Queue()
        n = Number(2)
        q.enqueue(n.div, 4)

        job = q.dequeue()

        # The instance has been pickled and unpickled, so it is now a separate
        # object. Test for equality using each object's __dict__ instead.
        self.assertEquals(job.instance.__dict__, n.__dict__)
        self.assertEquals(job.func.__name__, 'div')
        self.assertEquals(job.args, (4,))

    def test_dequeue_class_method(self):
        """Dequeueing class method jobs from queues."""
        q = Queue()
        q.enqueue(Number.divide, 3, 4)

        job = q.dequeue()

        self.assertEquals(job.instance.__dict__, Number.__dict__)
        self.assertEquals(job.func.__name__, 'divide')
        self.assertEquals(job.args, (3, 4))

    def test_dequeue_ignores_nonexisting_jobs(self):
        """Dequeuing silently ignores non-existing jobs."""

        q = Queue()
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)
        q.push_job_id(uuid)
        result = q.enqueue(say_hello, 'Nick', foo='bar')
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

        self.assertEquals(Queue.dequeue_any([fooq, barq], None), None)

        # Enqueue a single item
        barq.enqueue(say_hello)
        job, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEquals(job.func, say_hello)
        self.assertEquals(queue, barq)

        # Enqueue items on both queues
        barq.enqueue(say_hello, 'for Bar')
        fooq.enqueue(say_hello, 'for Foo')

        job, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEquals(queue, fooq)
        self.assertEquals(job.func, say_hello)
        self.assertEquals(job.origin, fooq.name)
        self.assertEquals(job.args[0], 'for Foo',
                'Foo should be dequeued first.')

        job, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEquals(queue, barq)
        self.assertEquals(job.func, say_hello)
        self.assertEquals(job.origin, barq.name)
        self.assertEquals(job.args[0], 'for Bar',
                'Bar should be dequeued second.')

    def test_dequeue_any_ignores_nonexisting_jobs(self):
        """Dequeuing (from any queue) silently ignores non-existing jobs."""

        q = Queue('low')
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)

        # Dequeue simply ignores the missing job and returns None
        self.assertEquals(q.count, 1)
        self.assertEquals(Queue.dequeue_any([Queue(), Queue('low')], None),  # noqa
                None)
        self.assertEquals(q.count, 0)

    def test_enqueue_sets_status(self):
        """Enqueueing a job sets its status to "queued"."""
        q = Queue()
        job = q.enqueue(say_hello)
        self.assertEqual(job.status, Status.QUEUED)

    def test_schedule_at(self):
        """Ensure queue.schedule_at assigns the correct score and origin."""
        scheduled_time = times.now()
        queue_name = 'scheduler_test'
        queue = Queue(name=queue_name, connection=self.testconn)
        scheduler = Scheduler(queue_name, connection=self.testconn)
        job = queue.schedule_at(scheduled_time, say_hello)
        self.assertEqual(job, Job.fetch(job.id, connection=self.testconn))
        self.assertEqual(job.origin, queue_name)
        self.assertIn(job.id,
            self.testconn.zrange(scheduler.scheduled_jobs_key, 0, 1))
        self.assertEqual(self.testconn.zscore(scheduler.scheduled_jobs_key, job.id),
                         times.to_unix(scheduled_time))

    def test_schedule_in(self):
        right_now = times.now()
        time_delta = timedelta(minutes=1)
        queue_name = 'scheduler_test'
        queue = Queue(name=queue_name, connection=self.testconn)
        scheduler = Scheduler(queue_name, connection=self.testconn)        
        job = queue.schedule_in(time_delta, say_hello)
        self.assertEqual(job.origin, queue_name)
        self.assertIn(job.id, self.testconn.zrange(scheduler.scheduled_jobs_key, 0, 1))
        self.assertEqual(self.testconn.zscore(scheduler.scheduled_jobs_key, job.id),
                         times.to_unix(right_now + time_delta))
        time_delta = timedelta(hours=1)
        job = scheduler.enqueue_in(time_delta, say_hello)
        self.assertEqual(self.testconn.zscore(scheduler.scheduled_jobs_key, job.id),
                         times.to_unix(right_now + time_delta))



class TestFailedQueue(RQTestCase):
    def test_requeue_job(self):
        """Requeueing existing jobs."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))  # noqa

        self.assertItemsEqual(Queue.all(), [get_failed_queue()])  # noqa
        self.assertEquals(get_failed_queue().count, 1)

        get_failed_queue().requeue(job.id)

        self.assertEquals(get_failed_queue().count, 0)
        self.assertEquals(Queue('fake').count, 1)

    def test_requeue_nonfailed_job_fails(self):
        """Requeueing non-failed jobs raises error."""
        q = Queue()
        job = q.enqueue(say_hello, 'Nick', foo='bar')

        # Assert that we cannot requeue a job that's not on the failed queue
        with self.assertRaises(InvalidJobOperationError):
            get_failed_queue().requeue(job.id)

    def test_quarantine_preserves_timeout(self):
        """Quarantine preserves job timeout."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.timeout = 200
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))

        self.assertEquals(job.timeout, 200)

    def test_requeueing_preserves_timeout(self):
        """Requeueing preserves job timeout."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.timeout = 200
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))
        get_failed_queue().requeue(job.id)

        job = Job.fetch(job.id)
        self.assertEquals(job.timeout, 200)

    def test_requeue_sets_status_to_queued(self):
        """Requeueing a job should set its status back to QUEUED."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))
        get_failed_queue().requeue(job.id)

        job = Job.fetch(job.id)
        self.assertEqual(job.status, Status.QUEUED)

    def test_enqueue_preserves_result_ttl(self):
        """Enqueueing persists result_ttl."""
        q = Queue()
        job = q.enqueue(div_by_zero, args=(1, 2, 3), result_ttl=10)
        self.assertEqual(job.result_ttl, 10)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(int(job_from_queue.result_ttl), 10)

    def test_async_false(self):
        """Executes a job immediately if async=False."""
        q = Queue(async=False)
        job = q.enqueue(some_calculation, args=(2, 3))
        self.assertEqual(job.return_value, 6)
