# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from rq import get_failed_queue, Queue
from rq.exceptions import InvalidJobOperationError, NoSuchJobError
from rq.job import Job, Status
from rq.worker import Worker

from tests import RQTestCase
from tests.fixtures import (div_by_zero, echo, Number, say_hello,
                            some_calculation)


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

    def test_empty_removes_jobs(self):
        """Emptying a queue deletes the associated job objects"""
        q = Queue('example')
        job = q.enqueue(say_hello)
        self.assertTrue(Job.exists(job.id))
        q.empty()
        self.assertFalse(Job.exists(job.id))

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

        with self.assertRaises(NoSuchJobError):
            job.refresh()

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

    def test_remember_past_jobs(self):
        """Getting past jobs from a queue."""
        q = Queue('example', remember_past_jobs=True)
        self.assertEqual(q.jobs, [])
        job = q.enqueue(say_hello)
        self.assertEqual(q.jobs, [job])

        # Completing a job does not remove it from the job list.
        w = Worker([q])
        w.work(burst=True)
        self.assertEqual(q.jobs, [job])
        self.assertEqual(job.get_status(), Status.FINISHED)

        # Deleting a job removes it from the job list.
        job.delete()
        self.assertEqual(q.job_ids, [])

    def test_compact(self):
        """Compacting queueus."""
        q = Queue()

        q.enqueue(say_hello, 'Alice')
        bob = q.enqueue(say_hello, 'Bob')
        q.enqueue(say_hello, 'Charlie')
        debrah = q.enqueue(say_hello, 'Debrah')

        bob.cancel()
        debrah.cancel()

        self.assertEquals(q.count, 4)

        q.compact()

        self.assertEquals(q.count, 2)

    def test_enqueue(self):
        """Enqueueing job onto queues."""
        q = Queue(remember_past_jobs=True)
        self.assertEquals(q.is_empty(), True)

        # say_hello spec holds which queue this is sent to
        job = q.enqueue(say_hello, 'Nick', foo='bar')
        job_id = job.id

        # Inspect data inside Redis
        q_key = 'rq:queue:default'
        self.assertEquals(self.testconn.llen(q_key), 1)
        self.assertEquals(
            self.testconn.lrange(q_key, 0, -1)[0].decode('ascii'),
            job_id)

        q_past_jobs_key = 'rq:queue:_pastjobs:default'
        self.assertTrue(self.testconn.exists(q_past_jobs_key))
        self.assertEquals(self.testconn.scard(q_past_jobs_key), 1)
        self.assertEquals(
            self.testconn.spop(q_past_jobs_key).decode('ascii'),
            job_id)

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
        barq = Queue('bar', remember_past_jobs=True)

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
        self.assertEqual(job.get_status(), Status.QUEUED)

    def test_enqueue_explicit_args(self):
        """enqueue() works for both implicit/explicit args."""
        q = Queue()

        # Implicit args/kwargs mode
        job = q.enqueue(echo, 1, timeout=1, result_ttl=1, bar='baz')
        self.assertEqual(job.timeout, 1)
        self.assertEqual(job.result_ttl, 1)
        self.assertEqual(
            job.perform(),
            ((1,), {'bar': 'baz'})
        )

        # Explicit kwargs mode
        kwargs = {
            'timeout': 1,
            'result_ttl': 1,
        }
        job = q.enqueue(echo, timeout=2, result_ttl=2, args=[1], kwargs=kwargs)
        self.assertEqual(job.timeout, 2)
        self.assertEqual(job.result_ttl, 2)
        self.assertEqual(
            job.perform(),
            ((1,), {'timeout': 1, 'result_ttl': 1})
        )

    def test_all_queues(self):
        """All queues"""
        q1 = Queue('first-queue')
        q2 = Queue('second-queue')
        q3 = Queue('third-queue')

        # Ensure a queue is added only once a job is enqueued
        self.assertEquals(len(Queue.all()), 0)
        q1.enqueue(say_hello)
        self.assertEquals(len(Queue.all()), 1)

        # Ensure this holds true for multiple queues
        q2.enqueue(say_hello)
        q3.enqueue(say_hello)
        names = [q.name for q in Queue.all()]
        self.assertEquals(len(Queue.all()), 3)

        # Verify names
        self.assertTrue('first-queue' in names)
        self.assertTrue('second-queue' in names)
        self.assertTrue('third-queue' in names)

        # Now empty two queues
        w = Worker([q2, q3])
        w.work(burst=True)

        # Queue.all() should still report the empty queues
        self.assertEquals(len(Queue.all()), 3)

    def test_enqueue_dependents(self):
        """Enqueueing the dependent jobs pushes all jobs in the depends set to the queue."""
        q = Queue()
        parent_job = Job.create(func=say_hello)
        parent_job.save()
        job_1 = Job.create(func=say_hello, depends_on=parent_job)
        job_1.save()
        job_1.register_dependency()
        job_2 = Job.create(func=say_hello, depends_on=parent_job)
        job_2.save()
        job_2.register_dependency()

        # After dependents is enqueued, job_1 and job_2 should be in queue
        self.assertEqual(q.job_ids, [])
        q.enqueue_dependents(parent_job)
        self.assertEqual(set(q.job_ids), set([job_1.id, job_2.id]))
        self.assertFalse(self.testconn.exists(parent_job.dependents_key))

    def test_enqueue_job_with_dependency(self):
        """Jobs are enqueued only when their dependencies are finished."""
        # Job with unfinished dependency is not immediately enqueued
        parent_job = Job.create(func=say_hello)
        q = Queue()
        q.enqueue_call(say_hello, depends_on=parent_job)
        self.assertEqual(q.job_ids, [])

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(Status.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, Queue.DEFAULT_TIMEOUT)

    def test_enqueue_job_with_dependency_and_timeout(self):
        """Jobs still know their specified timeout after being scheduled as a dependency."""
        # Job with unfinished dependency is not immediately enqueued
        parent_job = Job.create(func=say_hello)
        q = Queue()
        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=123)
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.timeout, 123)

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(Status.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=123)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, 123)


class TestFailedQueue(RQTestCase):
    def test_requeue_job(self):
        """Requeueing existing jobs."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))  # noqa

        self.assertEqual(Queue.all(), [get_failed_queue()])  # noqa
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
        self.assertEqual(job.get_status(), Status.QUEUED)

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
