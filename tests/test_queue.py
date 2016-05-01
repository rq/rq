# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from mock import patch
import os
from tests import RQTestCase
from tests.fixtures import (div_by_zero, echo, Number, say_hello,
                            some_calculation)

from rq.connections import RQConnection
from rq import Queue
from rq.exceptions import InvalidJobOperationError
from rq.job import Job, JobStatus
from rq.registry import DeferredJobRegistry
from rq.worker import Worker


class CustomJob(Job):
    pass


class TestQueue(RQTestCase):
    def test_create_queue(self):
        """Creating queues."""
        conn = RQConnection(redis_conn=self.testconn)
        q = conn.mkqueue('my-queue')
        self.assertEqual(q.name, 'my-queue')

    def test_create_default_queue(self):
        """Instantiating the default queue."""
        q = self.conn.mkqueue()
        self.assertEqual(q.name, 'default')

    def test_equality(self):
        """Mathematical equality of queues."""
        q1 = self.conn.mkqueue('foo')
        q2 = self.conn.mkqueue('foo')
        q3 = self.conn.mkqueue('bar')

        self.assertEqual(q1, q2)
        self.assertEqual(q2, q1)
        self.assertNotEqual(q1, q3)
        self.assertNotEqual(q2, q3)

    def test_empty_queue(self):
        """Emptying queues."""
        q = self.conn.mkqueue('example')

        self.testconn.rpush('rq:queue:example', 'foo')
        self.testconn.rpush('rq:queue:example', 'bar')
        self.assertEqual(q.is_empty(), False)

        q.empty()

        self.assertEqual(q.is_empty(), True)
        self.assertIsNone(self.testconn.lpop('rq:queue:example'))

    def test_empty_removes_jobs(self):
        """Emptying a queue deletes the associated job objects"""
        q = self.conn.mkqueue('example')
        job = q.enqueue(say_hello)

        self.assertTrue(self.testconn.exists('rq:job:'+job.id))
        q.empty()
        self.assertFalse(self.testconn.exists('rq:job:'+job.id))

    def test_queue_is_empty(self):
        """Detecting empty queues."""
        q = self.conn.mkqueue('example')
        self.assertEqual(q.is_empty(), True)

        self.testconn.rpush('rq:queue:example', 'sentinel message')
        self.assertEqual(q.is_empty(), False)

    def test_remove(self):
        """Ensure queue.remove properly removes Job from queue."""
        q = self.conn.mkqueue('example')
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
        q = self.conn.mkqueue('example')
        self.assertEqual(q.jobs, [])
        job = q.enqueue(say_hello)
        self.assertEqual(q.jobs, [job])

        # Deleting job removes it from queue
        job.delete()
        self.assertEqual(q.job_ids, [])

    def test_compact(self):
        """Queue.compact() removes non-existing jobs."""
        q = self.conn.mkqueue()

        q.enqueue(say_hello, 'Alice')
        q.enqueue(say_hello, 'Charlie')
        self.testconn.lpush(q.key, '1', '2')

        self.assertEqual(q.count, 4)
        self.assertEqual(len(q), 4)

        q.compact()

        self.assertEqual(q.count, 2)
        self.assertEqual(len(q), 2)

    def test_enqueue(self):
        """Enqueueing job onto queues."""
        q = self.conn.mkqueue()
        self.assertEqual(q.is_empty(), True)

        # say_hello spec holds which queue this is sent to
        job = q.enqueue(say_hello, 'Nick', foo='bar')
        job_id = job.id
        self.assertEqual(job.origin, q.name)

        # Inspect data inside Redis
        q_key = 'rq:queue:default'
        self.assertEqual(self.testconn.llen(q_key), 1)
        self.assertEqual(
            self.testconn.lrange(q_key, 0, -1)[0].decode('ascii'),
            job_id)

    def test_enqueue_sets_metadata(self):
        """Enqueueing job onto queues modifies meta data."""
        dummy_q = self.conn.mkqueue('dummy')
        dep = dummy_q.enqueue(say_hello)

        q = self.conn.mkqueue()
        job = q.enqueue(say_hello, 'Nick', foo='bar', depends_on=dep)

        # Preconditions
        self.assertIsNone(job.enqueued_at)

        # Action
        dep.perform()

        # Postconditions
        job.refresh()
        self.assertIsNotNone(job.enqueued_at)

    def test_pop_job_id(self):
        """Popping job IDs from queues."""
        # Set up
        q = self.conn.mkqueue()
        uuid = '112188ae-4e9d-4a5b-a5b3-f26f2cb054da'
        q.push_job_id(uuid)

        # Pop it off the queue...
        self.assertEqual(q.count, 1)
        self.assertEqual(self.conn._pop_job_id(q)[0], uuid)

        # ...and assert the queue count when down
        self.assertEqual(q.count, 0)

    def test_dequeue(self):
        """Dequeueing jobs from queues."""
        # Set up
        q = self.conn.mkqueue()
        result = q.enqueue(say_hello, 'Rick', foo='bar')

        # Dequeue a job (not a job ID) off the queue
        self.assertEqual(q.count, 1)
        job = q.dequeue()
        self.assertEqual(job.id, result.id)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, q.name)
        self.assertEqual(job.args[0], 'Rick')
        self.assertEqual(job.kwargs['foo'], 'bar')

        # ...and assert the queue count when down
        self.assertEqual(q.count, 0)

    def test_dequeue_deleted_jobs(self):
        """Dequeueing deleted jobs from queues don't blow the stack."""
        q = self.conn.mkqueue()
        for _ in range(1, 1000):
            job = q.enqueue(say_hello)
            job.delete()
        q.dequeue()

    def test_dequeue_instance_method(self):
        """Dequeueing instance method jobs from queues."""
        q = self.conn.mkqueue()
        n = Number(2)
        q.enqueue(n.div, 4)

        job = q.dequeue()

        # The instance has been pickled and unpickled, so it is now a separate
        # object. Test for equality using each object's __dict__ instead.
        self.assertEqual(job.instance.__dict__, n.__dict__)
        self.assertEqual(job.func.__name__, 'div')
        self.assertEqual(job.args, (4,))

    def test_dequeue_class_method(self):
        """Dequeueing class method jobs from queues."""
        q = self.conn.mkqueue()
        q.enqueue(Number.divide, 3, 4)

        job = q.dequeue()

        self.assertEqual(job.instance.__dict__, Number.__dict__)
        self.assertEqual(job.func.__name__, 'divide')
        self.assertEqual(job.args, (3, 4))

    def test_dequeue_ignores_nonexisting_jobs(self):
        """Dequeuing silently ignores non-existing jobs."""

        q = self.conn.mkqueue()
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)
        q.push_job_id(uuid)
        result = q.enqueue(say_hello, 'Nick', foo='bar')
        q.push_job_id(uuid)

        # Dequeue simply ignores the missing job and returns None
        self.assertEqual(q.count, 4)
        self.assertEqual(q.dequeue().id, result.id)
        self.assertIsNone(q.dequeue())
        self.assertEqual(q.count, 0)

    def test_dequeue_any(self):
        """Fetching work from any given queue."""
        fooq = self.conn.mkqueue('foo')
        barq = self.conn.mkqueue('bar')

        self.assertEqual(self.conn.dequeue_any([fooq, barq]), None)

        # Enqueue a single item
        barq.enqueue(say_hello)
        job, queue = self.conn.dequeue_any([fooq, barq])
        self.assertEqual(job.func, say_hello)
        self.assertEqual(queue, barq)

        # Enqueue items on both queues
        barq.enqueue(say_hello, 'for Bar')
        fooq.enqueue(say_hello, 'for Foo')

        job, queue = self.conn.dequeue_any([fooq, barq])
        self.assertEqual(queue, fooq)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, fooq.name)
        self.assertEqual(job.args[0], 'for Foo',
                         'Foo should be dequeued first.')

        job, queue = self.conn.dequeue_any([fooq, barq])
        self.assertEqual(queue, barq)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, barq.name)
        self.assertEqual(job.args[0], 'for Bar',
                         'Bar should be dequeued second.')

    def test_dequeue_any_ignores_nonexisting_jobs(self):
        """Dequeuing (from any queue) silently ignores non-existing jobs."""

        q = self.conn.mkqueue('low')
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)

        # Dequeue simply ignores the missing job and returns None
        self.assertEqual(q.count, 1)
        self.assertEqual(self.conn.dequeue_any(
            [self.conn.mkqueue(), self.conn.mkqueue('low')]), None) # noqa
        self.assertEqual(q.count, 0)

    def test_enqueue_sets_status(self):
        """Enqueueing a job sets its status to "queued"."""
        q = self.conn.mkqueue()
        job = q.enqueue(say_hello)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_enqueue_meta_arg(self):
        """enqueue() can set the job.meta contents."""
        q = self.conn.mkqueue()
        job = q.enqueue(say_hello, meta={'foo': 'bar', 'baz': 42})
        self.assertEqual(job.meta['foo'], 'bar')
        self.assertEqual(job.meta['baz'], 42)

    def test_enqueue_explicit_args(self):
        """enqueue() works for both implicit/explicit args."""
        q = self.conn.mkqueue()

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

    @patch('os.fork', return_value=0)
    @patch('os._exit')
    def test_all_queues(self, _1, _2):
        """All queues"""
        q1 = self.conn.mkqueue('first-queue')
        q2 = self.conn.mkqueue('second-queue')
        q3 = self.conn.mkqueue('third-queue')

        # Ensure a queue is added only once a job is enqueued
        self.assertEqual(len(self.conn.get_all_queues()), 0)
        q1.enqueue(say_hello)
        self.assertEqual(len(self.conn.get_all_queues()), 1)

        # Ensure this holds true for multiple queues
        q2.enqueue(say_hello)
        q3.enqueue(say_hello)
        names = [q.name for q in self.conn.get_all_queues()]
        self.assertEqual(len(self.conn.get_all_queues()), 3)

        # Verify names
        self.assertTrue('first-queue' in names)
        self.assertTrue('second-queue' in names)
        self.assertTrue('third-queue' in names)

        # Now empty two queues
        w = Worker([q2, q3], connection=self.conn)
        w.work(burst=True)

        # self.conn.get_all_queues() should still report the empty queues
        self.assertEqual(len(self.conn.get_all_queues()), 3)

    def test_enqueue_dependents(self):
        """
        Enqueueing dependent jobs pushes all jobs in the depends set to the queue
        and removes them from DeferredJobQueue.
        """
        parent_queue = self.conn.mkqueue('parent')
        parent_job = parent_queue.enqueue(say_hello)

        q = self.conn.mkqueue()
        job_1 = q.enqueue(say_hello, depends_on=parent_job)
        job_2 = q.enqueue(say_hello, depends_on=parent_job)

        # Jobs with unmet dependencies should be deferred, not queued
        registry = self.conn.get_deferred_registry(q.name)
        self.assertEqual(
            set(registry.get_job_ids()),
            set([job_1.id, job_2.id])
        )
        self.assertEqual(q.job_ids, [])

        parent_job._job_ended(0, None)

        # After job finishes, children should be enqueued
        self.assertEqual(set(q.job_ids), set([job_2.id, job_1.id]))
        self.assertFalse(self.testconn.exists(parent_job.children_key))

        # DeferredJobRegistry should also be empty
        self.assertEqual(registry.get_job_ids(), [])

    def test_enqueue_dependents_on_multiple_queues(self):
        """Enqueueing dependent jobs on multiple queues pushes jobs in the queues
        and removes them from DeferredJobRegistry for each different queue."""
        parent_queue = self.conn.mkqueue('parent')
        parent_job = parent_queue.enqueue(say_hello)

        q_1 = self.conn.mkqueue("queue_1")
        q_2 = self.conn.mkqueue("queue_2")
        job_1 = q_1.enqueue(say_hello, depends_on=parent_job)
        job_2 = q_2.enqueue(say_hello, depends_on=parent_job)

        # Each queue has its own DeferredJobRegistry
        registry_1 = self.conn.get_deferred_registry(q_1.name)
        self.assertEqual(
            set(registry_1.get_job_ids()),
            set([job_1.id])
        )
        registry_2 = self.conn.get_deferred_registry(q_2.name)
        self.assertEqual(
            set(registry_2.get_job_ids()),
            set([job_2.id])
        )

        # After dependents is enqueued, job_1 on queue_1 and
        # job_2 should be in queue_2
        self.assertEqual(q_1.job_ids, [])
        self.assertEqual(q_2.job_ids, [])

        parent_job._job_ended(0, None)

        self.assertEqual(set(q_1.job_ids), set([job_1.id]))
        self.assertEqual(set(q_2.job_ids), set([job_2.id]))
        self.assertFalse(self.testconn.exists(parent_job.children_key))

        # DeferredJobRegistry should also be empty
        self.assertEqual(registry_1.get_job_ids(), [])
        self.assertEqual(registry_2.get_job_ids(), [])

    def test_enqueue_job_with_dependency(self):
        """Jobs are enqueued only when their dependencies are finished."""
        # Job with unfinished dependency is not immediately enqueued
        parent_job = self.create_job(func=say_hello)
        parent_job.save()

        q = self.conn.mkqueue()
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(JobStatus.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, Queue.DEFAULT_TIMEOUT)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_enqueue_job_with_dependency_by_id(self):
        """Can specify job dependency with job object or job id."""
        parent_job = self.create_job(func=say_hello)
        parent_job.save()

        q = self.conn.mkqueue()
        q.enqueue_call(say_hello, depends_on=parent_job.id)
        self.assertEqual(q.job_ids, [])

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(JobStatus.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job.id)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, Queue.DEFAULT_TIMEOUT)

    def test_enqueue_job_with_dependency_and_timeout(self):
        """Jobs remember their timeout when enqueued as a dependency."""
        # Job with unfinished dependency is not immediately enqueued
        parent_queue = self.conn.mkqueue('parent')
        parent_job = parent_queue.enqueue(say_hello)

        q = self.conn.mkqueue()
        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=123)
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.timeout, 123)

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(JobStatus.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=123)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, 123)


class TestFailedQueue(RQTestCase):
    def test_requeue_job(self):
        """Requeueing existing jobs."""
        job = self.create_job(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.save()
        self.conn.get_failed_queue().quarantine(job, Exception('Some fake error'))  # noqa

        self.assertEqual(self.conn.get_all_queues(), [self.conn.get_failed_queue()])  # noqa
        self.assertEqual(self.conn.get_failed_queue().count, 1)

        self.conn.get_failed_queue().requeue(job.id)

        self.assertEqual(self.conn.get_failed_queue().count, 0)
        self.assertEqual(self.conn.mkqueue('fake').count, 1)

    def test_requeue_nonfailed_job_fails(self):
        """Requeueing non-failed jobs raises error."""
        q = self.conn.mkqueue()
        job = q.enqueue(say_hello, 'Nick', foo='bar')

        # Assert that we cannot requeue a job that's not on the failed queue
        with self.assertRaises(InvalidJobOperationError):
            self.conn.get_failed_queue().requeue(job.id)

    def test_quarantine_preserves_timeout(self):
        """Quarantine preserves job timeout."""
        job = self.create_job(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.timeout = 200
        job.save()
        self.conn.get_failed_queue().quarantine(job, Exception('Some fake error'))

        self.assertEqual(job.timeout, 200)

    def test_requeueing_preserves_timeout(self):
        """Requeueing preserves job timeout."""
        job = self.create_job(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.timeout = 200
        job.save()
        self.conn.get_failed_queue().quarantine(job, Exception('Some fake error'))
        self.conn.get_failed_queue().requeue(job.id)

        job = self.conn.get_job(job.id)
        self.assertEqual(job.timeout, 200)

    def test_requeue_sets_status_to_queued(self):
        """Requeueing a job should set its status back to QUEUED."""
        job = self.create_job(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.save()
        self.conn.get_failed_queue().quarantine(job, Exception('Some fake error'))
        self.conn.get_failed_queue().requeue(job.id)

        job = self.conn.get_job(job.id)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_enqueue_preserves_result_ttl(self):
        """Enqueueing persists result_ttl."""
        q = self.conn.mkqueue()
        job = q.enqueue(div_by_zero, args=(1, 2, 3), result_ttl=10)
        self.assertEqual(job.result_ttl, 10)
        job_from_queue = self.conn.get_job(job.id)
        self.assertEqual(int(job_from_queue.result_ttl), 10)

    def test_async_false(self):
        """Job executes and cleaned up immediately if async=False."""
        q = self.conn.mkqueue(async=False)
        job = q.enqueue(some_calculation, args=(2, 3))
        self.assertEqual(job.result, 6)
        self.assertNotEqual(self.testconn.ttl(job.key), -1)

    def test_skip_queue(self):
        """Ensure the skip_queue option functions"""
        q = self.conn.mkqueue('foo')
        job1 = q.enqueue(say_hello)
        job2 = q.enqueue(say_hello)
        assert q.dequeue() == job1
        skip_job = q.enqueue(say_hello, at_front=True)
        assert q.dequeue() == skip_job
        assert q.dequeue() == job2
