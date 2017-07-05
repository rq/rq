# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from tests import RQTestCase
from tests.fixtures import (div_by_zero, echo, Number, say_hello,
                            some_calculation)

from rq import get_failed_queue, Queue
from rq.exceptions import InvalidJobDependency, InvalidJobOperationError
from rq.job import Job, JobStatus
from rq.registry import DeferredJobRegistry
from rq.worker import Worker


class CustomJob(Job):
    pass


class TestQueue(RQTestCase):
    def test_create_queue(self):
        """Creating queues."""
        q = Queue('my-queue')
        self.assertEqual(q.name, 'my-queue')
        self.assertEqual(str(q), '<Queue my-queue>')

    def test_create_default_queue(self):
        """Instantiating the default queue."""
        q = Queue()
        self.assertEqual(q.name, 'default')

    def test_equality(self):
        """Mathematical equality of queues."""
        q1 = Queue('foo')
        q2 = Queue('foo')
        q3 = Queue('bar')

        self.assertEqual(q1, q2)
        self.assertEqual(q2, q1)
        self.assertNotEqual(q1, q3)
        self.assertNotEqual(q2, q3)
        self.assertGreater(q1, q3)
        self.assertRaises(TypeError, lambda: q1 == 'some string')
        self.assertRaises(TypeError, lambda: q1 < 'some string')

    def test_empty_queue(self):
        """Emptying queues."""
        q = Queue('example')

        self.testconn.rpush('rq:queue:example', 'foo')
        self.testconn.rpush('rq:queue:example', 'bar')
        self.assertEqual(q.is_empty(), False)

        q.empty()

        self.assertEqual(q.is_empty(), True)
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
        self.assertEqual(q.is_empty(), True)

        self.testconn.rpush('rq:queue:example', 'sentinel message')
        self.assertEqual(q.is_empty(), False)

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

        # Deleting job removes it from queue
        job.delete()
        self.assertEqual(q.job_ids, [])

    def test_compact(self):
        """Queue.compact() removes non-existing jobs."""
        q = Queue()

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
        q = Queue()
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
        q = Queue()
        job = Job.create(func=say_hello, args=('Nick',), kwargs=dict(foo='bar'))

        # Preconditions
        self.assertIsNone(job.enqueued_at)

        # Action
        q.enqueue_job(job)

        # Postconditions
        self.assertIsNotNone(job.enqueued_at)

    def test_pop_job_id(self):
        """Popping job IDs from queues."""
        # Set up
        q = Queue()
        uuid = '112188ae-4e9d-4a5b-a5b3-f26f2cb054da'
        q.push_job_id(uuid)

        # Pop it off the queue...
        self.assertEqual(q.count, 1)
        self.assertEqual(q.pop_job_id(), uuid)

        # ...and assert the queue count when down
        self.assertEqual(q.count, 0)

    def test_dequeue(self):
        """Dequeueing jobs from queues."""
        # Set up
        q = Queue()
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
        q = Queue()
        for _ in range(1, 1000):
            job = q.enqueue(say_hello)
            job.delete()
        q.dequeue()

    def test_dequeue_instance_method(self):
        """Dequeueing instance method jobs from queues."""
        q = Queue()
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
        q = Queue()
        q.enqueue(Number.divide, 3, 4)

        job = q.dequeue()

        self.assertEqual(job.instance.__dict__, Number.__dict__)
        self.assertEqual(job.func.__name__, 'divide')
        self.assertEqual(job.args, (3, 4))

    def test_dequeue_ignores_nonexisting_jobs(self):
        """Dequeuing silently ignores non-existing jobs."""

        q = Queue()
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
        fooq = Queue('foo')
        barq = Queue('bar')

        self.assertEqual(Queue.dequeue_any([fooq, barq], None), None)

        # Enqueue a single item
        barq.enqueue(say_hello)
        job, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(queue, barq)

        # Enqueue items on both queues
        barq.enqueue(say_hello, 'for Bar')
        fooq.enqueue(say_hello, 'for Foo')

        job, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEqual(queue, fooq)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, fooq.name)
        self.assertEqual(
            job.args[0], 'for Foo',
            'Foo should be dequeued first.'
        )

        job, queue = Queue.dequeue_any([fooq, barq], None)
        self.assertEqual(queue, barq)
        self.assertEqual(job.func, say_hello)
        self.assertEqual(job.origin, barq.name)
        self.assertEqual(
            job.args[0], 'for Bar',
            'Bar should be dequeued second.'
        )

    def test_dequeue_any_ignores_nonexisting_jobs(self):
        """Dequeuing (from any queue) silently ignores non-existing jobs."""

        q = Queue('low')
        uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
        q.push_job_id(uuid)

        # Dequeue simply ignores the missing job and returns None
        self.assertEqual(q.count, 1)
        self.assertEqual(
            Queue.dequeue_any([Queue(), Queue('low')], None),  # noqa
            None
        )
        self.assertEqual(q.count, 0)

    def test_enqueue_sets_status(self):
        """Enqueueing a job sets its status to "queued"."""
        q = Queue()
        job = q.enqueue(say_hello)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_enqueue_meta_arg(self):
        """enqueue() can set the job.meta contents."""
        q = Queue()
        job = q.enqueue(say_hello, meta={'foo': 'bar', 'baz': 42})
        self.assertEqual(job.meta['foo'], 'bar')
        self.assertEqual(job.meta['baz'], 42)

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
        self.assertEqual(len(Queue.all()), 0)
        q1.enqueue(say_hello)
        self.assertEqual(len(Queue.all()), 1)

        # Ensure this holds true for multiple queues
        q2.enqueue(say_hello)
        q3.enqueue(say_hello)
        names = [q.name for q in Queue.all()]
        self.assertEqual(len(Queue.all()), 3)

        # Verify names
        self.assertTrue('first-queue' in names)
        self.assertTrue('second-queue' in names)
        self.assertTrue('third-queue' in names)

        # Now empty two queues
        w = Worker([q2, q3])
        w.work(burst=True)

        # Queue.all() should still report the empty queues
        self.assertEqual(len(Queue.all()), 3)

    def test_all_custom_job(self):
        class CustomJob(Job):
            pass

        q = Queue('all-queue')
        q.enqueue(say_hello)
        queues = Queue.all(job_class=CustomJob)
        self.assertEqual(len(queues), 1)
        self.assertIs(queues[0].job_class, CustomJob)

    def test_from_queue_key(self):
        """Ensure being able to get a Queue instance manually from Redis"""
        q = Queue()
        key = Queue.redis_queue_namespace_prefix + 'default'
        reverse_q = Queue.from_queue_key(key)
        self.assertEqual(q, reverse_q)

    def test_from_queue_key_error(self):
        """Ensure that an exception is raised if the queue prefix is wrong"""
        key = 'some:weird:prefix:' + 'default'
        self.assertRaises(ValueError, Queue.from_queue_key, key)

    def test_enqueue_dependents(self):
        """Enqueueing dependent jobs pushes all jobs in the depends set to the queue
        and removes them from DeferredJobQueue."""
        q = Queue()
        parent_job = Job.create(func=say_hello)
        parent_job.save()
        job_1 = q.enqueue(say_hello, depends_on=parent_job)
        job_2 = q.enqueue(say_hello, depends_on=parent_job)

        registry = DeferredJobRegistry(q.name, connection=self.testconn)
        self.assertEqual(
            set(registry.get_job_ids()),
            set([job_1.id, job_2.id])
        )
        # After dependents is enqueued, job_1 and job_2 should be in queue
        self.assertEqual(q.job_ids, [])
        q.enqueue_dependents(parent_job)
        self.assertEqual(set(q.job_ids), set([job_2.id, job_1.id]))
        self.assertFalse(self.testconn.exists(parent_job.dependents_key))

        # DeferredJobRegistry should also be empty
        self.assertEqual(registry.get_job_ids(), [])

    def test_enqueue_dependents_on_multiple_queues(self):
        """Enqueueing dependent jobs on multiple queues pushes jobs in the queues
        and removes them from DeferredJobRegistry for each different queue."""
        q_1 = Queue("queue_1")
        q_2 = Queue("queue_2")
        parent_job = Job.create(func=say_hello)
        parent_job.save()
        job_1 = q_1.enqueue(say_hello, depends_on=parent_job)
        job_2 = q_2.enqueue(say_hello, depends_on=parent_job)

        # Each queue has its own DeferredJobRegistry
        registry_1 = DeferredJobRegistry(q_1.name, connection=self.testconn)
        self.assertEqual(
            set(registry_1.get_job_ids()),
            set([job_1.id])
        )
        registry_2 = DeferredJobRegistry(q_2.name, connection=self.testconn)
        self.assertEqual(
            set(registry_2.get_job_ids()),
            set([job_2.id])
        )

        # After dependents is enqueued, job_1 on queue_1 and
        # job_2 should be in queue_2
        self.assertEqual(q_1.job_ids, [])
        self.assertEqual(q_2.job_ids, [])
        q_1.enqueue_dependents(parent_job)
        q_2.enqueue_dependents(parent_job)
        self.assertEqual(set(q_1.job_ids), set([job_1.id]))
        self.assertEqual(set(q_2.job_ids), set([job_2.id]))
        self.assertFalse(self.testconn.exists(parent_job.dependents_key))

        # DeferredJobRegistry should also be empty
        self.assertEqual(registry_1.get_job_ids(), [])
        self.assertEqual(registry_2.get_job_ids(), [])

    def test_enqueue_job_with_dependency(self):
        """Jobs are enqueued only when their dependencies are finished."""
        # Job with unfinished dependency is not immediately enqueued
        parent_job = Job.create(func=say_hello)
        parent_job.save()
        q = Queue()
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

    def test_enqueue_job_with_multiple_dependencies_same_q(self):
        """Jobs are enqueued only when all their dependencies are finished."""
        # Job with unfinished dependency is not immediately enqueued
        parent_job1 = Job.create(func=say_hello)
        parent_job1.save()
        parent_job2 = Job.create(func=say_hello)
        parent_job2.save()
        q = Queue()
        job = q.enqueue_call(say_hello, depends_on=[parent_job1, parent_job2])
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # run parent 1
        q.enqueue_job(parent_job1)
        w = Worker([q])
        w.work(burst=True)

        # child should still be waiting
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # run parent 2
        q.enqueue_job(parent_job2)
        w = Worker([q])
        w.work(burst=True)

        # child should be done
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_enqueue_job_with_multiple_dependencies_different_q(self):
        """Jobs can be enqueued on different queues. Dependency still works"""
        # Job with unfinished dependency is not immediately enqueued
        parent_job1 = Job.create(func=say_hello)
        parent_job1.save()
        parent_job2 = Job.create(func=say_hello)
        parent_job2.save()
        q1 = Queue()
        q2 = Queue()
        job = q2.enqueue_call(say_hello, depends_on=[parent_job1, parent_job2])
        self.assertEqual(q1.job_ids, [])
        self.assertEqual(q2.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # run parent 1
        q1.enqueue_job(parent_job1)
        w = Worker([q1, q2])
        w.work(burst=True)

        # child should still be waiting
        self.assertEqual(q1.job_ids, [])
        self.assertEqual(q2.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # parent 2 has no state yet
        self.assertEqual(parent_job2.get_status(), None)

        # run parent 2
        q1.enqueue_job(parent_job2)
        w = Worker([q1, q2])
        w.work(burst=True)

        # child should be done
        self.assertEqual(q1.job_ids, [])
        self.assertEqual(q2.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_enqueue_job_with_multiple_dependencies_different_q_by_id(self):
        """Jobs can be enqueued on different queues. Dependency still works.
        Enqueuing can be done using job ids as well"""
        # Job with unfinished dependency is not immediately enqueued
        parent_job1 = Job.create(func=say_hello)
        parent_job1.save()
        parent_job2 = Job.create(func=say_hello)
        parent_job2.save()
        q1 = Queue()
        q2 = Queue()
        job = q2.enqueue_call(say_hello, depends_on=[parent_job1.id, parent_job2.id])
        self.assertEqual(q1.job_ids, [])
        self.assertEqual(q2.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # run parent 1
        q1.enqueue_job(parent_job1)
        w = Worker([q1, q2])
        w.work(burst=True)

        # child should still be waiting
        self.assertEqual(q1.job_ids, [])
        self.assertEqual(q2.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # parent 2 has no state yet
        self.assertEqual(parent_job2.get_status(), None)

        # run parent 2
        q1.enqueue_job(parent_job2)
        w = Worker([q1, q2])
        w.work(burst=True)

        # child should be done
        self.assertEqual(q1.job_ids, [])
        self.assertEqual(q2.job_ids, [])
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_enqueue_job_with_dependency_by_id(self):
        """Can specify job dependency with job object or job id."""
        parent_job = Job.create(func=say_hello)
        parent_job.save()

        q = Queue()
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
        parent_job = Job.create(func=say_hello)
        parent_job.save()
        q = Queue()
        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=123)
        self.assertEqual(q.job_ids, [])
        self.assertEqual(job.timeout, 123)

        # Jobs dependent on finished jobs are immediately enqueued
        parent_job.set_status(JobStatus.FINISHED)
        parent_job.save()
        job = q.enqueue_call(say_hello, depends_on=parent_job, timeout=123)
        self.assertEqual(q.job_ids, [job.id])
        self.assertEqual(job.timeout, 123)

    def test_enqueue_job_with_invalid_dependency(self):
        """Enqueuing a job fails, if the dependency does not exist at all."""
        parent_job = Job.create(func=say_hello)
        # without save() the job is not visible to others

        q = Queue()
        with self.assertRaises(InvalidJobDependency):
            q.enqueue_call(say_hello, depends_on=parent_job)

        with self.assertRaises(InvalidJobDependency):
            q.enqueue_call(say_hello, depends_on=parent_job.id)

        self.assertEqual(q.job_ids, [])

    def test_fetch_job_successful(self):
        """Fetch a job from a queue."""
        q = Queue('example')
        job_orig = q.enqueue(say_hello)
        job_fetch = q.fetch_job(job_orig.id)
        self.assertIsNotNone(job_fetch)
        self.assertEqual(job_orig.id, job_fetch.id)
        self.assertEqual(job_orig.description, job_fetch.description)

    def test_fetch_job_missing(self):
        """Fetch a job from a queue which doesn't exist."""
        q = Queue('example')
        job = q.fetch_job('123')
        self.assertIsNone(job)

    def test_fetch_job_different_queue(self):
        """Fetch a job from a queue which is in a different queue."""
        q1 = Queue('example1')
        q2 = Queue('example2')
        job_orig = q1.enqueue(say_hello)
        job_fetch = q2.fetch_job(job_orig.id)
        self.assertIsNone(job_fetch)

        job_fetch = q1.fetch_job(job_orig.id)
        self.assertIsNotNone(job_fetch)


class TestFailedQueue(RQTestCase):
    def test_get_failed_queue(self):
        """Use custom job class"""
        class CustomJob(Job):
            pass
        failed_queue = get_failed_queue(job_class=CustomJob)
        self.assertIs(failed_queue.job_class, CustomJob)

        failed_queue = get_failed_queue(job_class='rq.job.Job')
        self.assertIsNot(failed_queue.job_class, CustomJob)

    def test_requeue_job(self):
        """Requeueing existing jobs."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))  # noqa

        self.assertEqual(Queue.all(), [get_failed_queue()])  # noqa
        self.assertEqual(get_failed_queue().count, 1)

        requeued_job = get_failed_queue().requeue(job.id)

        self.assertEqual(get_failed_queue().count, 0)
        self.assertEqual(Queue('fake').count, 1)
        self.assertEqual(requeued_job.origin, job.origin)

    def test_get_job_on_failed_queue(self):
        default_queue = Queue()
        failed_queue = get_failed_queue()

        job = default_queue.enqueue(div_by_zero, args=(1, 2, 3))

        job_on_default_queue = default_queue.fetch_job(job.id)
        job_on_failed_queue = failed_queue.fetch_job(job.id)

        self.assertIsNotNone(job_on_default_queue)
        self.assertIsNone(job_on_failed_queue)

        job.set_status(JobStatus.FAILED)

        job_on_default_queue = default_queue.fetch_job(job.id)
        job_on_failed_queue = failed_queue.fetch_job(job.id)

        self.assertIsNotNone(job_on_default_queue)
        self.assertIsNotNone(job_on_failed_queue)
        self.assertTrue(job_on_default_queue.is_failed)

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

        self.assertEqual(job.timeout, 200)

    def test_requeueing_preserves_timeout(self):
        """Requeueing preserves job timeout."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.timeout = 200
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))
        get_failed_queue().requeue(job.id)

        job = Job.fetch(job.id)
        self.assertEqual(job.timeout, 200)

    def test_requeue_sets_status_to_queued(self):
        """Requeueing a job should set its status back to QUEUED."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))
        get_failed_queue().requeue(job.id)

        job = Job.fetch(job.id)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_enqueue_preserves_result_ttl(self):
        """Enqueueing persists result_ttl."""
        q = Queue()
        job = q.enqueue(div_by_zero, args=(1, 2, 3), result_ttl=10)
        self.assertEqual(job.result_ttl, 10)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(int(job_from_queue.result_ttl), 10)

    def test_async_false(self):
        """Job executes and cleaned up immediately if async=False."""
        q = Queue(async=False)
        job = q.enqueue(some_calculation, args=(2, 3))
        self.assertEqual(job.return_value, 6)
        self.assertNotEqual(self.testconn.ttl(job.key), -1)

    def test_custom_job_class(self):
        """Ensure custom job class assignment works as expected."""
        q = Queue(job_class=CustomJob)
        self.assertEqual(q.job_class, CustomJob)

    def test_skip_queue(self):
        """Ensure the skip_queue option functions"""
        q = Queue('foo')
        job1 = q.enqueue(say_hello)
        job2 = q.enqueue(say_hello)
        assert q.dequeue() == job1
        skip_job = q.enqueue(say_hello, at_front=True)
        assert q.dequeue() == skip_job
        assert q.dequeue() == job2

    def test_job_deletion(self):
        """Ensure job.delete() removes itself from FailedQueue."""
        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.timeout = 200
        job.save()

        job.set_status(JobStatus.FAILED)

        failed_queue = get_failed_queue()
        failed_queue.quarantine(job, Exception('Some fake error'))

        self.assertTrue(job.id in failed_queue.get_job_ids())

        job.delete()
        self.assertFalse(job.id in failed_queue.get_job_ids())

    def test_job_in_failed_queue_persists(self):
        """Make sure failed job key does not expire"""
        q = Queue('foo')
        job = q.enqueue(div_by_zero, args=(1,), ttl=5)
        self.assertEqual(self.testconn.ttl(job.key), 5)

        self.assertRaises(ZeroDivisionError, job.perform)
        job.set_status(JobStatus.FAILED)
        failed_queue = get_failed_queue()
        failed_queue.quarantine(job, Exception('Some fake error'))

        self.assertEqual(self.testconn.ttl(job.key), -1)
