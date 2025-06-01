import time
from datetime import timedelta

from redis import WatchError

from rq.job import Dependency, Job, JobStatus, cancel_job
from rq.queue import Queue
from rq.registry import (
    CanceledJobRegistry,
)
from rq.serializers import JSONSerializer
from rq.utils import now
from rq.worker import Worker
from tests import RQTestCase, fixtures


class TestJobDependency(RQTestCase):
    def test_dependency_parameter_constraints(self):
        """Ensures the proper constraints are in place for values passed in as job references."""
        dep_job = Job.create(func=fixtures.say_hello, connection=self.connection)
        # raise error on empty jobs
        self.assertRaises(ValueError, Dependency, jobs=[])
        # raise error on non-str/Job value in jobs iterable
        self.assertRaises(ValueError, Dependency, jobs=[dep_job, 1])

    def test_multiple_dependencies_are_accepted_and_persisted(self):
        """Ensure job._dependency_ids accepts different input formats, and
        is set and restored properly"""
        job_A = Job.create(func=fixtures.some_calculation, args=(3, 1, 4), id='A', connection=self.connection)
        job_B = Job.create(func=fixtures.some_calculation, args=(2, 7, 2), id='B', connection=self.connection)

        # No dependencies
        job = Job.create(func=fixtures.say_hello, connection=self.connection)
        job.save()
        Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job._dependency_ids, [])

        # Various ways of specifying dependencies
        cases = [
            ['A', ['A']],
            [job_A, ['A']],
            [['A', 'B'], ['A', 'B']],
            [[job_A, job_B], ['A', 'B']],
            [['A', job_B], ['A', 'B']],
            [('A', 'B'), ['A', 'B']],
            [(job_A, job_B), ['A', 'B']],
            [(job_A, 'B'), ['A', 'B']],
            [Dependency('A'), ['A']],
            [Dependency(job_A), ['A']],
            [Dependency(['A', 'B']), ['A', 'B']],
            [Dependency([job_A, job_B]), ['A', 'B']],
            [Dependency(['A', job_B]), ['A', 'B']],
            [Dependency(('A', 'B')), ['A', 'B']],
            [Dependency((job_A, job_B)), ['A', 'B']],
            [Dependency((job_A, 'B')), ['A', 'B']],
        ]
        for given, expected in cases:
            job = Job.create(func=fixtures.say_hello, depends_on=given, connection=self.connection)
            job.save()
            Job.fetch(job.id, connection=self.connection)
            self.assertEqual(job._dependency_ids, expected)

    def test_cleanup_expires_dependency_keys(self):
        dependency_job = Job.create(func=fixtures.say_hello, connection=self.connection)
        dependency_job.save()

        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job, connection=self.connection)

        dependent_job.register_dependency()
        dependent_job.save()

        dependent_job.cleanup(ttl=100)
        dependency_job.cleanup(ttl=100)

        self.assertEqual(self.connection.ttl(dependent_job.dependencies_key), 100)
        self.assertEqual(self.connection.ttl(dependency_job.dependents_key), 100)

    def test_job_with_dependents_delete_parent(self):
        """job.delete() deletes itself from Redis but not dependents.
        Wthout a save, the dependent job is never saved into redis. The delete
        method will get and pass a NoSuchJobError.
        """
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(fixtures.say_hello)
        job2 = Job.create(
            func=fixtures.say_hello, depends_on=job, serializer=JSONSerializer, connection=self.connection
        )
        job2.register_dependency()

        job.delete()
        self.assertFalse(self.connection.exists(job.key))
        self.assertFalse(self.connection.exists(job.dependents_key))

        # By default, dependents are not deleted, but The job is in redis only
        # if it was saved!
        self.assertFalse(self.connection.exists(job2.key))

        self.assertNotIn(job.id, queue.get_job_ids())

    def test_job_with_dependents_delete_parent_with_saved(self):
        """job.delete() deletes itself from Redis but not dependents. If the
        dependent job was saved, it will remain in redis."""
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(fixtures.say_hello)
        job2 = Job.create(
            func=fixtures.say_hello, depends_on=job, serializer=JSONSerializer, connection=self.connection
        )
        job2.register_dependency()
        job2.save()

        job.delete()
        self.assertFalse(self.connection.exists(job.key))
        self.assertFalse(self.connection.exists(job.dependents_key))

        # By default, dependents are not deleted, but The job is in redis only
        # if it was saved!
        self.assertTrue(self.connection.exists(job2.key))

        self.assertNotIn(job.id, queue.get_job_ids())

    def test_job_with_dependents_deleteall(self):
        """job.delete() deletes itself from Redis. Dependents need to be
        deleted explicitly."""
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(fixtures.say_hello)
        job2 = Job.create(
            func=fixtures.say_hello, depends_on=job, serializer=JSONSerializer, connection=self.connection
        )
        job2.register_dependency()

        job.delete(delete_dependents=True)
        self.assertFalse(self.connection.exists(job.key))
        self.assertFalse(self.connection.exists(job.dependents_key))
        self.assertFalse(self.connection.exists(job2.key))

        self.assertNotIn(job.id, queue.get_job_ids())

    def test_job_with_dependents_delete_all_with_saved(self):
        """job.delete() deletes itself from Redis. Dependents need to be
        deleted explictely. Without a save, the dependent job is never saved
        into redis. The delete method will get and pass a NoSuchJobError.
        """
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(fixtures.say_hello)
        job2 = Job.create(
            func=fixtures.say_hello,
            depends_on=job,
            serializer=JSONSerializer,
            connection=self.connection,
            status=JobStatus.QUEUED,
        )
        job2.register_dependency()
        job2.save()

        job.delete(delete_dependents=True)
        self.assertFalse(self.connection.exists(job.key))
        self.assertFalse(self.connection.exists(job.dependents_key))
        self.assertFalse(self.connection.exists(job2.key))

        self.assertNotIn(job.id, queue.get_job_ids())

    def test_dependent_job_creates_dependencies_key(self):
        queue = Queue(connection=self.connection)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job, connection=self.connection)

        dependent_job.register_dependency()
        dependent_job.save()

        self.assertTrue(self.connection.exists(dependent_job.dependencies_key))

    def test_dependent_job_deletes_dependencies_key(self):
        """
        job.delete() deletes itself from Redis.
        """
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(
            func=fixtures.say_hello,
            depends_on=dependency_job,
            serializer=JSONSerializer,
            connection=self.connection,
            status=JobStatus.QUEUED,
        )

        dependent_job.register_dependency()
        dependent_job.save()
        dependent_job.delete()

        self.assertTrue(self.connection.exists(dependency_job.key))
        self.assertFalse(self.connection.exists(dependent_job.dependencies_key))
        self.assertFalse(self.connection.exists(dependent_job.key))

    def test_create_and_cancel_job_enqueue_dependents(self):
        """Ensure job.cancel() works properly with enqueue_dependents=True"""
        queue = Queue(connection=self.connection)
        dependency = queue.enqueue(fixtures.say_hello)
        dependent = queue.enqueue(fixtures.say_hello, depends_on=dependency)

        self.assertEqual(1, len(queue.get_jobs()))
        self.assertEqual(1, len(queue.deferred_job_registry))
        cancel_job(dependency.id, enqueue_dependents=True, connection=self.connection)
        self.assertEqual(1, len(queue.get_jobs()))
        self.assertEqual(0, len(queue.deferred_job_registry))
        registry = CanceledJobRegistry(connection=self.connection, queue=queue)
        self.assertIn(dependency, registry)
        self.assertEqual(dependency.get_status(), JobStatus.CANCELED)
        self.assertIn(dependent, queue.get_jobs())
        self.assertEqual(dependent.get_status(), JobStatus.QUEUED)
        # If job is deleted, it's also removed from CanceledJobRegistry
        dependency.delete()
        self.assertNotIn(dependency, registry)

    def test_create_and_cancel_job_enqueue_dependents_in_registry(self):
        """Ensure job.cancel() works properly with enqueue_dependents=True and when the job is in a registry"""
        queue = Queue(connection=self.connection)
        dependency = queue.enqueue(fixtures.raise_exc)
        dependent = queue.enqueue(fixtures.say_hello, depends_on=dependency)
        print('# Post enqueue', self.connection.smembers(dependency.dependents_key))
        self.assertTrue(dependency.dependent_ids)

        self.assertEqual(1, len(queue.get_jobs()))
        self.assertEqual(1, len(queue.deferred_job_registry))
        w = Worker([queue])
        w.work(burst=True, max_jobs=1)
        self.assertTrue(dependency.dependent_ids)
        print('# Post work', self.connection.smembers(dependency.dependents_key))
        dependency.refresh()
        dependent.refresh()
        self.assertEqual(0, len(queue.get_jobs()))
        self.assertEqual(1, len(queue.deferred_job_registry))
        self.assertEqual(1, len(queue.failed_job_registry))

        print('# Pre cancel', self.connection.smembers(dependency.dependents_key))
        cancel_job(dependency.id, enqueue_dependents=True, connection=self.connection)
        dependency.refresh()
        dependent.refresh()
        print('#Post cancel', self.connection.smembers(dependency.dependents_key))

        self.assertEqual(1, len(queue.get_jobs()))
        self.assertEqual(0, len(queue.deferred_job_registry))
        self.assertEqual(0, len(queue.failed_job_registry))
        self.assertEqual(1, len(queue.canceled_job_registry))
        registry = CanceledJobRegistry(connection=self.connection, queue=queue)
        self.assertIn(dependency, registry)
        self.assertEqual(dependency.get_status(), JobStatus.CANCELED)
        self.assertNotIn(dependency, queue.failed_job_registry)
        self.assertIn(dependent, queue.get_jobs())
        self.assertEqual(dependent.get_status(), JobStatus.QUEUED)
        # If job is deleted, it's also removed from CanceledJobRegistry
        dependency.delete()
        self.assertNotIn(dependency, registry)

    def test_create_and_cancel_job_enqueue_dependents_with_pipeline(self):
        """Ensure job.cancel() works properly with enqueue_dependents=True"""
        queue = Queue(connection=self.connection)
        dependency = queue.enqueue(fixtures.say_hello)
        dependent = queue.enqueue(fixtures.say_hello, depends_on=dependency)

        self.assertEqual(1, len(queue.get_jobs()))
        self.assertEqual(1, len(queue.deferred_job_registry))
        self.connection.set('some:key', b'some:value')

        with self.connection.pipeline() as pipe:
            pipe.watch('some:key')
            self.assertEqual(self.connection.get('some:key'), b'some:value')
            dependency.cancel(pipeline=pipe, enqueue_dependents=True)
            pipe.set('some:key', b'some:other:value')
            pipe.execute()
        self.assertEqual(self.connection.get('some:key'), b'some:other:value')
        self.assertEqual(1, len(queue.get_jobs()))
        self.assertEqual(0, len(queue.deferred_job_registry))
        registry = CanceledJobRegistry(connection=self.connection, queue=queue)
        self.assertIn(dependency, registry)
        self.assertEqual(dependency.get_status(), JobStatus.CANCELED)
        self.assertIn(dependent, queue.get_jobs())
        self.assertEqual(dependent.get_status(), JobStatus.QUEUED)
        # If job is deleted, it's also removed from CanceledJobRegistry
        dependency.delete()
        self.assertNotIn(dependency, registry)

    def test_canceling_job_removes_it_from_dependency_dependents_key(self):
        """Cancel child jobs and verify their IDs are removed from the parent's dependents_key."""
        connection = self.connection
        queue = Queue(connection=connection)
        parent_job = queue.enqueue(fixtures.say_hello, job_id='parent_job')
        child_job_1 = queue.enqueue(fixtures.say_hello, depends_on=parent_job, job_id='child_job_1')
        child_job_2 = queue.enqueue(fixtures.say_hello, depends_on=parent_job, job_id='child_job_2')

        self.assertEqual(set(parent_job.dependent_ids), {child_job_1.id, child_job_2.id})
        child_job_1.cancel(remove_from_dependencies=True)
        self.assertEqual(set(parent_job.dependent_ids), {child_job_2.id})
        # child_job_2 still in dependents_key since remove_from_dependencies = False
        child_job_2.cancel(remove_from_dependencies=False)
        self.assertEqual(set(parent_job.dependent_ids), {child_job_2.id})

    def test_dependents_key_for_should_return_prefixed_job_id(self):
        """test redis key to store job dependents hash under"""
        job_id = 'random'
        key = Job.dependents_key_for(job_id=job_id)

        assert key == Job.redis_job_namespace_prefix + job_id + ':dependents'

    def test_dependencies_key_should_have_prefixed_job_id(self):
        job_id = 'random'
        job = Job(id=job_id, connection=self.connection)
        expected_key = Job.redis_job_namespace_prefix + ':' + job_id + ':dependencies'

        assert job.dependencies_key == expected_key

    def test_fetch_dependencies_returns_dependency_jobs(self):
        queue = Queue(connection=self.connection)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job, connection=self.connection)

        dependent_job.register_dependency()
        dependent_job.save()

        dependencies = dependent_job.fetch_dependencies(pipeline=self.connection)

        self.assertListEqual(dependencies, [dependency_job])

    def test_fetch_dependencies_returns_empty_if_not_dependent_job(self):
        dependent_job = Job.create(func=fixtures.say_hello, connection=self.connection)

        dependent_job.register_dependency()
        dependent_job.save()

        dependencies = dependent_job.fetch_dependencies(pipeline=self.connection)

        self.assertListEqual(dependencies, [])

    def test_fetch_dependencies_raises_if_dependency_deleted(self):
        queue = Queue(connection=self.connection)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job, connection=self.connection)

        dependent_job.register_dependency()
        dependent_job.save()

        dependency_job.delete()

        self.assertNotIn(
            dependent_job.id, [job.id for job in dependent_job.fetch_dependencies(pipeline=self.connection)]
        )

    def test_fetch_dependencies_watches(self):
        queue = Queue(connection=self.connection)
        dependency_job = queue.enqueue(fixtures.say_hello)
        dependent_job = Job.create(func=fixtures.say_hello, depends_on=dependency_job, connection=self.connection)

        dependent_job.register_dependency()
        dependent_job.save()

        with self.connection.pipeline() as pipeline:
            dependent_job.fetch_dependencies(watch=True, pipeline=pipeline)

            pipeline.multi()

            with self.assertRaises(WatchError):
                self.connection.set(Job.key_for(dependency_job.id), 'somethingelsehappened')
                pipeline.touch(dependency_job.id)
                pipeline.execute()

    def test_dependencies_finished_returns_false_if_dependencies_queued(self):
        queue = Queue(connection=self.connection)

        dependency_job_ids = [queue.enqueue(fixtures.say_hello).id for _ in range(5)]

        dependent_job = Job.create(func=fixtures.say_hello, connection=self.connection)
        dependent_job._dependency_ids = dependency_job_ids
        dependent_job.register_dependency()

        dependencies_finished = dependent_job.dependencies_are_met()

        self.assertFalse(dependencies_finished)

    def test_dependencies_finished_returns_true_if_no_dependencies(self):
        dependent_job = Job.create(func=fixtures.say_hello, connection=self.connection)
        dependent_job.register_dependency()

        dependencies_finished = dependent_job.dependencies_are_met()

        self.assertTrue(dependencies_finished)

    def test_dependencies_finished_returns_true_if_all_dependencies_finished(self):
        dependency_jobs = [Job.create(fixtures.say_hello, connection=self.connection) for _ in range(5)]

        dependent_job = Job.create(func=fixtures.say_hello, connection=self.connection)
        dependent_job._dependency_ids = [job.id for job in dependency_jobs]
        dependent_job.register_dependency()

        right_now = now()

        # Set ended_at timestamps
        for i, job in enumerate(dependency_jobs):
            job._status = JobStatus.FINISHED
            job.ended_at = right_now - timedelta(seconds=i)
            job.save()

        dependencies_finished = dependent_job.dependencies_are_met()

        self.assertTrue(dependencies_finished)

    def test_dependencies_finished_returns_false_if_unfinished_job(self):
        dependency_jobs = [Job.create(fixtures.say_hello, connection=self.connection) for _ in range(2)]

        dependency_jobs[0]._status = JobStatus.FINISHED
        dependency_jobs[0].ended_at = now()
        dependency_jobs[0].save()

        dependency_jobs[1]._status = JobStatus.STARTED
        dependency_jobs[1].ended_at = None
        dependency_jobs[1].save()

        dependent_job = Job.create(func=fixtures.say_hello, connection=self.connection)
        dependent_job._dependency_ids = [job.id for job in dependency_jobs]
        dependent_job.register_dependency()

        dependencies_finished = dependent_job.dependencies_are_met()

        self.assertFalse(dependencies_finished)

    def test_dependencies_finished_watches_job(self):
        queue = Queue(connection=self.connection)

        dependency_job = queue.enqueue(fixtures.say_hello)

        dependent_job = Job.create(func=fixtures.say_hello, connection=self.connection)
        dependent_job._dependency_ids = [dependency_job.id]
        dependent_job.register_dependency()

        with self.connection.pipeline() as pipeline:
            dependent_job.dependencies_are_met(
                pipeline=pipeline,
            )

            dependency_job.set_status(JobStatus.FAILED, pipeline=self.connection)
            pipeline.multi()

            with self.assertRaises(WatchError):
                pipeline.touch(Job.key_for(dependent_job.id))
                pipeline.execute()

    def test_execution_order_with_sole_dependency(self):
        queue = Queue(connection=self.connection)
        key = 'test_job:job_order'

        connection_kwargs = self.connection.connection_pool.connection_kwargs
        # When there are no dependencies, the two fast jobs ("A" and "B") run in the order enqueued.
        # Worker 1 will be busy with the slow job, so worker 2 will complete both fast jobs.
        job_slow = queue.enqueue(fixtures.rpush, args=[key, 'slow', connection_kwargs, True, 0.5], job_id='slow_job')
        job_A = queue.enqueue(fixtures.rpush, args=[key, 'A', connection_kwargs, True])
        job_B = queue.enqueue(fixtures.rpush, args=[key, 'B', connection_kwargs, True])
        fixtures.burst_two_workers(queue, connection=self.connection)
        time.sleep(0.75)
        jobs_completed = [v.decode() for v in self.connection.lrange(key, 0, 2)]
        self.assertEqual(queue.count, 0)
        self.assertTrue(all(job.is_finished for job in [job_slow, job_A, job_B]))
        self.assertEqual(jobs_completed, ['A:w2', 'B:w2', 'slow:w1'])
        self.connection.delete(key)

        # When job "A" depends on the slow job, then job "B" finishes before "A".
        # There is no clear requirement on which worker should take job "A", so we stay silent on that.
        job_slow = queue.enqueue(fixtures.rpush, args=[key, 'slow', connection_kwargs, True, 0.5], job_id='slow_job')
        job_A = queue.enqueue(fixtures.rpush, args=[key, 'A', connection_kwargs, False], depends_on='slow_job')
        job_B = queue.enqueue(fixtures.rpush, args=[key, 'B', connection_kwargs, True])
        fixtures.burst_two_workers(queue, connection=self.connection)
        time.sleep(0.75)
        jobs_completed = [v.decode() for v in self.connection.lrange(key, 0, 2)]
        self.assertEqual(queue.count, 0)
        self.assertTrue(all(job.is_finished for job in [job_slow, job_A, job_B]))
        self.assertEqual(jobs_completed, ['B:w2', 'slow:w1', 'A'])

    def test_execution_order_with_dual_dependency(self):
        """Test that jobs with dependencies are executed in the correct order."""
        queue = Queue(connection=self.connection)
        key = 'test_job:job_order'
        connection_kwargs = self.connection.connection_pool.connection_kwargs
        # When there are no dependencies, the two fast jobs ("A" and "B") run in the order enqueued.
        job_slow_1 = queue.enqueue(fixtures.rpush, args=[key, 'slow_1', connection_kwargs, True, 0.5], job_id='slow_1')
        job_slow_2 = queue.enqueue(fixtures.rpush, args=[key, 'slow_2', connection_kwargs, True, 0.75], job_id='slow_2')
        job_A = queue.enqueue(fixtures.rpush, args=[key, 'A', connection_kwargs, True])
        job_B = queue.enqueue(fixtures.rpush, args=[key, 'B', connection_kwargs, True])
        fixtures.burst_two_workers(queue, connection=self.connection)
        time.sleep(1)
        jobs_completed = [v.decode() for v in self.connection.lrange(key, 0, 3)]
        self.assertEqual(queue.count, 0)
        self.assertTrue(all(job.is_finished for job in [job_slow_1, job_slow_2, job_A, job_B]))
        self.assertEqual(jobs_completed, ['slow_1:w1', 'A:w1', 'B:w1', 'slow_2:w2'])
        self.connection.delete(key)

        # This time job "A" depends on two slow jobs, while job "B" depends only on the faster of
        # the two. Job "B" should be completed before job "A".
        # There is no clear requirement on which worker should take job "A", so we stay silent on that.
        job_slow_1 = queue.enqueue(fixtures.rpush, args=[key, 'slow_1', connection_kwargs, True, 0.5], job_id='slow_1')
        job_slow_2 = queue.enqueue(fixtures.rpush, args=[key, 'slow_2', connection_kwargs, True, 0.75], job_id='slow_2')
        job_A = queue.enqueue(
            fixtures.rpush, args=[key, 'A', connection_kwargs, False], depends_on=['slow_1', 'slow_2']
        )
        job_B = queue.enqueue(fixtures.rpush, args=[key, 'B', connection_kwargs, True], depends_on=['slow_1'])
        fixtures.burst_two_workers(queue, connection=self.connection)
        time.sleep(1)
        jobs_completed = [v.decode() for v in self.connection.lrange(key, 0, 3)]
        self.assertEqual(queue.count, 0)
        self.assertTrue(all(job.is_finished for job in [job_slow_1, job_slow_2, job_A, job_B]))
        self.assertEqual(jobs_completed, ['slow_1:w1', 'B:w1', 'slow_2:w2', 'A'])
