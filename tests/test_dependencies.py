from tests import RQTestCase
from tests.fixtures import check_dependencies_are_met, div_by_zero, say_hello

from rq import Queue, SimpleWorker, Worker
from rq.job import Job, JobStatus, Dependency


class TestDependencies(RQTestCase):
    def test_allow_failure_is_persisted(self):
        """Ensure that job.allow_dependency_failures is properly set
        when providing Dependency object to depends_on."""
        dep_job = Job.create(func=say_hello)

        # default to False, maintaining current behavior
        job = Job.create(func=say_hello, depends_on=Dependency([dep_job]))
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertFalse(job.allow_dependency_failures)

        job = Job.create(func=say_hello, depends_on=Dependency([dep_job], allow_failure=True))
        job.save()
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertTrue(job.allow_dependency_failures)

        jobs = Job.fetch_many([job.id], connection=self.testconn)
        self.assertTrue(jobs[0].allow_dependency_failures)

    def test_job_dependency(self):
        """Enqueue dependent jobs only when appropriate"""
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)

        # enqueue dependent job when parent successfully finishes
        parent_job = q.enqueue(say_hello)
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        q.empty()

        # don't enqueue dependent job when parent fails
        parent_job = q.enqueue(div_by_zero)
        job = q.enqueue_call(say_hello, depends_on=parent_job)
        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertNotEqual(job.get_status(), JobStatus.FINISHED)
        q.empty()

        # don't enqueue dependent job when Dependency.allow_failure=False (the default)
        parent_job = q.enqueue(div_by_zero)
        dependency = Dependency(jobs=parent_job)
        job = q.enqueue_call(say_hello, depends_on=dependency)
        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertNotEqual(job.get_status(), JobStatus.FINISHED)

        # enqueue dependent job when Dependency.allow_failure=True
        parent_job = q.enqueue(div_by_zero)
        dependency = Dependency(jobs=parent_job, allow_failure=True)
        job = q.enqueue_call(say_hello, depends_on=dependency)

        job = Job.fetch(job.id, connection=self.testconn)
        self.assertTrue(job.allow_dependency_failures)

        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        # When a failing job has multiple dependents, only enqueue those
        # with allow_failure=True
        parent_job = q.enqueue(div_by_zero)
        job_allow_failure = q.enqueue(say_hello, depends_on=Dependency(jobs=parent_job, allow_failure=True))
        job = q.enqueue(say_hello, depends_on=Dependency(jobs=parent_job, allow_failure=False))
        w.work(burst=True, max_jobs=1)
        self.assertEqual(parent_job.get_status(), JobStatus.FAILED)
        self.assertEqual(job_allow_failure.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)
        q.empty()

        # only enqueue dependent job when all dependencies have finished/failed
        first_parent_job = q.enqueue(div_by_zero)
        second_parent_job = q.enqueue(say_hello)
        dependencies = Dependency(jobs=[first_parent_job, second_parent_job], allow_failure=True)
        job = q.enqueue_call(say_hello, depends_on=dependencies)
        w.work(burst=True, max_jobs=1)
        self.assertEqual(first_parent_job.get_status(), JobStatus.FAILED)
        self.assertEqual(second_parent_job.get_status(), JobStatus.QUEUED)
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)

        # When second job finishes, dependent job should be queued
        w.work(burst=True, max_jobs=1)
        self.assertEqual(second_parent_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        # Test dependant is enqueued at front
        q.empty()
        parent_job = q.enqueue(say_hello)
        q.enqueue(say_hello, job_id='fake_job_id_1', depends_on=Dependency(jobs=[parent_job]))
        q.enqueue(say_hello, job_id='fake_job_id_2', depends_on=Dependency(jobs=[parent_job], enqueue_at_front=True))
        # q.enqueue(say_hello) # This is a filler job that will act as a separator for jobs, one will be enqueued at front while the other one at the end of the queue
        w.work(burst=True, max_jobs=1)

        self.assertEqual(q.job_ids, ["fake_job_id_2", "fake_job_id_1"])

    def test_multiple_jobs_with_dependencies(self):
        """Enqueue dependent jobs only when appropriate"""
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)

        # enqueue dependent job when parent successfully finishes
        parent_job = q.enqueue(say_hello)
        job1 = Queue.prepare_data(say_hello, depends_on=parent_job)
        job2 = Queue.prepare_data(say_hello, depends_on=parent_job)

        jobs = q.enqueue_many([job1, job2])
        w.work(burst=True)
        job1 = Job.fetch(jobs[0].id, connection=self.testconn)
        job2 = Job.fetch(jobs[1].id, connection=self.testconn)
        self.assertEqual(job1.get_status(), JobStatus.FINISHED)
        self.assertEqual(job2.get_status(), JobStatus.FINISHED)
        q.empty()

        # don't enqueue dependent job when parent fails
        parent_job = q.enqueue(div_by_zero)
        job1 = Queue.prepare_data(say_hello, depends_on=parent_job)
        job2 = Queue.prepare_data(say_hello, depends_on=parent_job)

        jobs = q.enqueue_many([job1, job2])
        w.work(burst=True)
        job1 = Job.fetch(jobs[0].id, connection=self.testconn)
        job2 = Job.fetch(jobs[1].id, connection=self.testconn)
        self.assertNotEqual(job1.get_status(), JobStatus.FINISHED)
        self.assertNotEqual(job2.get_status(), JobStatus.FINISHED)
        q.empty()

        # don't enqueue dependent job when Dependency.allow_failure=False (the default)
        parent_job = q.enqueue(div_by_zero)
        dependency = Dependency(jobs=parent_job)
        job1 = Queue.prepare_data(say_hello, depends_on=dependency)
        job2 = Queue.prepare_data(say_hello, depends_on=dependency)

        jobs = q.enqueue_many([job1, job2])
        w.work(burst=True)
        job1 = Job.fetch(jobs[0].id, connection=self.testconn)
        job2 = Job.fetch(jobs[1].id, connection=self.testconn)
        self.assertNotEqual(job1.get_status(), JobStatus.FINISHED)
        self.assertNotEqual(job2.get_status(), JobStatus.FINISHED)

        # enqueue dependent job when Dependency.allow_failure=True
        parent_job = q.enqueue(div_by_zero)
        dependency = Dependency(jobs=parent_job, allow_failure=True)
        job1 = Queue.prepare_data(say_hello, depends_on=dependency)
        job2 = Queue.prepare_data(say_hello, depends_on=dependency)

        jobs = q.enqueue_many([job1, job2])
        w.work(burst=True)
        job1 = Job.fetch(jobs[0].id, connection=self.testconn)
        job2 = Job.fetch(jobs[1].id, connection=self.testconn)
        self.assertEqual(job1.get_status(), JobStatus.FINISHED)
        self.assertEqual(job2.get_status(), JobStatus.FINISHED)

        # When a failing job has multiple dependents, only enqueue those
        # with allow_failure=True
        parent_job = q.enqueue(div_by_zero)
        dependency_allow_failure = Dependency(jobs=parent_job, allow_failure=True)
        dependency = Dependency(jobs=parent_job, allow_failure=False)

        job_allow_failure = Queue.prepare_data(say_hello, depends_on=dependency_allow_failure)
        job = Queue.prepare_data(say_hello, depends_on=dependency)
        jobs = q.enqueue_many([job_allow_failure, job])

        w.work(burst=True, max_jobs=1)
        self.assertEqual(parent_job.get_status(), JobStatus.FAILED)
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[1].get_status(), JobStatus.DEFERRED)
        q.empty()

        # only enqueue dependent job when all dependencies have finished/failed
        first_parent_job = q.enqueue(div_by_zero)
        second_parent_job = q.enqueue(say_hello)
        dependencies = Dependency(jobs=[first_parent_job, second_parent_job], allow_failure=True)
        job1 = Queue.prepare_data(say_hello, depends_on=dependencies)
        job2 = Queue.prepare_data(say_hello, depends_on=dependencies)

        jobs = q.enqueue_many([job1, job2])
        w.work(burst=True, max_jobs=1)
        self.assertEqual(first_parent_job.get_status(), JobStatus.FAILED)
        self.assertEqual(second_parent_job.get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[0].get_status(), JobStatus.DEFERRED)
        self.assertEqual(jobs[1].get_status(), JobStatus.DEFERRED)

        # When second job finishes, dependent job should be queued
        w.work(burst=True, max_jobs=1)
        self.assertEqual(second_parent_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[1].get_status(), JobStatus.QUEUED)
        w.work(burst=True)
        job1 = Job.fetch(jobs[0].id, connection=self.testconn)
        job2 = Job.fetch(jobs[1].id, connection=self.testconn)
        self.assertEqual(job1.get_status(), JobStatus.FINISHED)
        self.assertEqual(job2.get_status(), JobStatus.FINISHED)
        q.empty()

        # Multiple jobs are enqueued with correct status
        parent_job = q.enqueue(say_hello)
        job_no_deps = Queue.prepare_data(say_hello)
        job_with_deps = Queue.prepare_data(say_hello, depends_on=parent_job)
        jobs = q.enqueue_many([job_no_deps, job_with_deps])
        self.assertEqual(jobs[0].get_status(), JobStatus.QUEUED)
        self.assertEqual(jobs[1].get_status(), JobStatus.DEFERRED)
        w.work(burst=True, max_jobs=1)
        self.assertEqual(jobs[1].get_status(), JobStatus.QUEUED)
        q.empty()

        # Dependent jobs are enqueued if dependencies are met
        parent_job = q.enqueue(say_hello)
        job = Queue.prepare_data(say_hello, depends_on=parent_job)
        w.work(burst=True, max_jobs=1)
        jobs = q.enqueue_many([job])
        job = Job.fetch(jobs[0].id, connection=self.testconn)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        q.empty()

        # Dependent jobs are deferred if dependencies fail
        parent_job = q.enqueue(div_by_zero)
        job = Queue.prepare_data(say_hello, depends_on=parent_job)
        w.work(burst=True, max_jobs=1)
        jobs = q.enqueue_many([job])
        job = Job.fetch(jobs[0].id, connection=self.testconn)
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)
        q.empty()

        # Dependent jobs are enqueued if dependencies fail and failures allowed
        parent_job = q.enqueue(div_by_zero)
        dependency = Dependency(parent_job, allow_failure=True)
        job = Queue.prepare_data(say_hello, depends_on=dependency)
        w.work(burst=True, max_jobs=1)
        jobs = q.enqueue_many([job])
        job = Job.fetch(jobs[0].id, connection=self.testconn)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        q.empty()

    def test_dependency_list_in_depends_on(self):
        """Enqueue with Dependency list in depends_on"""
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)

        # enqueue dependent job when parent successfully finishes
        parent_job1 = q.enqueue(say_hello)
        parent_job2 = q.enqueue(say_hello)
        job = q.enqueue_call(say_hello, depends_on=[Dependency([parent_job1]), Dependency([parent_job2])])
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_enqueue_job_dependency(self):
        """Enqueue via Queue.enqueue_job() with depencency"""
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)

        # enqueue dependent job when parent successfully finishes
        parent_job = Job.create(say_hello)
        parent_job.save()
        job = Job.create(say_hello, depends_on=parent_job)
        q.enqueue_job(job)
        w.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.DEFERRED)
        q.enqueue_job(parent_job)
        w.work(burst=True)
        self.assertEqual(parent_job.get_status(), JobStatus.FINISHED)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

    def test_dependencies_are_met_if_parent_is_canceled(self):
        """When parent job is canceled, it should be treated as failed"""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello)
        job.set_status(JobStatus.CANCELED)
        dependent_job = queue.enqueue(say_hello, depends_on=job)
        # dependencies_are_met() should return False, whether or not
        # parent_job is provided
        self.assertFalse(dependent_job.dependencies_are_met(job))
        self.assertFalse(dependent_job.dependencies_are_met())

    def test_can_enqueue_job_if_dependency_is_deleted(self):
        queue = Queue(connection=self.testconn)

        dependency_job = queue.enqueue(say_hello, result_ttl=0)

        w = Worker([queue])
        w.work(burst=True)

        assert queue.enqueue(say_hello, depends_on=dependency_job)

    def test_dependencies_are_met_if_dependency_is_deleted(self):
        queue = Queue(connection=self.testconn)

        dependency_job = queue.enqueue(say_hello, result_ttl=0)
        dependent_job = queue.enqueue(say_hello, depends_on=dependency_job)

        w = Worker([queue])
        w.work(burst=True, max_jobs=1)

        assert dependent_job.dependencies_are_met()
        assert dependent_job.get_status() == JobStatus.QUEUED

    def test_dependencies_are_met_at_execution_time(self):
        queue = Queue(connection=self.testconn)
        queue.empty()
        queue.enqueue(say_hello, job_id="A")
        queue.enqueue(say_hello, job_id="B")
        job_c = queue.enqueue(check_dependencies_are_met, job_id="C", depends_on=["A", "B"])

        job_c.dependencies_are_met()
        w = Worker([queue])
        w.work(burst=True)
        assert job_c.result
