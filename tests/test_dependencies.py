from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello

from rq import Queue, SimpleWorker
from rq.job import Job, JobStatus, Dependency


class TestDependencies(RQTestCase):

    def test_allow_failure_is_persisted(self):
        """Ensure that job.allow_failure is properly set
        when providing Dependency object to depends_on."""
        dep_job = Job.create(func=say_hello)

        # default to False, maintaining current behavior
        job = Job.create(func=say_hello, depends_on=Dependency([dep_job]))
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertFalse(job.allow_failure)

        job = Job.create(func=say_hello, depends_on=Dependency([dep_job], allow_failure=True))
        job.save()
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertTrue(job.allow_failure)

        jobs = Job.fetch_many([job.id], connection=self.testconn)
        self.assertTrue(jobs[0].allow_failure)

    def test_job_dependency(self):
        """Enqueue dependent jobs only when appropriate"""
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)

        # enqueue dependent job when parent successfully finishes
        # parent_job = q.enqueue(say_hello)
        # job = q.enqueue_call(say_hello, depends_on=parent_job)
        # w.work(burst=True)
        # job = Job.fetch(job.id, connection=self.testconn)
        # self.assertEqual(job.get_status(), JobStatus.FINISHED)
        # q.empty()
        # # don't enqueue dependent job when parent fails
        # parent_job = q.enqueue(div_by_zero)
        # job = q.enqueue_call(say_hello, depends_on=parent_job)
        # w.work(burst=True)
        # job = Job.fetch(job.id, connection=self.testconn)
        # self.assertNotEqual(job.get_status(), JobStatus.FINISHED)

        # q.empty()

        # # don't enqueue dependent job when Dependency.allow_failure=False (the default)
        # parent_job = q.enqueue(div_by_zero)
        # dependency = Dependency(jobs=parent_job)
        # job = q.enqueue_call(say_hello, depends_on=dependency)
        # w.work(burst=True)
        # job = Job.fetch(job.id, connection=self.testconn)
        # self.assertNotEqual(job.get_status(), JobStatus.FINISHED)

        # enqueue dependent job when Dependency.allow_failure=True
        parent_job = q.enqueue(div_by_zero)
        dependency = Dependency(jobs=parent_job, allow_failure=True)
        job = q.enqueue_call(say_hello, depends_on=dependency)

        job = Job.fetch(job.id, connection=self.testconn)
        self.assertTrue(job.allow_failure)
        print('#####Dependent job ID', job.id)

        w.work(burst=True)
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)

        # When a failing job has multiple dependents, only enqueue those
        # with allow_failure=True
        parent_job = q.enqueue(div_by_zero)
        job_allow_failure = q.enqueue(say_hello,
                                      depends_on=Dependency(jobs=parent_job, allow_failure=True))
        job = q.enqueue(say_hello,
                        depends_on=Dependency(jobs=parent_job, allow_failure=False))
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
