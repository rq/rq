from tests import RQTestCase
from tests import testjob, failing_job
from tests.helpers import strip_milliseconds
from rq import Queue, Worker
from rq.job import Job


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

    def test_work_is_unreadable(self):
        """Unreadable jobs are put on the failure queue."""
        q = Queue()
        failure_q = Queue('failure')

        self.assertEquals(failure_q.count, 0)
        self.assertEquals(q.count, 0)

        # NOTE: We have to fake this enqueueing for this test case.
        # What we're simulating here is a call to a function that is not
        # importable from the worker process.
        job = Job.for_call(failing_job, 3)
        job.save()
        data = self.testconn.hget(job.key, 'data')
        invalid_data = data.replace('failing_job', 'nonexisting_job')
        self.testconn.hset(job.key, 'data', invalid_data)

        # We use the low-level internal function to enqueue any data (bypassing
        # validity checks)
        q.push_job_id(job.id)

        self.assertEquals(q.count, 1)

        # All set, we're going to process it
        w = Worker([q])
        w.work(burst=True)   # should silently pass
        self.assertEquals(q.count, 0)

        self.assertEquals(failure_q.count, 1)

    def test_work_fails(self):
        """Failing jobs are put on the failure queue."""
        q = Queue()
        failure_q = Queue('failure')

        self.assertEquals(failure_q.count, 0)
        self.assertEquals(q.count, 0)

        q.enqueue(failing_job)
        self.assertEquals(q.count, 1)

        w = Worker([q])
        w.work(burst=True)  # should silently pass
        self.assertEquals(q.count, 0)
        self.assertEquals(failure_q.count, 1)


