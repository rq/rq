import os
from tests import RQTestCase
from tests import testjob, failing_job
from tests.helpers import strip_milliseconds
from rq import Queue, Worker
from rq.job import Job


SENTINEL_FILE = '/tmp/rq-tests.txt'


def create_sentinel():
    # Create some evidence that the job ran
    with open(SENTINEL_FILE, 'w') as f:
        f.write('Just a sentinel.')


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
        """Unreadable jobs are put on the failed queue."""
        q = Queue()
        failed_q = Queue('failed')

        self.assertEquals(failed_q.count, 0)
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

        self.assertEquals(failed_q.count, 1)

    def test_work_fails(self):
        """Failing jobs are put on the failed queue."""
        q = Queue()
        failed_q = Queue('failed')

        # Preconditions
        self.assertEquals(failed_q.count, 0)
        self.assertEquals(q.count, 0)

        # Action
        job = q.enqueue(failing_job)
        self.assertEquals(q.count, 1)

        # keep for later
        enqueued_at_date = strip_milliseconds(job.enqueued_at)

        w = Worker([q])
        w.work(burst=True)  # should silently pass

        # Postconditions
        self.assertEquals(q.count, 0)
        self.assertEquals(failed_q.count, 1)

        # Check the job
        job = Job.fetch(job.id)
        self.assertEquals(job.origin, q.name)

        # should be the original enqueued_at date, not the date of enqueueing to
        # the failed queue
        self.assertEquals(job.enqueued_at, enqueued_at_date)
        self.assertIsNotNone(job.exc_info)  # should contain exc_info


    def test_cancelled_jobs_arent_executed(self):
        """Cancelling jobs."""
        try:
            # Remove the sentinel if it is leftover from a previous test run
            os.remove(SENTINEL_FILE)
        except OSError as e:
            if e.errno != 2:
                raise

        q = Queue()
        result = q.enqueue(create_sentinel)

        # Here, we cancel the job, so the sentinel file may not be created
        assert q.count == 1
        result.cancel()
        assert q.count == 1

        w = Worker([q])
        w.work(burst=True)
        assert q.count == 0

        # Should not have created evidence of execution
        self.assertEquals(os.path.exists(SENTINEL_FILE), False)

