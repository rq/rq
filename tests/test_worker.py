import os
from tests import RQTestCase, slow
from tests.fixtures import say_hello, div_by_zero, do_nothing, create_file, \
        create_file_after_timeout
from tests.helpers import strip_milliseconds
from rq import Queue, Worker, get_failed_queue
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
        self.assertEquals(w.work(burst=True), False,
                'Did not expect any work on the queue.')

        fooq.enqueue(say_hello, name='Frank')
        self.assertEquals(w.work(burst=True), True,
                'Expected at least some work done.')

    def test_work_via_string_argument(self):
        """Worker processes work fed via string arguments."""
        q = Queue('foo')
        w = Worker([q])
        result = q.enqueue('tests.fixtures.say_hello', name='Frank')
        self.assertEquals(w.work(burst=True), True,
                'Expected at least some work done.')
        self.assertEquals(result.return_value, 'Hi there, Frank!')

    def test_work_is_unreadable(self):
        """Unreadable jobs are put on the failed queue."""
        q = Queue()
        failed_q = get_failed_queue()

        self.assertEquals(failed_q.count, 0)
        self.assertEquals(q.count, 0)

        # NOTE: We have to fake this enqueueing for this test case.
        # What we're simulating here is a call to a function that is not
        # importable from the worker process.
        job = Job.create(func=div_by_zero, args=(3,))
        job.save()
        data = self.testconn.hget(job.key, 'data')
        invalid_data = data.replace('div_by_zero', 'nonexisting_job')
        assert data != invalid_data
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
        failed_q = get_failed_queue()

        # Preconditions
        self.assertEquals(failed_q.count, 0)
        self.assertEquals(q.count, 0)

        # Action
        job = q.enqueue(div_by_zero)
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

        # Should be the original enqueued_at date, not the date of enqueueing
        # to the failed queue
        self.assertEquals(job.enqueued_at, enqueued_at_date)
        self.assertIsNotNone(job.exc_info)  # should contain exc_info


    def test_cancelled_jobs_arent_executed(self):  # noqa
        """Cancelling jobs."""

        SENTINEL_FILE = '/tmp/rq-tests.txt'

        try:
            # Remove the sentinel if it is leftover from a previous test run
            os.remove(SENTINEL_FILE)
        except OSError as e:
            if e.errno != 2:
                raise

        q = Queue()
        result = q.enqueue(create_file, SENTINEL_FILE)

        # Here, we cancel the job, so the sentinel file may not be created
        assert q.count == 1
        result.cancel()
        assert q.count == 1

        w = Worker([q])
        w.work(burst=True)
        assert q.count == 0

        # Should not have created evidence of execution
        self.assertEquals(os.path.exists(SENTINEL_FILE), False)

    def test_cleaning_up_of_jobs(self):
        """Jobs get cleaned up after successful execution."""
        q = Queue()
        job_with_rv = q.enqueue(say_hello, 'Franklin')
        job_without_rv = q.enqueue(do_nothing)

        # Job hashes exists
        self.assertEquals(self.testconn.type(job_with_rv.key), 'hash')
        self.assertEquals(self.testconn.type(job_without_rv.key), 'hash')

        # Execute the job
        w = Worker([q])
        w.work(burst=True)

        # First, assert that the job executed successfully
        assert self.testconn.hget(job_with_rv.key, 'exc_info') is None
        assert self.testconn.hget(job_without_rv.key, 'exc_info') is None

        # Jobs with results expire after a certain TTL, while jobs without
        # results are immediately removed
        assert self.testconn.ttl(job_with_rv.key) > 0
        assert not self.testconn.exists(job_without_rv.key)


    @slow  # noqa
    def test_timeouts(self):
        """Worker kills jobs after timeout."""
        sentinel_file = '/tmp/.rq_sentinel'

        q = Queue()
        w = Worker([q])

        # Put it on the queue with a timeout value
        res = q.enqueue(
                create_file_after_timeout,
                args=(sentinel_file, 4),
                timeout=1)

        try:
            os.unlink(sentinel_file)
        except OSError as e:
            if e.errno == 2:
                pass

        self.assertEquals(os.path.exists(sentinel_file), False)
        w.work(burst=True)
        self.assertEquals(os.path.exists(sentinel_file), False)

        # TODO: Having to do the manual refresh() here is really ugly!
        res.refresh()
        self.assertIn('JobTimeoutException', res.exc_info)

    def test_worker_sets_result_ttl(self):
        """Ensure that Worker properly sets result_ttl for individual jobs."""
        q = Queue()
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=10)
        w = Worker([q])
        w.work(burst=True)
        self.assertEqual(self.testconn.ttl(job.key), 10)

        # Job with -1 result_ttl don't expire
        job = q.enqueue(say_hello, args=('Frank',), result_ttl=-1)
        w = Worker([q])
        w.work(burst=True)
        self.assertEqual(self.testconn.ttl(job.key), None)


