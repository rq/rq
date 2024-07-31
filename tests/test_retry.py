from datetime import datetime, timedelta, timezone

from rq.job import Job, JobStatus, Retry
from rq.queue import Queue
from rq.registry import FailedJobRegistry, StartedJobRegistry
from rq.worker import Worker
from tests import RQTestCase, fixtures
from tests.fixtures import div_by_zero, say_hello


class TestRetry(RQTestCase):
    def test_persistence_of_retry_data(self):
        """Retry related data is stored and restored properly"""
        job = Job.create(func=fixtures.some_calculation, connection=self.connection)
        job.retries_left = 3
        job.retry_intervals = [1, 2, 3]
        job.save()

        job.retries_left = None
        job.retry_intervals = None
        job.refresh()
        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.retry_intervals, [1, 2, 3])

    def test_retry_class(self):
        """Retry parses `max` and `interval` correctly"""
        retry = Retry(max=1)
        self.assertEqual(retry.max, 1)
        self.assertEqual(retry.intervals, [0])
        self.assertRaises(ValueError, Retry, max=0)

        retry = Retry(max=2, interval=5)
        self.assertEqual(retry.max, 2)
        self.assertEqual(retry.intervals, [5])

        retry = Retry(max=3, interval=[5, 10])
        self.assertEqual(retry.max, 3)
        self.assertEqual(retry.intervals, [5, 10])

        # interval can't be negative
        self.assertRaises(ValueError, Retry, max=1, interval=-5)
        self.assertRaises(ValueError, Retry, max=1, interval=[1, -5])

    def test_get_retry_interval(self):
        """get_retry_interval() returns the right retry interval"""
        job = Job.create(func=fixtures.say_hello, connection=self.connection)

        # Handle case where self.retry_intervals is None
        job.retries_left = 2
        self.assertEqual(job.get_retry_interval(), 0)

        # Handle the most common case
        job.retry_intervals = [1, 2]
        self.assertEqual(job.get_retry_interval(), 1)
        job.retries_left = 1
        self.assertEqual(job.get_retry_interval(), 2)

        # Handle cases where number of retries > length of interval
        job.retries_left = 4
        job.retry_intervals = [1, 2, 3]
        self.assertEqual(job.get_retry_interval(), 1)
        job.retries_left = 3
        self.assertEqual(job.get_retry_interval(), 1)
        job.retries_left = 2
        self.assertEqual(job.get_retry_interval(), 2)
        job.retries_left = 1
        self.assertEqual(job.get_retry_interval(), 3)

    def test_job_retry(self):
        """Test job.retry() works properly"""
        queue = Queue(connection=self.connection)
        retry = Retry(max=3, interval=5)
        job = queue.enqueue(div_by_zero, retry=retry)

        with self.connection.pipeline() as pipeline:
            job.retry(queue, pipeline)
            pipeline.execute()

        self.assertEqual(job.retries_left, 2)
        # status should be scheduled since it's retried with 5 seconds interval
        self.assertEqual(job.get_status(), JobStatus.SCHEDULED)

        retry = Retry(max=3)
        job = queue.enqueue(div_by_zero, retry=retry)

        with self.connection.pipeline() as pipeline:
            job.retry(queue, pipeline)

            pipeline.execute()

        self.assertEqual(job.retries_left, 2)
        # status should be queued
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_retry_interval(self):
        """Retries with intervals are scheduled"""
        connection = self.connection
        queue = Queue(connection=connection)
        retry = Retry(max=1, interval=5)
        job = queue.enqueue(div_by_zero, retry=retry)

        worker = Worker([queue])
        registry = queue.scheduled_job_registry
        # If job if configured to retry with interval, it will be scheduled,
        # not directly put back in the queue
        queue.empty()
        worker.handle_job_failure(job, queue)
        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.SCHEDULED)
        self.assertEqual(job.retries_left, 0)
        self.assertEqual(len(registry), 1)
        self.assertEqual(queue.job_ids, [])
        # Scheduled time is roughly 5 seconds from now
        scheduled_time = registry.get_scheduled_time(job)
        now = datetime.now(timezone.utc)
        self.assertTrue(now + timedelta(seconds=4) < scheduled_time < now + timedelta(seconds=10))

    def test_cleanup_handles_retries(self):
        """Expired jobs should also be retried"""
        queue = Queue(connection=self.connection)
        registry = StartedJobRegistry(connection=self.connection)
        failed_job_registry = FailedJobRegistry(connection=self.connection)
        job = queue.enqueue(say_hello, retry=Retry(max=1))

        # Add job to StartedJobRegistry with past expiration time
        self.connection.zadd(registry.key, {job.id: 2})

        registry.cleanup()
        self.assertEqual(len(queue), 2)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        self.assertNotIn(job, failed_job_registry)

        self.connection.zadd(registry.key, {job.id: 2})
        # Job goes to FailedJobRegistry because it's only retried once
        registry.cleanup()
        self.assertEqual(len(queue), 2)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn(job, failed_job_registry)
