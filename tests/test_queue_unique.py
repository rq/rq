"""Tests for the _persist_unique_job method in Queue."""

from datetime import datetime, timedelta, timezone

from rq import Queue
from rq.exceptions import DuplicateJobError
from rq.job import Job, JobStatus
from tests import RQTestCase
from tests.fixtures import say_hello


class TestEnqueueJobUnique(RQTestCase):
    """Tests for Queue._persist_unique_job method."""

    def _create_job_for_unique_enqueue(self, queue, job_id, ttl=None):
        """Helper to create and prepare a job for _persist_unique_job.

        This mimics what _enqueue_async_job does before calling _persist_unique_job.
        """
        job = queue.create_job(say_hello, job_id=job_id, ttl=ttl)
        queue._prepare_for_queue(job)
        return job

    def test_enqueue_job_unique_push_direction(self):
        """_persist_unique_job pushes job to correct position based on at_front and push_to_queue."""
        queue = Queue(connection=self.connection)

        # First enqueue a dummy job using the standard enqueue method
        queue.enqueue(say_hello, job_id='dummy-job')
        self.assertEqual(queue.get_job_ids(), ['dummy-job'])

        # Enqueue a unique job to the right (back) of queue
        job1 = self._create_job_for_unique_enqueue(queue, 'job-1')
        queue._persist_unique_job(job1, at_front=False)

        # Verify order: dummy-job should still be first, job-1 should be at the back
        job_ids = queue.get_job_ids()
        self.assertEqual(job_ids, ['dummy-job', 'job-1'])

        # Enqueue another unique job to the left (front) of queue
        job2 = self._create_job_for_unique_enqueue(queue, 'job-2')
        queue._persist_unique_job(job2, at_front=True)

        # Verify order: job-2 should be first (front), then dummy-job, then job-1
        job_ids = queue.get_job_ids()
        self.assertEqual(job_ids, ['job-2', 'dummy-job', 'job-1'])

        # Enqueue a unique job with enqueue=False (should not be added to queue)
        job3 = self._create_job_for_unique_enqueue(queue, 'job-3')
        queue._persist_unique_job(job3, enqueue=False)

        # Verify job data is saved in Redis
        self.assertTrue(self.connection.exists(job3.key))
        fetched_job = Job.fetch('job-3', connection=self.connection)
        self.assertEqual(fetched_job.id, 'job-3')

        # Verify job is NOT in the queue (queue unchanged)
        job_ids = queue.get_job_ids()
        self.assertEqual(job_ids, ['job-2', 'dummy-job', 'job-1'])

    def test_enqueue_job_unique_ttl(self):
        """_persist_unique_job sets TTL correctly based on job.ttl value."""
        queue = Queue(connection=self.connection)

        # Create job with TTL of 60 seconds
        job_with_ttl = self._create_job_for_unique_enqueue(queue, 'ttl-job', ttl=60)
        queue._persist_unique_job(job_with_ttl, at_front=False)

        # Verify job exists and TTL is set (should be close to 60 seconds)
        self.assertTrue(self.connection.exists(job_with_ttl.key))
        ttl = self.connection.ttl(job_with_ttl.key)
        self.assertGreater(ttl, 50)
        self.assertLessEqual(ttl, 60)

        # Create job without TTL (default)
        job_without_ttl = self._create_job_for_unique_enqueue(queue, 'no-ttl-job', ttl=None)
        queue._persist_unique_job(job_without_ttl, at_front=False)

        # Verify job exists and no TTL is set (-1 means no expiry in Redis)
        self.assertTrue(self.connection.exists(job_without_ttl.key))
        ttl = self.connection.ttl(job_without_ttl.key)
        self.assertEqual(ttl, -1)

    def test_enqueue_job_unique_raises_on_duplicate(self):
        """_persist_unique_job raises DuplicateJobError when job already exists."""
        queue = Queue(connection=self.connection)

        # Create and enqueue first job
        job1 = self._create_job_for_unique_enqueue(queue, 'duplicate-job')
        queue._persist_unique_job(job1, at_front=False)

        # Try to enqueue second job with same ID
        job2 = self._create_job_for_unique_enqueue(queue, 'duplicate-job')
        with self.assertRaises(DuplicateJobError) as context:
            queue._persist_unique_job(job2, at_front=False)

        self.assertIn('duplicate-job', str(context.exception))

        # Verify only one job is in the queue
        self.assertEqual(queue.count, 1)

        # Also test with enqueue=False (used for sync jobs)
        job3 = self._create_job_for_unique_enqueue(queue, 'sync-duplicate-job')
        queue._persist_unique_job(job3, enqueue=False)

        # Try to enqueue second job with same ID (also without pushing)
        job4 = self._create_job_for_unique_enqueue(queue, 'sync-duplicate-job')
        with self.assertRaises(DuplicateJobError) as context:
            queue._persist_unique_job(job4, enqueue=False)

        self.assertIn('sync-duplicate-job', str(context.exception))

    def test_enqueue_job_unique_stores_job_data_correctly(self):
        """_persist_unique_job stores all job data correctly in Redis."""
        queue = Queue(connection=self.connection)

        # Create job with various attributes
        job = queue.create_job(
            say_hello,
            job_id='data-job',
            ttl=120,
            meta={'custom': 'value'},
            description='Test job',
            timeout=300,
            result_ttl=600,
        )
        queue._prepare_for_queue(job)

        queue._persist_unique_job(job, at_front=False)

        # Fetch job fresh from Redis
        fetched_job = Job.fetch('data-job', connection=self.connection)

        # Verify job attributes
        self.assertEqual(fetched_job.id, 'data-job')
        self.assertEqual(fetched_job.origin, queue.name)
        self.assertEqual(fetched_job.meta, {'custom': 'value'})
        self.assertEqual(fetched_job.description, 'Test job')
        self.assertEqual(fetched_job.timeout, 300)
        self.assertEqual(fetched_job.result_ttl, 600)

    def test_unique_with_dependencies_raises_exception(self):
        """unique=True with job dependencies raises ValueError."""
        queue = Queue(connection=self.connection)

        # First create a dependency job
        dependency_job = queue.enqueue(say_hello, job_id='dependency-job')

        # Try to enqueue a unique job with dependencies
        with self.assertRaises(ValueError) as context:
            queue.enqueue(say_hello, job_id='dependent-job', depends_on=dependency_job, unique=True)

        self.assertIn('unique=True is not supported with job dependencies', str(context.exception))

    def test_schedule_job_unique_raises_on_duplicate(self):
        """schedule_job with unique=True raises DuplicateJobError for duplicate job_id."""
        queue = Queue(connection=self.connection)

        # Create and schedule first job
        job1 = queue.create_job(say_hello, job_id='scheduled-unique-job')
        scheduled_time = datetime.now(timezone.utc) + timedelta(hours=1)
        queue.schedule_job(job1, scheduled_time, unique=True)

        # Verify job is scheduled
        self.assertEqual(job1.get_status(), JobStatus.SCHEDULED)

        # Try to schedule second job with same ID
        job2 = queue.create_job(say_hello, job_id='scheduled-unique-job')
        with self.assertRaises(DuplicateJobError) as context:
            queue.schedule_job(job2, scheduled_time, unique=True)

        self.assertIn('scheduled-unique-job', str(context.exception))
