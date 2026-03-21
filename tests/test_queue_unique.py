"""Tests for Queue unique job enqueue behavior."""

from datetime import datetime, timedelta, timezone

from rq import Queue
from rq.exceptions import DuplicateJobError
from rq.job import JobStatus
from tests import RQTestCase
from tests.fixtures import say_hello


class TestEnqueueJobUnique(RQTestCase):
    """Tests for Queue unique enqueue integration."""

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

    def test_unique_requires_job_id(self):
        """unique=True without an explicit job_id raises ValueError."""
        queue = Queue(connection=self.connection)

        # enqueue
        with self.assertRaises(ValueError):
            queue.enqueue(say_hello, unique=True)

        # enqueue_job
        job = queue.create_job(say_hello)
        with self.assertRaises(ValueError):
            queue.enqueue_job(job, unique=True)

        # schedule_job
        job = queue.create_job(say_hello)
        scheduled_time = datetime.now(timezone.utc) + timedelta(hours=1)
        with self.assertRaises(ValueError):
            queue.schedule_job(job, scheduled_time, unique=True)
