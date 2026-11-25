"""Tests for Queue unique job enqueue behavior."""

import zlib
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from rq import Queue
from rq.exceptions import DuplicateJobError
from rq.job import ZSTD_MAGIC, Job, JobStatus, zstd
from rq.rate_limit import RateLimit
from rq.scheduler import RQScheduler
from tests import RQTestCase
from tests.fixtures import say_hello


class TestEnqueueJobUnique(RQTestCase):
    """Tests for Queue unique enqueue integration."""

    def _assert_zstd_job(self, job):
        compressed_data = self.connection.hget(job.key, 'data')
        self.assertTrue(compressed_data.startswith(ZSTD_MAGIC))
        self.assertEqual(zstd.decompress(compressed_data), job.data)

    def test_unique_async_enqueue_uses_queue_zstd_setting(self):
        """Unique async enqueue passes the Queue compression setting to its Lua serializer."""
        if zstd is None:
            self.skipTest('Zstandard is not available')

        queue = Queue(connection=self.connection, use_zstd=True)
        job = queue.enqueue(say_hello, job_id='zstd-unique-job', unique=True)

        self._assert_zstd_job(job)

    def test_unique_sync_enqueue_uses_queue_zstd_setting(self):
        """Unique synchronous enqueue passes the Queue compression setting to its Lua serializer."""
        if zstd is None:
            self.skipTest('Zstandard is not available')

        queue = Queue(connection=self.connection, is_async=False, use_zstd=True)
        # Stop before synchronous execution re-saves the job with Job.save's Zlib default.
        with patch.object(queue, 'run_sync', side_effect=lambda job: job):
            job = queue.enqueue(say_hello, job_id='zstd-sync-unique-job', unique=True)

        self._assert_zstd_job(job)

    def test_scheduled_enqueue_uses_queue_zstd_setting(self):
        """Regular and unique scheduled enqueue persist with the Queue compression setting."""
        if zstd is None:
            self.skipTest('Zstandard is not available')

        queue = Queue(connection=self.connection, use_zstd=True)
        scheduled_time = datetime.now(timezone.utc) + timedelta(hours=1)

        regular_job = queue.enqueue_at(scheduled_time, say_hello, job_id='zstd-scheduled-job')
        unique_job = queue.enqueue_at(scheduled_time, say_hello, job_id='zstd-unique-scheduled-job', unique=True)

        self._assert_zstd_job(regular_job)
        self._assert_zstd_job(unique_job)

    def test_unique_enqueue_supports_legacy_to_dict_override(self):
        """Unique serialization preserves the old to_dict override signature by default."""

        class LegacyToDictJob(Job):
            def to_dict(self, include_meta=True, include_result=True):
                return super().to_dict(include_meta, include_result)

        queue = Queue(connection=self.connection, job_class=LegacyToDictJob)
        job = queue.enqueue(say_hello, job_id='legacy-to-dict-unique-job', unique=True)

        compressed_data = self.connection.hget(job.key, 'data')
        self.assertEqual(zlib.decompress(compressed_data), job.data)
        self.assertEqual(queue.get_job_ids(), [job.id])

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

    def test_enqueue_at_unique_raises_on_duplicate(self):
        """enqueue_at and enqueue_in with unique=True raise DuplicateJobError for duplicate job_id."""
        queue = Queue(connection=self.connection)
        scheduled_time = datetime.now(timezone.utc) + timedelta(hours=1)

        job = queue.enqueue_at(scheduled_time, say_hello, job_id='enqueue-at-unique-job', unique=True)
        self.assertEqual(job.get_status(), JobStatus.SCHEDULED)
        self.assertIn(job.id, queue.scheduled_job_registry)

        with self.assertRaises(DuplicateJobError):
            queue.enqueue_at(scheduled_time, say_hello, job_id='enqueue-at-unique-job', unique=True)

        with self.assertRaises(DuplicateJobError):
            queue.enqueue_in(timedelta(hours=1), say_hello, job_id='enqueue-at-unique-job', unique=True)

    def test_scheduler_enqueues_unique_scheduled_job(self):
        """RQScheduler moves a job scheduled with unique=True to its queue."""
        queue = Queue(connection=self.connection)
        scheduled_time = datetime.now(timezone.utc) - timedelta(seconds=1)
        job = queue.enqueue_at(scheduled_time, say_hello, job_id='due-unique-job', unique=True)

        scheduler = RQScheduler([queue], connection=self.connection)
        scheduler.acquire_locks()
        scheduler.enqueue_scheduled_jobs()

        self.assertEqual(queue.job_ids, [job.id])
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        self.assertNotIn(job.id, queue.scheduled_job_registry)

        with self.assertRaises(DuplicateJobError):
            queue.enqueue(say_hello, job_id='due-unique-job', unique=True)

    def test_enqueue_at_unique_requires_job_id(self):
        """enqueue_at with unique=True without an explicit job_id raises ValueError."""
        queue = Queue(connection=self.connection)
        scheduled_time = datetime.now(timezone.utc) + timedelta(hours=1)

        with self.assertRaises(ValueError):
            queue.enqueue_at(scheduled_time, say_hello, unique=True)

    def test_unique_with_rate_limit_raises(self):
        """unique=True with a rate-limited job raises ValueError."""
        queue = Queue(connection=self.connection)
        rate_limit = RateLimit(key='rl', concurrency=1)

        with self.assertRaises(ValueError) as context:
            queue.enqueue(say_hello, job_id='unique-rl-job', unique=True, rate_limit=rate_limit)

        self.assertIn('rate-limited jobs', str(context.exception))

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
