from datetime import datetime

from rq.executions import Execution
from rq.job import Job, JobStatus, Retry
from rq.queue import Queue
from rq.rate_limit import RateLimit, RateLimitRegistry
from rq.registry import ScheduledJobRegistry, StartedJobRegistry
from rq.scheduler import RQScheduler
from rq.worker import SimpleWorker
from tests import RQTestCase
from tests.fixtures import div_by_zero, returns_retry, returns_retry_with_delay, say_hello


class TestRateLimit(RQTestCase):
    """Test the RateLimit dataclass."""

    def test_init(self):
        """RateLimit can be created with valid key and concurrency."""
        rl = RateLimit(key='test', concurrency=2)
        self.assertEqual(rl.key, 'test')
        self.assertEqual(rl.concurrency, 2)

    def test_empty_key_raises(self):
        """RateLimit raises ValueError for empty key."""
        with self.assertRaises(ValueError):
            RateLimit(key='', concurrency=1)

    def test_invalid_concurrency_raises(self):
        """RateLimit raises ValueError for concurrency < 1."""
        with self.assertRaises(ValueError):
            RateLimit(key='test', concurrency=0)
        with self.assertRaises(ValueError):
            RateLimit(key='test', concurrency=-1)


class TestRateLimitJob(RQTestCase):
    """Test rate limit fields on Job."""

    def test_rate_limit_job_persistence(self):
        """Rate limit fields survive save/fetch round-trip."""
        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job.rate_limit_key = 'my_key'
        job.rate_limit_concurrency = 3
        job.save()

        fetched = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(fetched.rate_limit_key, 'my_key')
        self.assertEqual(fetched.rate_limit_concurrency, 3)
        self.assertTrue(fetched.has_rate_limit)

    def test_has_rate_limit(self):
        """has_rate_limit returns True only when both fields are set."""
        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        self.assertFalse(job.has_rate_limit)

        job.rate_limit_key = 'key'
        self.assertFalse(job.has_rate_limit)

        job.rate_limit_concurrency = 2
        self.assertTrue(job.has_rate_limit)


class TestRateLimitRegistry(RQTestCase):
    """Test the RateLimitRegistry class."""

    def setUp(self):
        super().setUp()
        self.rate_limit_registry = RateLimitRegistry(key='test', connection=self.connection)
        with self.connection.pipeline() as pipe:
            self.rate_limit_registry.register(2, pipe)
            pipe.execute()
        self.queue = Queue('default', connection=self.connection)

    def _add_to_pending(self, job_id, timestamp=None):
        """Helper to add a job to the pending set using a pipeline."""
        with self.connection.pipeline() as pipe:
            self.rate_limit_registry.add_to_pending(job_id, pipe, timestamp=timestamp)
            pipe.execute()

    def _make_job(self, status: JobStatus, origin: str = 'default') -> Job:
        """Helper to create and save a job with the given status."""
        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection, origin=origin, status=status)
        job.save()
        return job

    def test_add_to_pending(self):
        """Jobs added to pending are ordered by timestamp."""
        self._add_to_pending('job1', timestamp=1)
        self._add_to_pending('job2', timestamp=2)
        self._add_to_pending('job3', timestamp=3)
        self.assertEqual(self.rate_limit_registry.get_pending_job_ids(), ['job1', 'job2', 'job3'])

    def test_acquire_and_enqueue(self):
        """acquire_and_enqueue enqueues a pending job when capacity is available,
        returns None when at capacity or no pending jobs."""
        # No pending jobs, nothing happens
        result = self.rate_limit_registry.acquire_and_enqueue(max_concurrency=2)
        self.assertIsNone(result)

        job = self._make_job(status=JobStatus.RATE_LIMITED)

        self._add_to_pending(job.id)
        result = self.rate_limit_registry.acquire_and_enqueue(max_concurrency=2)

        self.assertEqual(result, job.id)
        self.assertEqual(self.rate_limit_registry.get_active_job_count(), 1)
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 0)
        self.assertIn(job.id, self.rate_limit_registry.get_active_job_ids())
        self.assertIn(job.id, self.queue.job_ids)
        self.assertEqual(job.get_status(), 'queued')
        self.assertIsNotNone(self.connection.hget(job.key, 'enqueued_at'))

        # Now fill active set to capacity and verify nothing is enqueued
        self.connection.zadd(self.rate_limit_registry.active_key, {'active2': 2})
        self._add_to_pending('job2')
        result = self.rate_limit_registry.acquire_and_enqueue(max_concurrency=2)

        self.assertIsNone(result)
        self.assertEqual(self.rate_limit_registry.get_active_job_count(), 2)
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 1)

    def test_acquire_and_enqueue_enqueues_oldest_first(self):
        """acquire_and_enqueue enqueues the oldest pending job (lowest score)."""
        job1 = self._make_job(status=JobStatus.RATE_LIMITED)
        job2 = self._make_job(status=JobStatus.RATE_LIMITED)

        self._add_to_pending(job1.id, timestamp=1)
        self._add_to_pending(job2.id, timestamp=2)

        result = self.rate_limit_registry.acquire_and_enqueue(max_concurrency=1)
        self.assertEqual(result, job1.id)
        self.assertEqual(self.rate_limit_registry.get_pending_job_ids(), [job2.id])

    def test_release_capacity_and_enqueue(self):
        """release_capacity_and_enqueue removes from active and enqueues next pending job."""
        # Releasing a nonexistent job is a no-op
        result = self.rate_limit_registry.release_capacity_and_enqueue('nonexistent')
        self.assertIsNone(result)

        # Releasing with no pending jobs returns None
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})
        result = self.rate_limit_registry.release_capacity_and_enqueue('active1')
        self.assertIsNone(result)
        self.assertEqual(self.rate_limit_registry.get_active_job_count(), 0)

        # Releasing with a pending job enqueues it
        self.connection.zadd(self.rate_limit_registry.active_key, {'active2': 1})
        job = self._make_job(status=JobStatus.RATE_LIMITED)
        self._add_to_pending(job.id)

        result = self.rate_limit_registry.release_capacity_and_enqueue('active2')
        self.assertEqual(result, job.id)
        self.assertNotIn('active2', self.rate_limit_registry.get_active_job_ids())
        self.assertIn(job.id, self.rate_limit_registry.get_active_job_ids())
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 0)
        self.assertIn(job.id, self.queue.job_ids)
        self.assertEqual(job.get_status(), 'queued')
        self.assertIsNotNone(self.connection.hget(job.key, 'enqueued_at'))

    def test_cancel_active_job(self):
        """cancel removes an active job and enqueues the next pending job."""
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})

        job2 = self._make_job(status=JobStatus.RATE_LIMITED)
        self._add_to_pending(job2.id)

        result = self.rate_limit_registry.cancel('active1')

        self.assertEqual(result, job2.id)
        self.assertNotIn('active1', self.rate_limit_registry.get_active_job_ids())
        self.assertIn(job2.id, self.rate_limit_registry.get_active_job_ids())

    def test_cancel_pending_job(self):
        """cancel removes a pending job without affecting active set."""
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})
        self._add_to_pending('pending1')

        result = self.rate_limit_registry.cancel('pending1')

        self.assertIsNone(result)
        self.assertIn('active1', self.rate_limit_registry.get_active_job_ids())
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 0)

    def test_cancel_nonexistent_job(self):
        """cancel is a no-op for jobs not in any set."""
        result = self.rate_limit_registry.cancel('nonexistent')
        self.assertIsNone(result)

    def test_cleanup_releases_missing_active_job(self):
        """cleanup() frees an active slot whose job no longer exists."""
        self.connection.zadd(self.rate_limit_registry.active_key, {'gone': 1})

        self.rate_limit_registry.cleanup()

        self.assertNotIn('gone', self.rate_limit_registry.get_active_job_ids())

    def test_cleanup_releases_terminal_active_jobs(self):
        """cleanup() frees active slots held by jobs in a terminal state."""
        finished_job = self._make_job(status=JobStatus.FINISHED)
        failed_job = self._make_job(status=JobStatus.FAILED)
        canceled_job = self._make_job(status=JobStatus.CANCELED)
        stopped_job = self._make_job(status=JobStatus.STOPPED)
        self.connection.zadd(
            self.rate_limit_registry.active_key,
            {finished_job.id: 1, failed_job.id: 2, canceled_job.id: 3, stopped_job.id: 4},
        )

        self.rate_limit_registry.cleanup()

        active_ids = self.rate_limit_registry.get_active_job_ids()
        self.assertNotIn(finished_job.id, active_ids)
        self.assertNotIn(failed_job.id, active_ids)
        self.assertNotIn(canceled_job.id, active_ids)
        self.assertNotIn(stopped_job.id, active_ids)

    def test_cleanup_releases_scheduled_active_job(self):
        """cleanup() frees an active slot held by a scheduled job (a delayed
        retry whose slot release failed); a terminal-only check would miss it."""
        scheduled_job = self._make_job(status=JobStatus.SCHEDULED)
        self.connection.zadd(self.rate_limit_registry.active_key, {scheduled_job.id: 1})

        self.rate_limit_registry.cleanup()

        self.assertNotIn(scheduled_job.id, self.rate_limit_registry.get_active_job_ids())

    def test_cleanup_releases_malformed_status_active_jobs(self):
        """cleanup() frees active slots with a malformed or missing status
        without crashing (no full Job hydration)."""
        bogus_status_job = self._make_job(status=JobStatus.QUEUED)
        self.connection.hset(bogus_status_job.key, 'status', 'bogus')
        missing_status_job = self._make_job(status=JobStatus.QUEUED)
        self.connection.hdel(missing_status_job.key, 'status')
        self.connection.zadd(self.rate_limit_registry.active_key, {bogus_status_job.id: 1, missing_status_job.id: 2})

        self.rate_limit_registry.cleanup()

        active_ids = self.rate_limit_registry.get_active_job_ids()
        self.assertNotIn(bogus_status_job.id, active_ids)
        self.assertNotIn(missing_status_job.id, active_ids)

    def test_cleanup_keeps_legitimately_active_jobs(self):
        """cleanup() does not evict jobs that are queued or started."""
        queued_job = self._make_job(status=JobStatus.QUEUED)
        started_job = self._make_job(status=JobStatus.STARTED)
        self.connection.zadd(self.rate_limit_registry.active_key, {queued_job.id: 1, started_job.id: 2})

        self.rate_limit_registry.cleanup()

        active_ids = self.rate_limit_registry.get_active_job_ids()
        self.assertIn(queued_job.id, active_ids)
        self.assertIn(started_job.id, active_ids)

    def test_cleanup_promotes_pending_after_releasing_stale_active(self):
        """Freeing a stale active slot makes room to promote a pending job."""
        registry = RateLimitRegistry(key='solo', connection=self.connection)
        with self.connection.pipeline() as pipe:
            registry.register(1, pipe)
            pipe.execute()

        self.connection.zadd(registry.active_key, {'gone': 1})
        pending_job = self._make_job(status=JobStatus.RATE_LIMITED)
        self._add_to_pending_for(registry, pending_job.id)

        registry.cleanup()

        self.assertNotIn('gone', registry.get_active_job_ids())
        self.assertIn(pending_job.id, registry.get_active_job_ids())
        self.assertEqual(registry.get_pending_job_count(), 0)
        self.assertIn(pending_job.id, self.queue.job_ids)
        self.assertEqual(pending_job.get_status(), JobStatus.QUEUED)

    def test_cleanup_stale_pending_does_not_crash_promotion(self):
        """A stale pending id (no job hash) is pruned instead of crashing the
        promotion, and a valid pending job behind it is still promoted."""
        registry = RateLimitRegistry(key='solo', connection=self.connection)
        with self.connection.pipeline() as pipe:
            registry.register(1, pipe)
            pipe.execute()

        self.connection.zadd(registry.active_key, {'gone': 1})
        pending_job = self._make_job(status=JobStatus.RATE_LIMITED)
        self._add_to_pending_for(registry, 'stale-pending', timestamp=1)
        self._add_to_pending_for(registry, pending_job.id, timestamp=2)

        registry.cleanup()

        self.assertNotIn('stale-pending', registry.get_pending_job_ids())
        self.assertIn(pending_job.id, registry.get_active_job_ids())
        self.assertIn(pending_job.id, self.queue.job_ids)

    def test_cleanup_skips_non_rate_limited_pending(self):
        """A pending entry whose job is not rate_limited (e.g. canceled) is
        pruned without being promoted; the next valid pending job is promoted."""
        registry = RateLimitRegistry(key='solo', connection=self.connection)
        with self.connection.pipeline() as pipe:
            registry.register(1, pipe)
            pipe.execute()

        canceled_job = self._make_job(status=JobStatus.CANCELED)
        pending_job = self._make_job(status=JobStatus.RATE_LIMITED)
        self._add_to_pending_for(registry, canceled_job.id, timestamp=1)
        self._add_to_pending_for(registry, pending_job.id, timestamp=2)

        registry.cleanup()

        self.assertNotIn(canceled_job.id, registry.get_pending_job_ids())
        self.assertNotIn(canceled_job.id, registry.get_active_job_ids())
        self.assertNotIn(canceled_job.id, self.queue.job_ids)
        self.assertIn(pending_job.id, registry.get_active_job_ids())

    def test_cleanup_removes_registry_with_only_stale_active(self):
        """cleanup() deletes the registry when active holds only stale ids and
        pending is empty."""
        self.connection.zadd(self.rate_limit_registry.active_key, {'gone1': 1, 'gone2': 2})

        self.rate_limit_registry.cleanup()

        self.assertEqual(RateLimitRegistry.all(self.connection), [])
        self.assertFalse(self.connection.sismember(RateLimitRegistry.rl_keys_key, 'test'))
        self.assertFalse(self.connection.exists(self.rate_limit_registry.config_key))

    def _add_to_pending_for(self, registry, job_id, timestamp=None):
        """Helper to add a job to a given registry's pending set."""
        with self.connection.pipeline() as pipe:
            registry.add_to_pending(job_id, pipe, timestamp=timestamp)
            pipe.execute()

    def test_different_keys_are_independent(self):
        """Registries with different keys don't interfere with each other."""
        registry_a = RateLimitRegistry(key='key_a', connection=self.connection)
        registry_b = RateLimitRegistry(key='key_b', connection=self.connection)

        job_a = self._make_job(status=JobStatus.RATE_LIMITED)
        job_b = self._make_job(status=JobStatus.RATE_LIMITED)

        with self.connection.pipeline() as pipe:
            registry_a.add_to_pending(job_a.id, pipe)
            registry_b.add_to_pending(job_b.id, pipe)
            pipe.execute()

        # Fill key_a to capacity
        self.connection.zadd(registry_a.active_key, {'x': 1})
        result_a = registry_a.acquire_and_enqueue(max_concurrency=1)
        # key_a is full, should not enqueue
        self.assertIsNone(result_a)

        # key_b still has capacity
        result_b = registry_b.acquire_and_enqueue(max_concurrency=1)
        self.assertEqual(result_b, job_b.id)


class TestRateLimitEnqueue(RQTestCase):
    """Test rate limiting through Queue.enqueue()."""

    def setUp(self):
        super().setUp()
        self.queue = Queue('default', connection=self.connection)

    def test_rate_limit_rejected_on_sync_queue(self):
        """RateLimit is not supported on synchronous (is_async=False) queues."""
        sync_queue = Queue('default', connection=self.connection, is_async=False)
        with self.assertRaises(ValueError):
            sync_queue.enqueue(say_hello, rate_limit=RateLimit(key='test', concurrency=1))

    def test_enqueue_with_rate_limit(self):
        """Jobs exceeding concurrency limit are deferred, others are queued."""
        rate_limit = RateLimit(key='test', concurrency=2)

        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        job3 = self.queue.enqueue(say_hello, rate_limit=rate_limit)

        # A job that immediately acquires capacity returns an object already
        # reflecting QUEUED status and enqueued_at — no refresh() needed.
        self.assertEqual(job1.get_status(refresh=False), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(refresh=False), JobStatus.QUEUED)
        self.assertEqual(job3.get_status(refresh=False), JobStatus.RATE_LIMITED)
        self.assertIsNotNone(job1.enqueued_at)

        # Verify rate limit fields are persisted
        self.assertTrue(job1.has_rate_limit)
        self.assertEqual(job1.rate_limit_key, 'test')
        self.assertEqual(job1.rate_limit_concurrency, 2)

        # Verify the registry state
        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertEqual(registry.get_active_job_count(), 2)
        self.assertEqual(registry.get_pending_job_count(), 1)

        # Only the first 2 jobs should be in the queue
        self.assertEqual(len(self.queue.job_ids), 2)

    def test_enqueue_with_rate_limit_and_dependencies(self):
        """Rate limit check happens after dependencies are met."""
        rate_limit = RateLimit(key='test', concurrency=1)

        # First job takes the only slot
        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)

        # Second job depends on job1 and also has rate limit
        job2 = self.queue.enqueue(say_hello, depends_on=job1, rate_limit=rate_limit)
        # Should be deferred due to dependency (not rate limit)
        self.assertEqual(job2.get_status(), JobStatus.DEFERRED)

        # Simulate job1 completion: enqueue dependents
        job1._status = JobStatus.FINISHED
        job1.save()
        self.queue.enqueue_dependents(job1)

        # job2's dependency is met, but rate limit slot is still held by job1
        # in the active set. So job2 should go to RL pending.
        registry = RateLimitRegistry(key='test', connection=self.connection)
        # job1 is still in active (not released yet), job2 should be pending
        self.assertEqual(registry.get_active_job_count(), 1)
        self.assertEqual(registry.get_pending_job_count(), 1)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

    def test_enqueue_registers_rate_limiter(self):
        """Enqueueing a rate-limited job registers the key in rq:rate-limiters."""
        rate_limit = RateLimit(key='my_key', concurrency=3)
        self.queue.enqueue(say_hello, rate_limit=rate_limit)

        registries = RateLimitRegistry.all(self.connection)
        self.assertEqual(len(registries), 1)
        self.assertEqual(registries[0].key, 'my_key')
        self.assertEqual(registries[0].max_concurrency, 3)

    def test_cleanup_removes_empty_registry(self):
        """cleanup() removes registry from rq:rate-limiters when both sets are empty."""
        rate_limit = RateLimit(key='test', concurrency=1)
        job = self.queue.enqueue(say_hello, rate_limit=rate_limit)

        # Registry should be registered
        self.assertEqual(len(RateLimitRegistry.all(self.connection)), 1)

        registry = RateLimitRegistry(key='test', connection=self.connection)

        # Active set has the job, cleanup should not remove
        registry.cleanup()
        self.assertEqual(len(RateLimitRegistry.all(self.connection)), 1)

        # Remove the job from active to simulate completion
        self.connection.zrem(registry.active_key, job.id)
        registry.cleanup()

        # Registry should be removed since both sets are empty
        self.assertEqual(len(RateLimitRegistry.all(self.connection)), 0)

    def test_cleanup_enqueues_stuck_pending_jobs(self):
        """cleanup() enqueues pending jobs when there is capacity."""
        rate_limit = RateLimit(key='test', concurrency=1)

        # Enqueue 2 jobs: first gets queued, second is rate limited
        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        registry = RateLimitRegistry(key='test', connection=self.connection)

        # Simulate job1 completing but release not happening (e.g., worker crash)
        self.connection.zrem(registry.active_key, job1.id)

        # job2 is stuck in pending with capacity available
        self.assertEqual(registry.get_active_job_count(), 0)
        self.assertEqual(registry.get_pending_job_count(), 1)

        # cleanup should enqueue job2
        registry.cleanup()
        self.assertEqual(registry.get_active_job_count(), 1)
        self.assertEqual(registry.get_pending_job_count(), 0)
        self.assertEqual(job2.get_status(), JobStatus.QUEUED)

    def test_release_on_success(self):
        """Completing a rate-limited job releases capacity and enqueues the next pending job."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        worker = SimpleWorker([self.queue], connection=self.connection)
        worker.work(max_jobs=1)

        # job1 completed, job2 should now be enqueued
        self.assertEqual(job1.get_status(), JobStatus.FINISHED)
        self.assertEqual(job2.get_status(), JobStatus.QUEUED)
        rate_limit_registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertEqual(rate_limit_registry.get_active_job_count(), 1)
        self.assertIn(job2.id, rate_limit_registry.get_active_job_ids())

    def test_release_on_failure(self):
        """Failing a rate-limited job releases capacity and enqueues the next pending job."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(div_by_zero, rate_limit=rate_limit)
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        worker = SimpleWorker([self.queue], connection=self.connection)
        worker.work(max_jobs=1)

        # job1 failed, job2 should now be enqueued
        self.assertEqual(job1.get_status(), JobStatus.FAILED)
        self.assertEqual(job2.get_status(), JobStatus.QUEUED)
        rate_limit_registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertEqual(rate_limit_registry.get_active_job_count(), 1)
        self.assertIn(job2.id, rate_limit_registry.get_active_job_ids())

    def test_cancel_removes_from_registry_and_promotes_pending(self):
        """Canceling a pending rate-limited job removes it (without promotion);
        canceling the active job frees its slot and promotes the next pending job.
        Promoting job3 — not the older job2 — proves the canceled job2 is gone
        from pending and cannot be resurrected."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # active
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # pending
        job3 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # pending
        registry = RateLimitRegistry(key='test', connection=self.connection)

        # Canceling a pending job removes it without promoting (it wasn't active).
        job2.cancel()
        self.assertNotIn(job2.id, registry.get_pending_job_ids())
        self.assertIn(job1.id, registry.get_active_job_ids())

        # Canceling the active job frees its slot and promotes the next pending job.
        job1.cancel()
        self.assertNotIn(job1.id, registry.get_active_job_ids())
        self.assertIn(job3.id, registry.get_active_job_ids())
        self.assertEqual(job3.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.CANCELED)

    def test_delete_removes_from_registry_and_promotes_pending(self):
        """Deleting a pending rate-limited job removes it (without promotion);
        deleting the active job frees its slot and promotes the next pending job.
        Promoting job3 — not the older job2 — proves the deleted job2 is gone."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # active
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # pending
        job3 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # pending
        registry = RateLimitRegistry(key='test', connection=self.connection)

        # Deleting a pending job removes it without promoting (it wasn't active).
        job2.delete()
        self.assertNotIn(job2.id, registry.get_pending_job_ids())
        self.assertIn(job1.id, registry.get_active_job_ids())

        # Deleting the active job frees its slot and promotes the next pending job.
        job1.delete()
        self.assertNotIn(job1.id, registry.get_active_job_ids())
        self.assertIn(job3.id, registry.get_active_job_ids())
        self.assertEqual(job3.get_status(), JobStatus.QUEUED)

    def test_cancel_with_pipeline_removes_from_registry(self):
        """Canceling via a caller-owned pipeline buffers the rate-limit removal into the
        transaction (no in-transaction promotion). A canceled pending job is dropped and
        cannot be resurrected; the freed slot is promoted by later cleanup."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # active
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # pending
        job3 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # pending
        registry = RateLimitRegistry(key='test', connection=self.connection)

        # Cancel the pending job via a pipeline → dropped from pending on EXEC.
        pipe = self.connection.pipeline()
        job2.cancel(pipeline=pipe)
        pipe.execute()
        self.assertNotIn(job2.id, registry.get_pending_job_ids())
        self.assertIn(job1.id, registry.get_active_job_ids())

        # Cancel the active job via a pipeline → dropped from active on EXEC, but the
        # pending job is not promoted inside the caller transaction.
        pipe = self.connection.pipeline()
        job1.cancel(pipeline=pipe)
        pipe.execute()
        self.assertNotIn(job1.id, registry.get_active_job_ids())
        self.assertEqual(registry.get_active_job_count(), 0)
        self.assertIn(job3.id, registry.get_pending_job_ids())

        # Maintenance cleanup promotes job3 — not the canceled job2 — proving job2 is
        # gone from pending and cannot be resurrected.
        registry.cleanup()
        self.assertIn(job3.id, registry.get_active_job_ids())
        self.assertEqual(job3.get_status(), JobStatus.QUEUED)

    def test_delete_with_pipeline_removes_from_registry(self):
        """Deleting via a caller-owned pipeline buffers the rate-limit removal into the
        transaction (no in-transaction promotion). A deleted pending job is dropped and
        cannot be resurrected; the freed slot is promoted by later cleanup."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # active
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # pending
        job3 = self.queue.enqueue(say_hello, rate_limit=rate_limit)  # pending
        registry = RateLimitRegistry(key='test', connection=self.connection)

        # Delete the pending job via a pipeline → dropped from pending on EXEC.
        pipe = self.connection.pipeline()
        job2.delete(pipeline=pipe)
        pipe.execute()
        self.assertNotIn(job2.id, registry.get_pending_job_ids())
        self.assertIn(job1.id, registry.get_active_job_ids())

        # Delete the active job via a pipeline → dropped from active on EXEC, but the
        # pending job is not promoted inside the caller transaction.
        pipe = self.connection.pipeline()
        job1.delete(pipeline=pipe)
        pipe.execute()
        self.assertNotIn(job1.id, registry.get_active_job_ids())
        self.assertEqual(registry.get_active_job_count(), 0)
        self.assertIn(job3.id, registry.get_pending_job_ids())

        # Maintenance cleanup promotes job3 — not the deleted job2 — proving job2 is gone.
        registry.cleanup()
        self.assertIn(job3.id, registry.get_active_job_ids())
        self.assertEqual(job3.get_status(), JobStatus.QUEUED)

    def test_release_on_abandoned_job_cleanup(self):
        """When StartedJobRegistry cleans up an abandoned rate-limited job,
        capacity is released and the next pending job is enqueued."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        # Simulate job1 being picked up by a worker that then dies:
        # remove from queue, add to StartedJobRegistry with an expired ttl
        self.queue.remove(job1.id)
        started_registry = StartedJobRegistry(connection=self.connection)
        execution = Execution(id='execution', job_id=job1.id, connection=self.connection)
        with self.connection.pipeline() as pipe:
            started_registry.add_execution(execution, pipe, ttl=0)
            pipe.execute()

        # Cleanup should detect the abandoned job, fail it, and release capacity
        started_registry.cleanup()

        self.assertEqual(job1.get_status(), JobStatus.FAILED)
        self.assertEqual(job2.get_status(), JobStatus.QUEUED)
        rate_limit_registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertEqual(rate_limit_registry.get_active_job_count(), 1)
        self.assertIn(job2.id, rate_limit_registry.get_active_job_ids())


class TestRateLimitScheduledJobs(RQTestCase):
    """Test that scheduled jobs honor rate limits when they become due."""

    def setUp(self):
        super().setUp()
        self.queue = Queue('default', connection=self.connection)

    def _run_scheduler_tick_for_due_job(self, rate_limit):
        """Enqueue a rate-limited job for the past and run one scheduler tick.
        Returns the scheduled job after the tick."""
        scheduled_job = self.queue.enqueue_at(datetime(2019, 1, 1), say_hello, rate_limit=rate_limit)
        scheduler = RQScheduler([self.queue], connection=self.connection)
        scheduler.acquire_locks()
        scheduler.enqueue_scheduled_jobs()
        # ScheduledJobRegistry should be drained.
        self.assertEqual(len(ScheduledJobRegistry(queue=self.queue)), 0)
        return scheduled_job

    def test_scheduled_rate_limited_job_routes_through_registry(self):
        """A due rate-limited job goes through RateLimitRegistry: it lands in active
        (and on the queue) when capacity is free, or in pending when capacity is exhausted."""
        rate_limit = RateLimit(key='test', concurrency=1)

        # Capacity free: due job acquires the slot and lands on the queue.
        scheduled_job = self._run_scheduler_tick_for_due_job(rate_limit)
        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertEqual(scheduled_job.get_status(), JobStatus.QUEUED)
        self.assertIn(scheduled_job.id, self.queue.job_ids)
        self.assertIn(scheduled_job.id, registry.get_active_job_ids())

        self.connection.flushdb()

        # Capacity exhausted: due job lands in pending, not on the queue.
        self.queue.enqueue(say_hello, rate_limit=rate_limit)
        scheduled_job = self._run_scheduler_tick_for_due_job(rate_limit)
        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertEqual(scheduled_job.get_status(), JobStatus.RATE_LIMITED)
        self.assertNotIn(scheduled_job.id, self.queue.job_ids)
        self.assertIn(scheduled_job.id, registry.get_pending_job_ids())


class TestRateLimitRetry(RQTestCase):
    """Test rate-limit slot management during job retries."""

    def setUp(self):
        super().setUp()
        self.queue = Queue('default', connection=self.connection)

    def test_delayed_retry_releases_slot(self):
        """A delayed retry of a rate-limited job releases its slot, which lets
        a pending same-key job promote into the freed capacity."""
        rate_limit = RateLimit(key='test', concurrency=1)

        # job1 will fail with delayed retry; job2 sits in pending.
        job1 = self.queue.enqueue(div_by_zero, rate_limit=rate_limit, retry=Retry(max=1, interval=30))
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        worker = SimpleWorker([self.queue], connection=self.connection)
        worker.work(max_jobs=1)

        # job1 is scheduled for retry, slot released; job2 promoted to active.
        self.assertEqual(job1.get_status(), JobStatus.SCHEDULED)
        self.assertIn(job1.id, ScheduledJobRegistry(queue=self.queue).get_job_ids())
        self.assertEqual(job2.get_status(), JobStatus.QUEUED)
        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertNotIn(job1.id, registry.get_active_job_ids())
        self.assertIn(job2.id, registry.get_active_job_ids())

    def test_immediate_retry_keeps_slot(self):
        """A rate-limited job retrying with interval=0 keeps its active slot."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(div_by_zero, rate_limit=rate_limit, retry=Retry(max=1, interval=0))
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        # Run just the failing job — it'll requeue itself for immediate retry.
        worker = SimpleWorker([self.queue], connection=self.connection)
        worker.work(max_jobs=1)

        # job1 should be back on the queue holding its slot; job2 still pending.
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)
        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertIn(job1.id, registry.get_active_job_ids())
        self.assertNotIn(job2.id, registry.get_active_job_ids())
        self.assertIn(job2.id, registry.get_pending_job_ids())

    def test_returned_delayed_retry_releases_slot(self):
        """A job that returns Retry(interval>0) releases its slot, which lets
        a pending same-key job promote into the freed capacity."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(returns_retry_with_delay, rate_limit=rate_limit)
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        worker = SimpleWorker([self.queue], connection=self.connection)
        worker.work(max_jobs=1)

        # job1 is scheduled for retry, slot released; job2 promoted to active.
        self.assertEqual(job1.get_status(), JobStatus.SCHEDULED)
        self.assertIn(job1.id, ScheduledJobRegistry(queue=self.queue).get_job_ids())
        self.assertEqual(job2.get_status(), JobStatus.QUEUED)
        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertNotIn(job1.id, registry.get_active_job_ids())
        self.assertIn(job2.id, registry.get_active_job_ids())

    def test_returned_immediate_retry_keeps_slot(self):
        """A job that returns Retry(interval=0) keeps its active slot."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(returns_retry, rate_limit=rate_limit)
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        worker = SimpleWorker([self.queue], connection=self.connection)
        worker.work(max_jobs=1)

        # job1 is back on the queue holding its slot; job2 still pending.
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)
        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertIn(job1.id, registry.get_active_job_ids())
        self.assertNotIn(job2.id, registry.get_active_job_ids())
        self.assertIn(job2.id, registry.get_pending_job_ids())

    def test_returned_retry_exhausted_releases_slot(self):
        """When a returned-Retry job exhausts its retries, the terminal failure
        releases its slot and the next pending same-key job is promoted."""
        rate_limit = RateLimit(key='test', concurrency=1)

        job1 = self.queue.enqueue(returns_retry, rate_limit=rate_limit)
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        # First run retries immediately (keeps slot), second run exhausts retries.
        worker = SimpleWorker([self.queue], connection=self.connection)
        worker.work(max_jobs=2)

        # job1 terminally failed, slot released; job2 promoted to active.
        self.assertEqual(job1.get_status(), JobStatus.FAILED)
        self.assertIn(job1.id, self.queue.failed_job_registry.get_job_ids())
        self.assertEqual(job2.get_status(), JobStatus.QUEUED)
        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertNotIn(job1.id, registry.get_active_job_ids())
        self.assertIn(job2.id, registry.get_active_job_ids())

    def _simulate_abandoned_job(self, job):
        """Put `job` into a state where it's execution has expired without
        completing (worker crash, OOM, etc.): remove job from queue and register
        an expired execution in StartedJobRegistry."""
        self.queue.remove(job.id)
        started_registry = StartedJobRegistry(connection=self.connection)
        execution = Execution(id='execution', job_id=job.id, connection=self.connection)
        with self.connection.pipeline() as pipe:
            started_registry.add_execution(execution, pipe, ttl=0)
            pipe.execute()

    def test_abandoned_retry_slot_management(self):
        """StartedJobRegistry.cleanup honors retry-interval slot semantics:
        immediate retries keep the slot (otherwise a pending same-key job
        would promote and exceed the cap), delayed retries release it."""
        rate_limit = RateLimit(key='test', concurrency=1)

        # Immediate retry: job1 keeps the slot, job2 stays pending.
        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit, retry=Retry(max=1, interval=0))
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self._simulate_abandoned_job(job1)
        StartedJobRegistry(connection=self.connection).cleanup()

        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertIn(job1.id, registry.get_active_job_ids())
        self.assertEqual(registry.get_active_job_count(), 1)
        self.assertIn(job1.id, self.queue.job_ids)
        self.assertEqual(job2.get_status(), JobStatus.RATE_LIMITED)

        self.connection.flushdb()

        # Delayed retry: job1 is scheduled, slot freed, job2 promoted.
        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit, retry=Retry(max=1, interval=30))
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        self._simulate_abandoned_job(job1)
        StartedJobRegistry(connection=self.connection).cleanup()

        registry = RateLimitRegistry(key='test', connection=self.connection)
        self.assertEqual(job1.get_status(), JobStatus.SCHEDULED)
        self.assertIn(job1.id, ScheduledJobRegistry(queue=self.queue).get_job_ids())
        self.assertIn(job2.id, registry.get_active_job_ids())
