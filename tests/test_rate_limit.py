from rq.executions import Execution
from rq.job import Job, JobStatus
from rq.queue import Queue
from rq.rate_limit import RateLimit, RateLimitRegistry
from rq.registry import StartedJobRegistry
from rq.worker import SimpleWorker
from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


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

        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection, origin='default')
        job.save()

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
        job1 = Job.create(func='tests.fixtures.say_hello', connection=self.connection, origin='default')
        job1.save()
        job2 = Job.create(func='tests.fixtures.say_hello', connection=self.connection, origin='default')
        job2.save()

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
        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection, origin='default')
        job.save()
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

        job2 = Job.create(func='tests.fixtures.say_hello', connection=self.connection, origin='default')
        job2.save()
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

    def test_different_keys_are_independent(self):
        """Registries with different keys don't interfere with each other."""
        registry_a = RateLimitRegistry(key='key_a', connection=self.connection)
        registry_b = RateLimitRegistry(key='key_b', connection=self.connection)

        job_a = Job.create(func='tests.fixtures.say_hello', connection=self.connection, origin='default')
        job_a.save()
        job_b = Job.create(func='tests.fixtures.say_hello', connection=self.connection, origin='default')
        job_b.save()

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

    def test_enqueue_with_rate_limit(self):
        """Jobs exceeding concurrency limit are deferred, others are queued."""
        rate_limit = RateLimit(key='test', concurrency=2)

        job1 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        job2 = self.queue.enqueue(say_hello, rate_limit=rate_limit)
        job3 = self.queue.enqueue(say_hello, rate_limit=rate_limit)

        self.assertEqual(job1.get_status(), JobStatus.QUEUED)
        self.assertEqual(job2.get_status(), JobStatus.QUEUED)
        self.assertEqual(job3.get_status(), JobStatus.RATE_LIMITED)

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
