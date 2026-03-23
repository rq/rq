from rq.job import Job
from rq.queue import Queue
from rq.rate_limit import RateLimit, RateLimitRegistry
from tests import RQTestCase


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

    def test_zero_concurrency_raises(self):
        """RateLimit raises ValueError for concurrency < 1."""
        with self.assertRaises(ValueError):
            RateLimit(key='test', concurrency=0)

    def test_negative_concurrency_raises(self):
        """RateLimit raises ValueError for negative concurrency."""
        with self.assertRaises(ValueError):
            RateLimit(key='test', concurrency=-1)


class TestRateLimitRegistry(RQTestCase):
    """Test the RateLimitRegistry class."""

    def setUp(self):
        super().setUp()
        self.rate_limit_registry = RateLimitRegistry(key='test', connection=self.connection)
        self.queue = Queue('default', connection=self.connection)

    def test_keys(self):
        """Registry produces correct Redis key names."""
        self.assertEqual(self.rate_limit_registry.active_key, 'rq:rl:test:active')
        self.assertEqual(self.rate_limit_registry.pending_key, 'rq:rl:test:pending')

    def test_add_to_pending(self):
        """add_to_pending adds a job ID to the pending sorted set."""
        self.rate_limit_registry.add_to_pending('job1')
        self.assertEqual(self.rate_limit_registry.get_pending_job_ids(), ['job1'])
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 1)

    def test_add_to_pending_preserves_order(self):
        """Jobs added to pending are ordered by timestamp."""
        self.rate_limit_registry.add_to_pending('job1', timestamp=1)
        self.rate_limit_registry.add_to_pending('job2', timestamp=2)
        self.rate_limit_registry.add_to_pending('job3', timestamp=3)
        self.assertEqual(self.rate_limit_registry.get_pending_job_ids(), ['job1', 'job2', 'job3'])

    def test_acquire_and_enqueue(self):
        """acquire_and_enqueue promotes a pending job when capacity is available,
        returns None when at capacity or no pending jobs."""
        # No pending jobs, nothing happens
        result = self.rate_limit_registry.acquire_and_enqueue('default', max_concurrency=2)
        self.assertIsNone(result)

        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job.save()

        self.rate_limit_registry.add_to_pending(job.id, timestamp=1)
        result = self.rate_limit_registry.acquire_and_enqueue('default', max_concurrency=2)

        self.assertEqual(result, job.id)
        self.assertEqual(self.rate_limit_registry.get_active_job_count(), 1)
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 0)
        self.assertIn(job.id, self.rate_limit_registry.get_active_job_ids())
        self.assertIn(job.id, self.queue.job_ids)
        status = self.connection.hget(job.key, 'status').decode()
        self.assertEqual(status, 'queued')

        # Now fill active set to capacity and verify no promotion
        self.connection.zadd(self.rate_limit_registry.active_key, {'active2': 2})
        self.rate_limit_registry.add_to_pending('job2', timestamp=3)
        result = self.rate_limit_registry.acquire_and_enqueue('default', max_concurrency=2)

        self.assertIsNone(result)
        self.assertEqual(self.rate_limit_registry.get_active_job_count(), 2)
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 1)

    def test_acquire_and_enqueue_promotes_oldest_first(self):
        """acquire_and_enqueue promotes the oldest pending job (lowest score)."""
        job1 = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job1.save()
        job2 = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job2.save()

        self.rate_limit_registry.add_to_pending(job1.id, timestamp=1)
        self.rate_limit_registry.add_to_pending(job2.id, timestamp=2)

        result = self.rate_limit_registry.acquire_and_enqueue('default', max_concurrency=1)
        self.assertEqual(result, job1.id)
        self.assertEqual(self.rate_limit_registry.get_pending_job_ids(), [job2.id])

    def test_release_capacity(self):
        """release_capacity removes from active and promotes next pending job."""
        # Set up: active1 is active, job2 is pending
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})

        job2 = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job2.save()
        self.rate_limit_registry.add_to_pending(job2.id, timestamp=2)

        result = self.rate_limit_registry.release_capacity('default', 'active1', max_concurrency=1)

        self.assertEqual(result, job2.id)
        self.assertNotIn('active1', self.rate_limit_registry.get_active_job_ids())
        self.assertIn(job2.id, self.rate_limit_registry.get_active_job_ids())
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 0)
        self.assertIn(job2.id, self.queue.job_ids)

    def test_release_capacity_no_pending(self):
        """release_capacity removes from active and returns None when no pending jobs."""
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})

        result = self.rate_limit_registry.release_capacity('default', 'active1', max_concurrency=1)

        self.assertIsNone(result)
        self.assertEqual(self.rate_limit_registry.get_active_job_count(), 0)

    def test_release_capacity_nonexistent_job(self):
        """release_capacity handles releasing a job that's not in active."""
        result = self.rate_limit_registry.release_capacity('default', 'nonexistent', max_concurrency=1)
        self.assertIsNone(result)

    def test_cancel_active_job(self):
        """cancel removes an active job and promotes the next pending job."""
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})

        job2 = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job2.save()
        self.rate_limit_registry.add_to_pending(job2.id, timestamp=2)

        result = self.rate_limit_registry.cancel('default', 'active1', max_concurrency=1)

        self.assertEqual(result, job2.id)
        self.assertNotIn('active1', self.rate_limit_registry.get_active_job_ids())
        self.assertIn(job2.id, self.rate_limit_registry.get_active_job_ids())

    def test_cancel_pending_job(self):
        """cancel removes a pending job without affecting active set."""
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})
        self.rate_limit_registry.add_to_pending('pending1', timestamp=2)

        result = self.rate_limit_registry.cancel('default', 'pending1', max_concurrency=1)

        self.assertIsNone(result)
        self.assertIn('active1', self.rate_limit_registry.get_active_job_ids())
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 0)

    def test_cancel_nonexistent_job(self):
        """cancel is a no-op for jobs not in any set."""
        result = self.rate_limit_registry.cancel('default', 'nonexistent', max_concurrency=1)
        self.assertIsNone(result)

    def test_different_keys_are_independent(self):
        """Registries with different keys don't interfere with each other."""
        registry_a = RateLimitRegistry(key='key_a', connection=self.connection)
        registry_b = RateLimitRegistry(key='key_b', connection=self.connection)

        job_a = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job_a.save()
        job_b = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job_b.save()

        registry_a.add_to_pending(job_a.id, timestamp=1)
        registry_b.add_to_pending(job_b.id, timestamp=1)

        # Fill key_a to capacity
        self.connection.zadd(registry_a.active_key, {'x': 1})
        result_a = registry_a.acquire_and_enqueue('default', max_concurrency=1)
        # key_a is full, should not promote
        self.assertIsNone(result_a)

        # key_b still has capacity
        result_b = registry_b.acquire_and_enqueue('default', max_concurrency=1)
        self.assertEqual(result_b, job_b.id)

    def test_acquire_sets_job_status_and_enqueued_at(self):
        """The Lua script sets the job's status to 'queued' and enqueued_at."""
        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job.set_status('deferred')
        job.save()

        self.rate_limit_registry.add_to_pending(job.id, timestamp=1)
        self.rate_limit_registry.acquire_and_enqueue('default', max_concurrency=1)

        status = self.connection.hget(job.key, 'status').decode()
        self.assertEqual(status, 'queued')
        enqueued_at = self.connection.hget(job.key, 'enqueued_at')
        self.assertIsNotNone(enqueued_at)

    def test_release_sets_promoted_job_status_and_enqueued_at(self):
        """release_capacity sets the promoted job's status and enqueued_at via Lua script."""
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})

        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job.set_status('deferred')
        job.save()
        self.rate_limit_registry.add_to_pending(job.id, timestamp=2)

        self.rate_limit_registry.release_capacity('default', 'active1', max_concurrency=1)

        status = self.connection.hget(job.key, 'status').decode()
        self.assertEqual(status, 'queued')
        enqueued_at = self.connection.hget(job.key, 'enqueued_at')
        self.assertIsNotNone(enqueued_at)
