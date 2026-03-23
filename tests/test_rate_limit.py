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
        self.queue = Queue('default', connection=self.connection)

    def test_add_to_pending(self):
        """Jobs added to pending are ordered by timestamp."""
        self.rate_limit_registry.add_to_pending('job1', timestamp=1)
        self.rate_limit_registry.add_to_pending('job2', timestamp=2)
        self.rate_limit_registry.add_to_pending('job3', timestamp=3)
        self.assertEqual(self.rate_limit_registry.get_pending_job_ids(), ['job1', 'job2', 'job3'])

    def test_acquire_and_enqueue(self):
        """acquire_and_enqueue enqueues a pending job when capacity is available,
        returns None when at capacity or no pending jobs."""
        # No pending jobs, nothing happens
        result = self.rate_limit_registry.acquire_and_enqueue('default', max_concurrency=2)
        self.assertIsNone(result)

        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job.save()

        self.rate_limit_registry.add_to_pending(job.id)
        result = self.rate_limit_registry.acquire_and_enqueue('default', max_concurrency=2)

        self.assertEqual(result, job.id)
        self.assertEqual(self.rate_limit_registry.get_active_job_count(), 1)
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 0)
        self.assertIn(job.id, self.rate_limit_registry.get_active_job_ids())
        self.assertIn(job.id, self.queue.job_ids)
        self.assertEqual(job.get_status(), 'queued')
        self.assertIsNotNone(self.connection.hget(job.key, 'enqueued_at'))

        # Now fill active set to capacity and verify nothing is enqueued
        self.connection.zadd(self.rate_limit_registry.active_key, {'active2': 2})
        self.rate_limit_registry.add_to_pending('job2')
        result = self.rate_limit_registry.acquire_and_enqueue('default', max_concurrency=2)

        self.assertIsNone(result)
        self.assertEqual(self.rate_limit_registry.get_active_job_count(), 2)
        self.assertEqual(self.rate_limit_registry.get_pending_job_count(), 1)

    def test_acquire_and_enqueue_enqueues_oldest_first(self):
        """acquire_and_enqueue enqueues the oldest pending job (lowest score)."""
        job1 = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job1.save()
        job2 = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job2.save()

        self.rate_limit_registry.add_to_pending(job1.id, timestamp=1)
        self.rate_limit_registry.add_to_pending(job2.id, timestamp=2)

        result = self.rate_limit_registry.acquire_and_enqueue('default', max_concurrency=1)
        self.assertEqual(result, job1.id)
        self.assertEqual(self.rate_limit_registry.get_pending_job_ids(), [job2.id])

    def test_release_capacity_and_enqueue(self):
        """release_capacity_and_enqueue removes from active and enqueues next pending job."""
        # Releasing a nonexistent job is a no-op
        result = self.rate_limit_registry.release_capacity_and_enqueue('default', 'nonexistent', max_concurrency=1)
        self.assertIsNone(result)

        # Releasing with no pending jobs returns None
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})
        result = self.rate_limit_registry.release_capacity_and_enqueue('default', 'active1', max_concurrency=1)
        self.assertIsNone(result)
        self.assertEqual(self.rate_limit_registry.get_active_job_count(), 0)

        # Releasing with a pending job enqueues it
        self.connection.zadd(self.rate_limit_registry.active_key, {'active2': 1})
        job = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job.save()
        self.rate_limit_registry.add_to_pending(job.id)

        result = self.rate_limit_registry.release_capacity_and_enqueue('default', 'active2', max_concurrency=1)
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

        job2 = Job.create(func='tests.fixtures.say_hello', connection=self.connection)
        job2.save()
        self.rate_limit_registry.add_to_pending(job2.id)

        result = self.rate_limit_registry.cancel('default', 'active1', max_concurrency=1)

        self.assertEqual(result, job2.id)
        self.assertNotIn('active1', self.rate_limit_registry.get_active_job_ids())
        self.assertIn(job2.id, self.rate_limit_registry.get_active_job_ids())

    def test_cancel_pending_job(self):
        """cancel removes a pending job without affecting active set."""
        self.connection.zadd(self.rate_limit_registry.active_key, {'active1': 1})
        self.rate_limit_registry.add_to_pending('pending1')

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

        registry_a.add_to_pending(job_a.id)
        registry_b.add_to_pending(job_b.id)

        # Fill key_a to capacity
        self.connection.zadd(registry_a.active_key, {'x': 1})
        result_a = registry_a.acquire_and_enqueue('default', max_concurrency=1)
        # key_a is full, should not enqueue
        self.assertIsNone(result_a)

        # key_b still has capacity
        result_b = registry_b.acquire_and_enqueue('default', max_concurrency=1)
        self.assertEqual(result_b, job_b.id)
