import tempfile
import time
import unittest
from datetime import timedelta
from unittest.mock import PropertyMock, patch

from redis import Redis

from rq.defaults import UNSERIALIZABLE_RETURN_VALUE_PAYLOAD
from rq.job import Job
from rq.queue import Queue
from rq.registry import StartedJobRegistry
from rq.results import Result, get_key
from rq.utils import get_version, utcnow
from rq.worker import Worker
from tests import RQTestCase

from .fixtures import div_by_zero, say_hello


@unittest.skipIf(get_version(Redis()) < (5, 0, 0), 'Skip if Redis server < 5.0')
class TestScheduledJobRegistry(RQTestCase):
    def test_save_and_get_result(self):
        """Ensure data is saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        result = Result.fetch_latest(job)
        self.assertIsNone(result)

        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        result = Result.fetch_latest(job)
        self.assertEqual(result.return_value, 1)
        self.assertEqual(job.latest_result().return_value, 1)

        # Check that ttl is properly set
        key = get_key(job.id)
        ttl = self.connection.pttl(key)
        self.assertTrue(5000 < ttl <= 10000)

        # Check job with None return value
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=None)
        result = Result.fetch_latest(job)
        self.assertIsNone(result.return_value)
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=2)
        result = Result.fetch_latest(job)
        self.assertEqual(result.return_value, 2)

    def test_create_failure(self):
        """Ensure data is saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        Result.create_failure(job, ttl=10, exc_string='exception')
        result = Result.fetch_latest(job)
        self.assertEqual(result.exc_string, 'exception')

        # Check that ttl is properly set
        key = get_key(job.id)
        ttl = self.connection.pttl(key)
        self.assertTrue(5000 < ttl <= 10000)

    def test_getting_results(self):
        """Check getting all execution results"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        # latest_result() returns None when there's no result
        self.assertIsNone(job.latest_result())

        result_1 = Result.create_failure(job, ttl=10, exc_string='exception')
        result_2 = Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        result_3 = Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)

        # Result.fetch_latest() returns the latest result
        result = Result.fetch_latest(job)
        self.assertEqual(result, result_3)
        self.assertEqual(job.latest_result(), result_3)

        # Result.all() and job.results() returns all results, newest first
        results = Result.all(job)
        self.assertEqual(results, [result_3, result_2, result_1])
        self.assertEqual(job.results(), [result_3, result_2, result_1])

    def test_count(self):
        """Result.count(job) returns number of results"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        self.assertEqual(Result.count(job), 0)
        Result.create_failure(job, ttl=10, exc_string='exception')
        self.assertEqual(Result.count(job), 1)
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        self.assertEqual(Result.count(job), 2)

    def test_delete_all(self):
        """Result.delete_all(job) deletes all results from Redis"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        Result.create_failure(job, ttl=10, exc_string='exception')
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        Result.delete_all(job)
        self.assertEqual(Result.count(job), 0)

    def test_job_successful_result_fallback(self):
        """Changes to job.result handling should be backwards compatible."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.register_birth()

        self.assertEqual(worker.failed_job_count, 0)
        self.assertEqual(worker.successful_job_count, 0)
        self.assertEqual(worker.total_working_time, 0)

        # These should only run on workers that supports Redis streams
        registry = StartedJobRegistry(connection=self.connection)
        job.started_at = utcnow()
        job.ended_at = job.started_at + timedelta(seconds=0.75)
        job._result = 'Success'
        worker.handle_job_success(job, queue, registry)

        payload = self.connection.hgetall(job.key)
        self.assertFalse(b'result' in payload.keys())
        self.assertEqual(job.result, 'Success')

        with patch('rq.worker.Worker.supports_redis_streams', new_callable=PropertyMock) as mock:
            with patch('rq.job.Job.supports_redis_streams', new_callable=PropertyMock) as job_mock:
                job_mock.return_value = False
                mock.return_value = False
                worker = Worker([queue])
                worker.register_birth()
                job = queue.enqueue(say_hello)
                job._result = 'Success'
                job.started_at = utcnow()
                job.ended_at = job.started_at + timedelta(seconds=0.75)

                # If `save_result_to_job` = True, result will be saved to job
                # hash, simulating older versions of RQ

                worker.handle_job_success(job, queue, registry)
                payload = self.connection.hgetall(job.key)
                self.assertTrue(b'result' in payload.keys())
                # Delete all new result objects so we only have result stored in job hash,
                # this should simulate a job that was executed in an earlier RQ version
                self.assertEqual(job.result, 'Success')

    def test_job_failed_result_fallback(self):
        """Changes to job.result failure handling should be backwards compatible."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.register_birth()

        self.assertEqual(worker.failed_job_count, 0)
        self.assertEqual(worker.successful_job_count, 0)
        self.assertEqual(worker.total_working_time, 0)

        registry = StartedJobRegistry(connection=self.connection)
        job.started_at = utcnow()
        job.ended_at = job.started_at + timedelta(seconds=0.75)
        worker.handle_job_failure(job, exc_string='Error', queue=queue, started_job_registry=registry)

        job = Job.fetch(job.id, connection=self.connection)
        payload = self.connection.hgetall(job.key)
        self.assertFalse(b'exc_info' in payload.keys())
        self.assertEqual(job.exc_info, 'Error')

        with patch('rq.worker.Worker.supports_redis_streams', new_callable=PropertyMock) as mock:
            with patch('rq.job.Job.supports_redis_streams', new_callable=PropertyMock) as job_mock:
                job_mock.return_value = False
                mock.return_value = False
                worker = Worker([queue])
                worker.register_birth()

                job = queue.enqueue(say_hello)
                job.started_at = utcnow()
                job.ended_at = job.started_at + timedelta(seconds=0.75)

                # If `save_result_to_job` = True, result will be saved to job
                # hash, simulating older versions of RQ

                worker.handle_job_failure(job, exc_string='Error', queue=queue, started_job_registry=registry)
                payload = self.connection.hgetall(job.key)
                self.assertTrue(b'exc_info' in payload.keys())
                # Delete all new result objects so we only have result stored in job hash,
                # this should simulate a job that was executed in an earlier RQ version
                Result.delete_all(job)
                job = Job.fetch(job.id, connection=self.connection)
                self.assertEqual(job.exc_info, 'Error')

    def test_job_return_value(self):
        """Test job.return_value"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNone(job.return_value())

        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        self.assertEqual(job.return_value(), 1)

        # Returns None if latest result is a failure
        Result.create_failure(job, ttl=10, exc_string='exception')
        self.assertIsNone(job.return_value(refresh=True))

    def test_job_return_value_sync(self):
        """Test job.return_value when queue.is_async=False"""
        queue = Queue(connection=self.connection, is_async=False)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNotNone(job.return_value())

        job = queue.enqueue(div_by_zero)
        self.assertEqual(job.latest_result().type, Result.Type.FAILED)

    def test_job_return_value_result_ttl_infinity(self):
        """Test job.return_value when queue.result_ttl=-1"""
        queue = Queue(connection=self.connection, result_ttl=-1)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNone(job.return_value())

        Result.create(job, Result.Type.SUCCESSFUL, ttl=-1, return_value=1)
        self.assertEqual(job.return_value(), 1)

    def test_job_return_value_result_ttl_zero(self):
        """Test job.return_value when queue.result_ttl=0"""
        queue = Queue(connection=self.connection, result_ttl=0)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNone(job.return_value())

        Result.create(job, Result.Type.SUCCESSFUL, ttl=0, return_value=1)
        self.assertIsNone(job.return_value())

    def test_job_return_value_unserializable(self):
        """Test job.return_value when it is not serializable"""
        queue = Queue(connection=self.connection, result_ttl=0)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNone(job.return_value())

        # tempfile.NamedTemporaryFile() is not picklable
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=tempfile.NamedTemporaryFile())
        self.assertEqual(job.return_value(), UNSERIALIZABLE_RETURN_VALUE_PAYLOAD)
        self.assertEqual(Result.count(job), 1)

        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        self.assertEqual(Result.count(job), 2)

    def test_blocking_results(self):
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        # Should block if there's no result.
        timeout = 1
        self.assertIsNone(Result.fetch_latest(job))
        started_at = time.time()
        self.assertIsNone(Result.fetch_latest(job, timeout=timeout))
        blocked_for = time.time() - started_at
        self.assertGreaterEqual(blocked_for, timeout)

        # Shouldn't block if there's already a result present.
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        timeout = 1
        result_sync = Result.fetch_latest(job)
        started_at = time.time()
        result_blocking = Result.fetch_latest(job, timeout=timeout)
        blocked_for = time.time() - started_at
        self.assertEqual(result_sync.return_value, result_blocking.return_value)
        self.assertGreater(timeout, blocked_for)

        # Should return the latest result if there are multiple.
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=2)
        result_blocking = Result.fetch_latest(job, timeout=1)
        self.assertEqual(result_blocking.return_value, 2)
