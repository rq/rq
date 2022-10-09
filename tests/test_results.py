from datetime import timedelta

from tests import RQTestCase

from rq.job import Job
from rq.queue import Queue
from rq.registry import StartedJobRegistry
from rq.results import Result, get_key
from rq.utils import utcnow
from rq.worker import Worker

from .fixtures import say_hello


class TestScheduledJobRegistry(RQTestCase):

    def test_save_and_get_result(self):
        """Ensure data is saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        result = Result.get_latest(job)
        self.assertIsNone(result)

        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        result = Result.get_latest(job)
        self.assertEqual(result.return_value, 1)
        self.assertEqual(job.latest_result().return_value, 1)

        # Check that ttl is properly set
        key = get_key(job.id)
        ttl = self.connection.pttl(key)
        self.assertTrue(5000 < ttl <= 10000)

        # Check job with None return value
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=None)
        result = Result.get_latest(job)
        self.assertIsNone(result.return_value)
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=2)
        result = Result.get_latest(job)
        self.assertEqual(result.return_value, 2)

    def test_create_failure(self):
        """Ensure data is saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        Result.create_failure(job, ttl=10, exc_string='exception')
        result = Result.get_latest(job)
        self.assertEqual(result.exc_string, 'exception')

        # Check that ttl is properly set
        key = get_key(job.id)
        ttl = self.connection.pttl(key)
        self.assertTrue(5000 < ttl <= 10000)

    def test_getting_results(self):
        """Check getting all execution results"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        result_1 = Result.create_failure(job, ttl=10, exc_string='exception')
        result_2 = Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        result_3 = Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)

        # Result.get_latest() returns the latest result
        result = Result.get_latest(job)
        self.assertEqual(result, result_3)

        # Result.all() returns all results, newest first
        results = Result.all(job)
        self.assertEqual(results, [result_3, result_2, result_1])

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
        worker = Worker([queue])
        worker.register_birth()

        self.assertEqual(worker.failed_job_count, 0)
        self.assertEqual(worker.successful_job_count, 0)
        self.assertEqual(worker.total_working_time, 0)

        registry = StartedJobRegistry(connection=self.connection)
        job.started_at = utcnow()
        job.ended_at = job.started_at + timedelta(seconds=0.75)
        job._result = 'Success'
        worker.handle_job_success(job, queue, registry)

        payload = self.connection.hgetall(job.key)
        self.assertFalse(b'result' in payload.keys())
        self.assertEqual(job.result, 'Success')

        job = queue.enqueue(say_hello)
        job._result = 'Success'
        job.started_at = utcnow()
        job.ended_at = job.started_at + timedelta(seconds=0.75)

        # If `save_result_to_job` = True, result will be saved to job
        # hash, simulating older versions of RQ

        worker.handle_job_success(job, queue, registry, _save_result_to_job=True)
        payload = self.connection.hgetall(job.key)
        self.assertTrue(b'result' in payload.keys())
        # Delete all new result objects so we only have result stored in job hash,
        # this should simulate a job that was executed in an earlier RQ version
        Result.delete_all(job)
        self.assertEqual(job.result, 'Success')

    def test_job_failed_result_fallback(self):
        """Changes to job.result failure handling should be backwards compatible."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue])
        worker.register_birth()

        self.assertEqual(worker.failed_job_count, 0)
        self.assertEqual(worker.successful_job_count, 0)
        self.assertEqual(worker.total_working_time, 0)

        registry = StartedJobRegistry(connection=self.connection)
        job.started_at = utcnow()
        job.ended_at = job.started_at + timedelta(seconds=0.75)
        worker.handle_job_failure(job, exc_string='Error', queue=queue,
                                  started_job_registry=registry)

        job = Job.fetch(job.id, connection=self.connection)
        payload = self.connection.hgetall(job.key)
        self.assertFalse(b'exc_info' in payload.keys())
        self.assertEqual(job.exc_info, 'Error')

        job = queue.enqueue(say_hello)
        job.started_at = utcnow()
        job.ended_at = job.started_at + timedelta(seconds=0.75)

        # If `save_result_to_job` = True, result will be saved to job
        # hash, simulating older versions of RQ

        worker.handle_job_failure(job, exc_string='Error', queue=queue,
                                  started_job_registry=registry, _save_exc_to_job=True)
        payload = self.connection.hgetall(job.key)
        self.assertTrue(b'exc_info' in payload.keys())
        # Delete all new result objects so we only have result stored in job hash,
        # this should simulate a job that was executed in an earlier RQ version
        Result.delete_all(job)
        job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.exc_info, 'Error')
