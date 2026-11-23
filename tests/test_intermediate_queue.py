from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
from unittest.mock import Mock, patch

import pytest

from rq import Queue, Worker
from rq.intermediate_queue import IntermediateQueue
from rq.job import JobStatus
from rq.maintenance import clean_intermediate_queue
from tests import RQTestCase, min_redis_version
from tests.fixtures import say_hello


@min_redis_version((6, 2, 0))
class TestIntermediateQueue(RQTestCase):
    def setUp(self):
        super().setUp()
        self.queue = Queue('foo', connection=self.connection)
        self.intermediate_queue = IntermediateQueue(self.queue.key, connection=self.connection)

    def test_set_first_seen(self):
        """Ensure that the first_seen attribute is set correctly."""
        intermediate_queue = self.intermediate_queue
        job = self.queue.enqueue(say_hello)

        # set_first_seen() should only succeed the first time around
        self.assertTrue(intermediate_queue.set_first_seen(job.id))
        self.assertFalse(intermediate_queue.set_first_seen(job.id))
        # It should succeed again after deleting the key
        self.connection.delete(intermediate_queue.get_first_seen_key(job.id))
        self.assertTrue(intermediate_queue.set_first_seen(job.id))

    def test_get_first_seen(self):
        """Ensure that the first_seen attribute is set correctly."""
        intermediate_queue = self.intermediate_queue
        job = self.queue.enqueue(say_hello)

        self.assertIsNone(intermediate_queue.get_first_seen(job.id))

        # Check first seen was set correctly
        intermediate_queue.set_first_seen(job.id)
        timestamp = intermediate_queue.get_first_seen(job.id)
        assert timestamp
        self.assertLess(datetime.now(tz=timezone.utc) - timestamp, timedelta(seconds=5))

    def test_should_be_cleaned_up(self):
        """Job in the intermediate queue should be cleaned up if it was seen more than 1 minute ago."""
        intermediate_queue = self.intermediate_queue
        job = self.queue.enqueue(say_hello)

        # Returns False if there's no first seen timestamp
        self.assertFalse(intermediate_queue.should_be_cleaned_up(job.id))
        # Returns False since first seen timestamp is less than 1 minute ago
        intermediate_queue.set_first_seen(job.id)
        self.assertFalse(intermediate_queue.should_be_cleaned_up(job.id))

        first_seen_key = intermediate_queue.get_first_seen_key(job.id)
        two_minutes_ago = datetime.now(tz=timezone.utc) - timedelta(minutes=2)
        self.connection.set(first_seen_key, two_minutes_ago.timestamp(), ex=10)
        self.assertTrue(intermediate_queue.should_be_cleaned_up(job.id))

    def test_get_job_ids(self):
        """Dequeueing job from a single queue moves job to intermediate queue."""
        intermediate_queue = self.intermediate_queue
        job_1 = self.queue.enqueue(say_hello)

        # Ensure that the intermediate queue is empty
        self.connection.delete(intermediate_queue.key)

        # Job ID is not in intermediate queue
        self.assertEqual(intermediate_queue.get_job_ids(), [])
        result = Queue.dequeue_any([self.queue], timeout=None, connection=self.connection)
        assert result
        _job, queue = result
        # After job is dequeued, the job ID is in the intermediate queue
        self.assertEqual(intermediate_queue.get_job_ids(), [job_1.id])

        # Test the blocking version
        job_2 = queue.enqueue(say_hello)
        result = Queue.dequeue_any([queue], timeout=1, connection=self.connection)
        assert result
        _job, queue = result
        # After job is dequeued, the job ID is in the intermediate queue
        self.assertEqual(intermediate_queue.get_job_ids(), [job_1.id, job_2.id])

        # After job_1.id is removed, only job_2.id is in the intermediate queue
        intermediate_queue.remove(job_1.id)
        self.assertEqual(intermediate_queue.get_job_ids(), [job_2.id])

    def test_cleanup_intermediate_queue_in_maintenance(self):
        """Ensure jobs stuck in the intermediate queue are cleaned up."""
        intermediate_queue = self.intermediate_queue
        job = self.queue.enqueue(say_hello)
        self.connection.delete(intermediate_queue.key)

        # If job execution fails after it's dequeued, job should be in the intermediate queue
        # and it's status is still QUEUED
        with patch.object(Worker, 'execute_job'):
            worker = Worker(self.queue, connection=self.connection)
            worker.work(burst=True)

            # If worker.execute_job() does nothing, job status should be `queued`
            # even though it's not in the queue, but it should be in the intermediate queue
            self.assertEqual(job.get_status(), JobStatus.QUEUED)
            self.assertNotIn(job.id, self.queue.get_job_ids())
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            self.assertIsNone(intermediate_queue.get_first_seen(job.id))
            intermediate_queue.cleanup(worker, self.queue)
            # After intermediate_queue.cleanup is called, the job should be marked as seen,
            # but since it's been less than 1 minute, it should not be cleaned up
            self.assertIsNotNone(intermediate_queue.get_first_seen(job.id))
            self.assertFalse(intermediate_queue.should_be_cleaned_up(job.id))
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            # If we set the first seen timestamp to 2 minutes ago, the job should be cleaned up
            first_seen_key = intermediate_queue.get_first_seen_key(job.id)
            two_minutes_ago = datetime.now(tz=timezone.utc) - timedelta(minutes=2)
            self.connection.set(first_seen_key, two_minutes_ago.timestamp(), ex=10)

            intermediate_queue.cleanup(worker, self.queue)
            self.assertEqual(intermediate_queue.get_job_ids(), [])
            self.assertEqual(job.get_status(), 'failed')

            job = self.queue.enqueue(say_hello)
            worker.work(burst=True)
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            # If job is gone, it should be immediately removed from the intermediate queue
            job.delete()
            intermediate_queue.cleanup(worker, self.queue)
            self.assertEqual(intermediate_queue.get_job_ids(), [])

    def test_cleanup_intermediate_queue(self):
        """Ensure jobs stuck in the intermediate queue are cleaned up."""
        intermediate_queue = self.intermediate_queue
        job = self.queue.enqueue(say_hello)

        self.connection.delete(intermediate_queue.key)

        # If job execution fails after it's dequeued, job should be in the intermediate queue
        # and it's status is still QUEUED
        with patch.object(Worker, 'execute_job'):
            worker = Worker(self.queue, connection=self.connection)
            worker.work(burst=True)

            # If worker.execute_job() does nothing, job status should be `queued`
            # even though it's not in the queue, but it should be in the intermediate queue
            self.assertEqual(job.get_status(), JobStatus.QUEUED)
            self.assertNotIn(job.id, self.queue.get_job_ids())
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            self.assertIsNone(intermediate_queue.get_first_seen(job.id))
            intermediate_queue.cleanup(worker, self.queue)
            # After intermediate_queue.cleanup is called, the job should be marked as seen,
            # but since it's been less than 1 minute, it should not be cleaned up
            self.assertIsNotNone(intermediate_queue.get_first_seen(job.id))
            self.assertFalse(intermediate_queue.should_be_cleaned_up(job.id))
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            # If we set the first seen timestamp to 2 minutes ago, the job should be cleaned up
            first_seen_key = intermediate_queue.get_first_seen_key(job.id)
            two_minutes_ago = datetime.now(tz=timezone.utc) - timedelta(minutes=2)
            self.connection.set(first_seen_key, two_minutes_ago.timestamp(), ex=10)

            intermediate_queue.cleanup(worker, self.queue)
            self.assertEqual(intermediate_queue.get_job_ids(), [])
            self.assertEqual(job.get_status(), 'failed')

            job = self.queue.enqueue(say_hello)
            worker.work(burst=True)
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            # If job is gone, it should be immediately removed from the intermediate queue
            job.delete()
            intermediate_queue.cleanup(worker, self.queue)
            self.assertEqual(intermediate_queue.get_job_ids(), [])

    def test_no_cleanup_while_in_started_queue(self):
        """Ensure jobs stuck in the intermediate queue are cleaned up."""
        intermediate_queue = self.intermediate_queue
        job = self.queue.enqueue(say_hello)

        self.connection.delete(intermediate_queue.key)

        # If job execution fails after it's dequeued, job should be in the intermediate queue
        # and it's status is still QUEUED
        with patch.object(Worker, 'perform_job'):
            worker = Worker(self.queue, connection=self.connection)
            worker.work(burst=True)

            # If worker.perform_job() does nothing, job status should be `queued`
            # even though it's not in the queue, but it should be in the intermediate queue
            # and the job should be in the started queue (since we only mocked perform_job)
            self.assertEqual(job.get_status(), JobStatus.QUEUED)
            self.assertNotIn(job.id, self.queue.get_job_ids())
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            self.assertIn(job.id, job.started_job_registry.get_job_ids())
            # The worker's in-process execution is dropped once the horse exits,
            # but the execution itself is still in Redis
            execution = job.get_executions()[0]
            self.assertIn(
                (execution.job_id, execution.id),
                job.started_job_registry.get_job_and_execution_ids(),
            )

            # this should NOT remove the job from the queue, nor set the first see key
            # because it's still in the "execution state".
            # perform_job was mocked and never called success or failure.
            intermediate_queue.cleanup(worker, self.queue)
            self.assertIsNone(intermediate_queue.get_first_seen(job.id))

            self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_clean_intermediate_queue_deprecation(self):
        with pytest.deprecated_call():
            worker = Worker(self.queue, connection=self.connection)
            clean_intermediate_queue(worker, self.queue)


class TestIntermediateQueueRecovery(RQTestCase):
    def setUp(self):
        super().setUp()
        self.queue = Queue('recovery', connection=self.connection)
        self.intermediate_queue = IntermediateQueue(self.queue.key, connection=self.connection)

    def test_concurrent_requeue_attempts_add_only_one_ready_entry(self):
        """Concurrent recovery is atomic and idempotent for intermediate jobs."""
        job = self.queue.enqueue(say_hello)
        original_data = self.connection.hget(job.key, 'data')
        self.connection.lrem(self.queue.key, 1, job.id)
        self.connection.rpush(self.intermediate_queue.key, job.id)
        barrier = Barrier(2)

        def requeue_job():
            barrier.wait()
            self.intermediate_queue.requeue(job.id)

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(requeue_job) for _ in range(2)]
            for future in futures:
                future.result()

        self.assertEqual(self.queue.get_job_ids(), [job.id])
        self.assertEqual(self.intermediate_queue.get_job_ids(), [])
        self.assertEqual(self.connection.hget(job.key, 'data'), original_data)

    def test_requeue_when_legacy_dequeue_already_popped_job(self):
        """Recovery restores a job even when the legacy path left no intermediate ID."""
        job = self.queue.enqueue(say_hello)
        original_data = self.connection.hget(job.key, 'data')
        self.connection.lpop(self.queue.key)

        self.assertEqual(self.intermediate_queue.get_job_ids(), [])
        self.intermediate_queue.requeue(job.id)

        self.assertEqual(self.queue.get_job_ids(), [job.id])
        self.assertEqual(self.connection.hget(job.key, 'data'), original_data)

    def test_cleanup_propagates_unrelated_runtime_error(self):
        """Cleanup leaves unrelated RuntimeError failures in the intermediate queue."""
        job = self.queue.enqueue(say_hello)
        self.connection.lrem(self.queue.key, 1, job.id)
        self.connection.rpush(self.intermediate_queue.key, job.id)

        with patch.object(self.queue, 'fetch_job', side_effect=RuntimeError('cleanup bug')):
            with self.assertRaisesRegex(RuntimeError, 'cleanup bug'):
                self.intermediate_queue.cleanup(Mock(), self.queue)

        self.assertEqual(self.intermediate_queue.get_job_ids(), [job.id])
        self.assertEqual(self.queue.get_job_ids(), [])
