import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from redis import Redis

from rq import Queue, Worker
from rq.intermediate_queue import IntermediateQueue
from rq.job import JobStatus
from rq.maintenance import clean_intermediate_queue
from rq.utils import get_version
from tests import RQTestCase
from tests.fixtures import say_hello


@unittest.skipIf(get_version(Redis()) < (6, 2, 0), 'Skip if Redis server < 6.2.0')
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
        self.assertTrue(datetime.now(tz=timezone.utc) - timestamp < timedelta(seconds=5))

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
            self.assertFalse(job.id in self.queue.get_job_ids())
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
            self.assertFalse(job.id in self.queue.get_job_ids())
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
            self.assertFalse(job.id in self.queue.get_job_ids())
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            self.assertTrue(job.id in job.started_job_registry.get_job_ids())
            self.assertTrue(
                (worker.execution.job_id, worker.execution.id) in job.started_job_registry.get_job_and_execution_ids()
            )

            # this should NOT remove the job from the queue, nor set the first see key
            # because it's still in the "execution state".
            # perform_job was mocked and never called success or failure.
            intermediate_queue.cleanup(worker, self.queue)
            self.assertIsNone(intermediate_queue.get_first_seen(job.id))

            self.assertTrue(job.get_status() == JobStatus.QUEUED)

    def test_clean_intermediate_queue_deprecation(self):
        with pytest.deprecated_call():
            worker = Worker(self.queue, connection=self.connection)
            clean_intermediate_queue(worker, self.queue)
