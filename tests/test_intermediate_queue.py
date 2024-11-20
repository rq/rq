import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from redis import Redis

from rq import Queue, Worker
from rq.intermediate_queue import IntermediateQueue
from rq.maintenance import clean_intermediate_queue
from rq.utils import get_version
from tests import RQTestCase
from tests.fixtures import say_hello


@unittest.skipIf(get_version(Redis()) < (6, 2, 0), 'Skip if Redis server < 6.2.0')
class TestIntermediateQueue(RQTestCase):
    def test_set_first_seen(self):
        """Ensure that the first_seen attribute is set correctly."""
        queue = Queue('foo', connection=self.connection)
        intermediate_queue = IntermediateQueue(queue.key, connection=self.connection)
        job = queue.enqueue(say_hello)

        # set_first_seen() should only succeed the first time around
        self.assertTrue(intermediate_queue.set_first_seen(job.id))
        self.assertFalse(intermediate_queue.set_first_seen(job.id))
        # It should succeed again after deleting the key
        self.connection.delete(intermediate_queue.get_first_seen_key(job.id))
        self.assertTrue(intermediate_queue.set_first_seen(job.id))

    def test_get_first_seen(self):
        """Ensure that the first_seen attribute is set correctly."""
        queue = Queue('foo', connection=self.connection)
        intermediate_queue = IntermediateQueue(queue.key, connection=self.connection)
        job = queue.enqueue(say_hello)

        self.assertIsNone(intermediate_queue.get_first_seen(job.id))

        # Check first seen was set correctly
        intermediate_queue.set_first_seen(job.id)
        timestamp = intermediate_queue.get_first_seen(job.id)
        self.assertTrue(datetime.now(tz=timezone.utc) - timestamp < timedelta(seconds=5))  # type: ignore

    def test_should_be_cleaned_up(self):
        """Job in the intermediate queue should be cleaned up if it was seen more than 1 minute ago."""
        queue = Queue('foo', connection=self.connection)
        intermediate_queue = IntermediateQueue(queue.key, connection=self.connection)
        job = queue.enqueue(say_hello)

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
        queue = Queue('foo', connection=self.connection)
        job_1 = queue.enqueue(say_hello)

        intermediate_queue = IntermediateQueue(queue.key, connection=self.connection)

        # Ensure that the intermediate queue is empty
        self.connection.delete(intermediate_queue.key)

        # Job ID is not in intermediate queue
        self.assertEqual(intermediate_queue.get_job_ids(), [])
        job, queue = Queue.dequeue_any([queue], timeout=None, connection=self.connection)
        # After job is dequeued, the job ID is in the intermediate queue
        self.assertEqual(intermediate_queue.get_job_ids(), [job_1.id])

        # Test the blocking version
        job_2 = queue.enqueue(say_hello)
        job, queue = Queue.dequeue_any([queue], timeout=1, connection=self.connection)
        # After job is dequeued, the job ID is in the intermediate queue
        self.assertEqual(intermediate_queue.get_job_ids(), [job_1.id, job_2.id])

        # After job_1.id is removed, only job_2.id is in the intermediate queue
        intermediate_queue.remove(job_1.id)
        self.assertEqual(intermediate_queue.get_job_ids(), [job_2.id])

    def test_cleanup_intermediate_queue_in_maintenance(self):
        """Ensure jobs stuck in the intermediate queue are cleaned up."""
        queue = Queue('foo', connection=self.connection)
        job = queue.enqueue(say_hello)

        intermediate_queue = IntermediateQueue(queue.key, connection=self.connection)
        self.connection.delete(intermediate_queue.key)

        # If job execution fails after it's dequeued, job should be in the intermediate queue
        # and it's status is still QUEUED
        with patch.object(Worker, 'execute_job'):
            worker = Worker(queue, connection=self.connection)
            worker.work(burst=True)

            # If worker.execute_job() does nothing, job status should be `queued`
            # even though it's not in the queue, but it should be in the intermediate queue
            self.assertEqual(job.get_status(), 'queued')
            self.assertFalse(job.id in queue.get_job_ids())
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            self.assertIsNone(intermediate_queue.get_first_seen(job.id))
            clean_intermediate_queue(worker, queue)
            # After clean_intermediate_queue is called, the job should be marked as seen,
            # but since it's been less than 1 minute, it should not be cleaned up
            self.assertIsNotNone(intermediate_queue.get_first_seen(job.id))
            self.assertFalse(intermediate_queue.should_be_cleaned_up(job.id))
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            # If we set the first seen timestamp to 2 minutes ago, the job should be cleaned up
            first_seen_key = intermediate_queue.get_first_seen_key(job.id)
            two_minutes_ago = datetime.now(tz=timezone.utc) - timedelta(minutes=2)
            self.connection.set(first_seen_key, two_minutes_ago.timestamp(), ex=10)

            clean_intermediate_queue(worker, queue)
            self.assertEqual(intermediate_queue.get_job_ids(), [])
            self.assertEqual(job.get_status(), 'failed')

            job = queue.enqueue(say_hello)
            worker.work(burst=True)
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            # If job is gone, it should be immediately removed from the intermediate queue
            job.delete()
            clean_intermediate_queue(worker, queue)
            self.assertEqual(intermediate_queue.get_job_ids(), [])

    def test_cleanup_intermediate_queue(self):
        """Ensure jobs stuck in the intermediate queue are cleaned up."""
        queue = Queue('foo', connection=self.connection)
        job = queue.enqueue(say_hello)

        intermediate_queue = IntermediateQueue(queue.key, connection=self.connection)
        self.connection.delete(intermediate_queue.key)

        # If job execution fails after it's dequeued, job should be in the intermediate queue
        # and it's status is still QUEUED
        with patch.object(Worker, 'execute_job'):
            worker = Worker(queue, connection=self.connection)
            worker.work(burst=True)

            # If worker.execute_job() does nothing, job status should be `queued`
            # even though it's not in the queue, but it should be in the intermediate queue
            self.assertEqual(job.get_status(), 'queued')
            self.assertFalse(job.id in queue.get_job_ids())
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            self.assertIsNone(intermediate_queue.get_first_seen(job.id))
            intermediate_queue.cleanup(worker, queue)
            # After clean_intermediate_queue is called, the job should be marked as seen,
            # but since it's been less than 1 minute, it should not be cleaned up
            self.assertIsNotNone(intermediate_queue.get_first_seen(job.id))
            self.assertFalse(intermediate_queue.should_be_cleaned_up(job.id))
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            # If we set the first seen timestamp to 2 minutes ago, the job should be cleaned up
            first_seen_key = intermediate_queue.get_first_seen_key(job.id)
            two_minutes_ago = datetime.now(tz=timezone.utc) - timedelta(minutes=2)
            self.connection.set(first_seen_key, two_minutes_ago.timestamp(), ex=10)

            intermediate_queue.cleanup(worker, queue)
            self.assertEqual(intermediate_queue.get_job_ids(), [])
            self.assertEqual(job.get_status(), 'failed')

            job = queue.enqueue(say_hello)
            worker.work(burst=True)
            self.assertEqual(intermediate_queue.get_job_ids(), [job.id])

            # If job is gone, it should be immediately removed from the intermediate queue
            job.delete()
            intermediate_queue.cleanup(worker, queue)
            self.assertEqual(intermediate_queue.get_job_ids(), [])
