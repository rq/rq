from datetime import datetime, timedelta, timezone

from rq import Queue
from rq.intermediate_queue import IntermediateQueue
from tests import RQTestCase
from tests.fixtures import say_hello


class TestWorker(RQTestCase):
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
