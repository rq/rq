from tests import RQTestCase

from rq.job import Job
from rq.queue import Queue
from rq.results import Result, get_key

from .fixtures import say_hello


class TestScheduledJobRegistry(RQTestCase):

    def test_save(self):
        """Ensure data is saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        result = Result.get_latest(job.id, self.connection)
        self.assertEqual(result.return_value, 1)

        # Check that ttl is properly set
        key = get_key(job.id)
        ttl = self.connection.pttl(key)
        self.assertTrue(5000 < ttl <= 10000)
