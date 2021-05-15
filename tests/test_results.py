from tests import RQTestCase

from rq.job import Job
from rq.queue import Queue
from rq.results import Result

from .fixtures import say_hello


class TestScheduledJobRegistry(RQTestCase):

    def test_save(self):
        """Ensure data is saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        print(Result.get_latest(job.id, self.connection))
        Result.create(job, Result.Type.SUCCESSFUL, return_value=1)
        result = Result.get_latest(job.id, self.connection)
        print(result)
        self.assertEqual(result.return_value, 1)
