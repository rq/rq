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
        result = Result.create(job, Result.Type.SUCCESSFUL, return_value=1)
        print(Result.get_latest(job.id, self.connection))