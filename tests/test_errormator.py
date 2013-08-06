from tests import RQTestCase
from rq import Queue, Worker, get_failed_queue
from rq.contrib.errormator import register_errormator
from errormator_client.client import Client

class TestErrormator(RQTestCase):

    def test_work_fails(self):
        """Non importable jobs should be put on the failed queue event with sentry"""
        q = Queue()
        failed_q = get_failed_queue()

        # Action
        q.enqueue('_non.importable.job')
        self.assertEquals(q.count, 1)

        w = Worker([q])
        register_errormator(Client({}), w)

        w.work(burst=True)
        
        # Postconditions
        self.assertEquals(failed_q.count, 1)
        self.assertEquals(q.count, 0)
