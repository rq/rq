from rq import Queue
from tests import RQTestCase, fixtures


class TestFixtures(RQTestCase):
    def test_rpush_fixture(self):
        fixtures.rpush('foo', 'bar')
        assert self.testconn.lrange('foo', 0, 0)[0].decode() == 'bar'

    def test_start_worker_fixture(self):
        queue = Queue(name='testing', connection=self.testconn)
        queue.enqueue(fixtures.say_hello)
        conn_kwargs = self.testconn.connection_pool.connection_kwargs
        fixtures.start_worker(queue.name, conn_kwargs, 'w1', True)
        assert not queue.jobs
