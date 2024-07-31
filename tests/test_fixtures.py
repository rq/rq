from rq import Queue
from tests import RQTestCase, fixtures


class TestFixtures(RQTestCase):
    def test_rpush_fixture(self):
        connection_kwargs = self.connection.connection_pool.connection_kwargs
        fixtures.rpush('foo', 'bar', connection_kwargs)
        assert self.connection.lrange('foo', 0, 0)[0].decode() == 'bar'

    def test_start_worker_fixture(self):
        queue = Queue(name='testing', connection=self.connection)
        queue.enqueue(fixtures.say_hello)
        conn_kwargs = self.connection.connection_pool.connection_kwargs
        fixtures.start_worker(queue.name, conn_kwargs, 'w1', True)
        assert not queue.jobs
