from rq import Queue
from rq.connections import get_connection_kwargs
from tests import RQTestCase, fixtures


class TestFixtures(RQTestCase):
    def test_rpush_fixture(self):
        connection_kwargs = get_connection_kwargs(self.connection)
        fixtures.rpush('foo', 'bar', connection_kwargs)
        assert self.connection.lrange('foo', 0, 0)[0].decode() == 'bar'

    def test_start_worker_fixture(self):
        queue = Queue(name='testing', connection=self.connection)
        queue.enqueue(fixtures.say_hello)
        conn_kwargs = get_connection_kwargs(self.connection)
        fixtures.start_worker(queue.name, conn_kwargs, 'w1', True)
        assert not queue.jobs
