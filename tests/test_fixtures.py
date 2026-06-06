from rq import Queue
from rq.connections import RedisConnectionBuilder
from tests import RQTestCase, fixtures


class TestFixtures(RQTestCase):
    def test_rpush_fixture(self):
        connection_builder = RedisConnectionBuilder.parse_connection(self.connection)
        fixtures.rpush('foo', 'bar', connection_builder)
        assert self.connection.lrange('foo', 0, 0)[0].decode() == 'bar'

    def test_start_worker_fixture(self):
        queue = Queue(name='testing', connection=self.connection)
        queue.enqueue(fixtures.say_hello)
        connection_builder = RedisConnectionBuilder.parse_connection(self.connection)
        fixtures.start_worker(queue.name, connection_builder, 'w1', True)
        assert not queue.jobs
