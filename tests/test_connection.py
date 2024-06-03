from redis import ConnectionPool, Redis, SSLConnection, UnixDomainSocketConnection

from rq.connections import parse_connection
from tests import RQTestCase


class TestConnectionInheritance(RQTestCase):

    def test_parse_connection(self):
        """Test parsing the connection"""
        conn_class, pool_class, pool_kwargs = parse_connection(Redis(ssl=True))
        self.assertEqual(conn_class, Redis)
        self.assertEqual(pool_class, SSLConnection)

        path = '/tmp/redis.sock'
        pool = ConnectionPool(connection_class=UnixDomainSocketConnection, path=path)
        conn_class, pool_class, pool_kwargs = parse_connection(Redis(connection_pool=pool))
        self.assertEqual(conn_class, Redis)
        self.assertEqual(pool_class, UnixDomainSocketConnection)
        self.assertEqual(pool_kwargs, {"path": path})
