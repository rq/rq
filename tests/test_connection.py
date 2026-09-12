import redis.asyncio
from redis import ConnectionPool, Redis, SSLConnection, UnixDomainSocketConnection

from rq.connections import get_async_connection, parse_connection
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
        self.assertEqual(pool_kwargs, {'path': path})

    def test_get_async_connection_maps_connection_class(self):
        """SSL and unix sockets are encoded in the sync connection class, so the
        async pool must be built from the mapped class, not from Redis(**kwargs)."""
        async_connection = get_async_connection(self.connection)
        self.assertIs(async_connection.connection_pool.connection_class, redis.asyncio.Connection)
        self.assertEqual(
            async_connection.connection_pool.connection_kwargs['host'],
            self.connection.connection_pool.connection_kwargs['host'],
        )

        async_connection = get_async_connection(Redis(ssl=True))
        self.assertIs(async_connection.connection_pool.connection_class, redis.asyncio.SSLConnection)

        async_connection = get_async_connection(Redis(unix_socket_path='/tmp/rq-test.sock'))
        self.assertIs(async_connection.connection_pool.connection_class, redis.asyncio.UnixDomainSocketConnection)
        self.assertEqual(async_connection.connection_pool.connection_kwargs['path'], '/tmp/rq-test.sock')
