from unittest.mock import MagicMock, patch

from redis import ConnectionPool, Redis, SSLConnection, UnixDomainSocketConnection
from redis.sentinel import SentinelConnectionPool

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
        self.assertEqual(pool_kwargs, {'path': path})

    def test_parse_connection_sentinel(self):
        """Test parsing sentinel connection - should filter out sentinel-specific kwargs"""
        # Create a mock sentinel connection pool
        mock_pool = MagicMock(spec=SentinelConnectionPool)
        mock_pool.connection_class = Redis
        mock_pool.connection_kwargs = {
            'host': 'localhost',
            'port': 6379,
            'master_name': 'mymaster',
            'sentinel': MagicMock(),
            'min_other_servers': 0,
            'db': 0,
        }

        # Create a mock connection using the sentinel pool
        mock_conn = MagicMock(spec=Redis)
        mock_conn.connection_pool = mock_pool
        mock_conn.__class__ = Redis

        conn_class, pool_class, pool_kwargs = parse_connection(mock_conn)

        self.assertEqual(conn_class, Redis)
        # Should have filtered out sentinel-specific kwargs
        self.assertNotIn('master_name', pool_kwargs)
        self.assertNotIn('sentinel', pool_kwargs)
        self.assertNotIn('min_other_servers', pool_kwargs)
        # Should still have valid kwargs
        self.assertIn('host', pool_kwargs)
        self.assertIn('port', pool_kwargs)
        self.assertIn('db', pool_kwargs)
