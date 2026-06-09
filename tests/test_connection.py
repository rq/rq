
from redis import ConnectionPool, Redis, RedisCluster, SSLConnection, UnixDomainSocketConnection

from rq.connections import RedisConnectionBuilder
from tests import RQTestCase, cluster_test


class TestConnectionInheritance(RQTestCase):
    def test_parse_connection(self):
        """Test parsing the connection"""
        connection_builder = RedisConnectionBuilder.parse_connection(Redis(ssl=True))
        self.assertEqual(connection_builder._connection_class, Redis)
        self.assertEqual(connection_builder._connection_in_pool_class, SSLConnection)

        path = '/tmp/redis.sock'
        pool = ConnectionPool(connection_class=UnixDomainSocketConnection, path=path)
        connection_builder = RedisConnectionBuilder.parse_connection(Redis(connection_pool=pool))
        self.assertEqual(connection_builder._connection_class, Redis)
        self.assertEqual(connection_builder._connection_in_pool_class, UnixDomainSocketConnection)
        self.assertEqual(connection_builder._connection_pool_kwargs, {'path': path})

    @cluster_test
    def test_parse_cluster_connection(self):
        connection = RedisCluster(
            url=self.redis_url,
        )
        connection_builder = RedisConnectionBuilder.parse_connection(connection)
        self.assertEqual(connection_builder._connection_class, RedisCluster)
        self.assertIsNotNone(connection_builder._cluster_nodes)
        assert connection_builder._cluster_nodes is not None
        self.assertEqual(len(connection_builder._cluster_nodes), 3)

        for host, port, server_type in connection_builder._cluster_nodes:
            other_node = connection.get_node(host, port)
            self.assertIsNotNone(other_node)
            self.assertEqual(other_node.host, host)
            self.assertEqual(other_node.port, port)
            self.assertEqual(other_node.server_type, server_type)
            self.assertEqual(server_type, 'primary')
