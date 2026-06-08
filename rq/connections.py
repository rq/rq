from collections.abc import Iterable
from typing import cast

import redis.cluster
from redis import Connection as RedisConnection
from redis import ConnectionPool, Redis, RedisCluster
from redis.cluster import KWARGS_DISABLED_KEYS, ClusterNode, NodesManager

from rq.utils import import_attribute, is_cluster

REALLY_ALL_REDIS_ALLOWED_KEYS = list(redis.cluster.REDIS_ALLOWED_KEYS) + ['health_check_interval', 'retry_on_error']

RQ_KEY_PREFIX = '{rq}'


class NoRedisConnectionException(Exception):
    pass


class RedisMismatchedConnectionClassException(Exception):
    pass


# redis-py >= 8 may add RESP3 maintenance-notification handlers and derived
# connection metadata to connection_kwargs. Those values describe the current
# pool's internal state, and some contain locks, so RQ drops them before
# rebuilding a Redis connection in another process.
REDIS_RUNTIME_CONNECTION_KWARGS = (
    'maint_notifications_config',
    'maint_notifications_pool_handler',
    'event_dispatcher',
    'orig_host_address',
    'orig_socket_timeout',
    'orig_socket_connect_timeout',
    # the following two attributes are present in a `RedisCluster`'s
    # `Connection` metadata
    'redis_connect_func',
    'oss_cluster_maint_notifications_handler',
)


def _get_kwargs(kwargs: dict) -> dict:
    for key in REDIS_RUNTIME_CONNECTION_KWARGS:
        kwargs.pop(key, None)
    # redis-py marks unset kwargs (e.g. socket_keepalive_options) with a sentinel object() that it
    # recognizes by identity. Pickling/repr across the process boundary creates a new object(), so
    # the child treats it as a real value and breaks; drop it so the child re-applies its default.
    return {key: value for key, value in kwargs.items() if type(value) is not object}


def get_connection_class_from_pool(connection: Redis | RedisCluster | None) -> type[RedisConnection]:
    if connection is None:
        raise NoRedisConnectionException()

    if not is_cluster(connection):
        connection = cast(Redis, connection)
        return connection.connection_pool.connection_class

    connection = cast(RedisCluster, connection)
    default_node = connection.get_default_node()
    if default_node is None:
        raise NoRedisConnectionException()

    # well, this is rather mysterious - each node has an own connection class, but
    # `RedisCluster` does not care for the connection class of its startup nodes...
    # therefore, we make sure that we at least notice when there should be multiple conection
    # classes floating around.
    connection_in_pool_classes = list(map(lambda node:
        get_connection_class_from_pool(node.redis_connection), connection.get_nodes()
    ))
    if (len(connection_in_pool_classes) == 0
            or connection_in_pool_classes.count(connection_in_pool_classes[0]) != len(connection_in_pool_classes)):
        raise RedisMismatchedConnectionClassException()

    return get_connection_class_from_pool(default_node.redis_connection)


def get_connection_kwargs(connection: Redis | RedisCluster | None) -> dict:
    """Return pool kwargs suitable for rebuilding this Redis connection in a child process."""
    if connection is None:
        raise NoRedisConnectionException()

    if is_cluster(connection):
        connection = cast(RedisCluster, connection)
        kwargs = connection.nodes_manager.connection_kwargs.copy()
        # unfortunately, the `connection_class` does end up here due to the awkward way we
        # have to smuggle it in the `ConnectionPool`. however, we do not need it in the kwargs
        # anyway, and it does confuse e.g. `Registry.__eq__`, so let's just drop it if it exists.
        # the same goes for the `connection_pool`
        for undesired_arg in ['connection_class', 'connection_pool']:
            if undesired_arg in kwargs:
                del kwargs[undesired_arg]
    else:
        connection = cast(Redis, connection)
        kwargs = connection.connection_pool.connection_kwargs.copy()

    return _get_kwargs(kwargs)


def copy_cluster_node_without_connection(node: ClusterNode) -> ClusterNode:
    # create a dummy cluster without the redis connection, so that it is essentially just
    # a handy struct that we can use now to make typing and our life easier
    return ClusterNode(host=node.host, port=node.port, server_type=node.server_type)


class RedisConnectionBuilder:

    def __init__(self, connection_class: type[Redis | RedisCluster] | str,
            connection_in_pool_class: type[RedisConnection] | str,
            connection_pool_kwargs: dict,
            cluster_nodes: list[tuple[str, int, str | None]] | None):
        if isinstance(connection_class, str):
            self._connection_class = cast(type[Redis | RedisCluster], import_attribute(connection_class))
        else:
            self._connection_class = connection_class
        if isinstance(connection_in_pool_class, str):
            self._connection_in_pool_class = cast(type[RedisConnection], import_attribute(connection_in_pool_class))
        else:
            self._connection_in_pool_class = connection_in_pool_class
        self._connection_pool_kwargs = connection_pool_kwargs
        self._cluster_nodes = cluster_nodes

    @classmethod
    def _fully_qualified_name(_, redis_class) -> str:
        return f'{redis_class.__module__}.{redis_class.__qualname__}'

    def __repr__(self):
        cluster_node_list = 'None'
        if self._cluster_nodes is not None:
            cluster_nodes = []
            for host, port, server_type in self._cluster_nodes:
                node_args = [f'"{host}"', f'"{port}"', f'"{server_type}"']
                cluster_nodes.append(f'({",".join(node_args)}), ')
            cluster_node_list = '[' + "\n".join(cluster_nodes) + ']'

        return (f'RedisConnectionBuilder(connection_class=\'{self._fully_qualified_name(self._connection_class)}\', '
            f'connection_in_pool_class=\'{self._fully_qualified_name(self._connection_in_pool_class)}\', '
            f'connection_pool_kwargs={self._connection_pool_kwargs!r}, '
            f'cluster_nodes={cluster_node_list})'
        )

    def filter_kwargs(self, keys: Iterable[str]):
        for key in keys:
            if key in self._connection_pool_kwargs:
                del self._connection_pool_kwargs[key]

    @staticmethod
    def parse_connection(connection: Redis | RedisCluster) -> 'RedisConnectionBuilder':
        connection_pool_kwargs = get_connection_kwargs(connection)

        if not is_cluster(connection):
            connection_in_pool_class = get_connection_class_from_pool(cast(Redis, connection))
            return RedisConnectionBuilder(
                connection.__class__, connection_in_pool_class, connection_pool_kwargs, None
            )

        connection = cast(RedisCluster, connection)
        default_node = connection.get_default_node()
        if default_node is None:
            raise NoRedisConnectionException()

        connection_pool_kwargs = get_connection_kwargs(connection)
        connection_in_pool_class = get_connection_class_from_pool(connection)

        # unfortunately, the interaction between `RedisCluster`, its `NodesManager` and `Redis` is rather awkward.
        # the kwargs to `RedisCluster` are sanitized before they are handed over to `NodesManager`, so
        # `NodesManager.connection_kwargs` will not contain any custom arguments beyond what `RedisCluster` accepts.
        # therefore, we have to include all non-Redis arguments from the node's connection themselves in order to
        # preserve # the custom kwargs -> this is basically an adaption of `redis.cluster.cleanup_kwargs()`
        connection_pool_kwargs |= {
            k: v
            for k, v in get_connection_kwargs(default_node.redis_connection).items()
            if k not in REALLY_ALL_REDIS_ALLOWED_KEYS and k not in KWARGS_DISABLED_KEYS
        }

        cluster_nodes = list(map(
            lambda node: (node.host, node.port, node.server_type),
            connection.get_nodes()
        ))

        return RedisConnectionBuilder(
            connection.__class__, connection_in_pool_class, connection_pool_kwargs, cluster_nodes
        )

    def build_connection(self) -> Redis | RedisCluster:
        if self._cluster_nodes is None:
            return self._connection_class(
                connection_pool=ConnectionPool(
                    connection_class=self._connection_in_pool_class, **self._connection_pool_kwargs
                )
            )

        cluster_nodes = list(map(
            lambda node: ClusterNode(node[0], node[1], node[2]),
            self._cluster_nodes
        ))

        # here it gets even more hack-ish, since as explained above we need to work around `RedisCluster.init`
        # to get custom kwargs directly to `NodesManager`. In addition, we also suffer from the problem that
        # `NodesManager.create_redis_node` will only pass create a connection pool and pass along the kwargs there
        # if it thinks that `RedisCluster` has been initialized with an explicit URL. otherwise, it will just hand
        # over the arguments to `Redis` directly and that won't well...

        # another option to approach this problem would be to the `ConnectionPool` ourselves and hand it to
        # `RedisCluster` directly, but I am not sure if this is actually desirable, since all node connections
        # then would inherit the same connection pool and that sounds intuitively like a bad-ish idea.
        connection_class = cast(type[RedisCluster], self._connection_class)
        cluster_connection = connection_class(startup_nodes=cluster_nodes)

        cluster_connection.nodes_manager.close()
        connection_pool_kwargs = self._connection_pool_kwargs | {'connection_class': self._connection_in_pool_class}

        cluster_connection.nodes_manager = NodesManager(
            from_url=True,
            startup_nodes=cluster_nodes,
            **connection_pool_kwargs
        )
        return cluster_connection

