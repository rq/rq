from typing import Tuple, Type

from redis import Connection as RedisConnection
from redis import Redis


class NoRedisConnectionException(Exception):
    pass


def parse_connection(connection: Redis) -> Tuple[Type[Redis], Type[RedisConnection], dict]:
    connection_pool_kwargs = connection.connection_pool.connection_kwargs.copy()
    connection_pool_class = connection.connection_pool.connection_class

    return connection.__class__, connection_pool_class, connection_pool_kwargs
