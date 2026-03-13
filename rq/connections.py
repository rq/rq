from redis import Connection as RedisConnection
from redis import Redis


class NoRedisConnectionException(Exception):
    pass


def parse_connection(connection: Redis) -> tuple[type[Redis], type[RedisConnection], dict]:
    connection_pool = connection.connection_pool
    
    # SentinelConnectionPool has different attributes than regular ConnectionPool
    # For sentinel connections, we need to extract the underlying connection class
    # and use default pool kwargs since sentinel-specific ones won't work with ConnectionPool
    if connection_pool.__class__.__name__ == 'SentinelConnectionPool':
        # For sentinel connections, get the connection class from the pool
        # and use minimal kwargs suitable for regular ConnectionPool
        connection_pool_class = connection_pool.connection_class
        # Filter out sentinel-specific kwargs that aren't valid for ConnectionPool
        sentinel_kwargs = {'master_name', 'sentinel', 'min_other_servers'}
        connection_pool_kwargs = {
            k: v for k, v in connection_pool.connection_kwargs.items()
            if k not in sentinel_kwargs
        }
    else:
        connection_pool_kwargs = connection_pool.connection_kwargs.copy()
        connection_pool_class = connection_pool.connection_class

    return connection.__class__, connection_pool_class, connection_pool_kwargs
