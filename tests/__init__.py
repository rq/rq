import logging
import os
import unittest

import pytest
from redis import Redis, RedisCluster

from rq.utils import get_version, is_cluster


def get_cluster_host_and_port() -> tuple[str | None, int]:
    cluster_host = (os.environ.get('REDIS_CLUSTER_HOST', None)
        or os.environ.get('VALKEY_CLUSTER_HOST', None))
    cluster_port = int(os.environ.get('REDIS_CLUSTER_PORT', None)
        or os.environ.get('VALKEY_CLUSTER_PORT', 6379))

    return cluster_host, cluster_port


def find_empty_redis_database(ssl=False, can_be_non_empty=False) -> Redis | RedisCluster:
    """On a single redis instance, tries to connect to a random Redis
    database (starting from 4), and will use/connect it when no keys are
    in there. As Redis clusters do not support database selection, there
    always try to use the database shared by all clusters, but only when it
    is guaranteed to be empty.
    """
    connection_kwargs = {}
    cluster_host, cluster_port = get_cluster_host_and_port()
    if ssl or os.environ.get('ALWAYS_USE_SSL', None) is not None:
        if cluster_host is None:
            connection_kwargs['port'] = 9736
        connection_kwargs['ssl'] = True
        # disable certificate validation
        connection_kwargs['ssl_cert_reqs'] = 'none' # use str here, so that we always have the same type in the kwargs

    if cluster_host is not None:
        testconn = RedisCluster(host=cluster_host, port=cluster_port, **connection_kwargs)  # type: ignore
        if testconn.dbsize() == 0 or can_be_non_empty:
            return testconn
    else:
        for dbnum in range(4, 17):
            db_kwargs = connection_kwargs | {'db': dbnum}
            testconn = Redis(**db_kwargs)  # type: ignore
            empty = testconn.dbsize() == 0
            if empty or can_be_non_empty:
                return testconn
            testconn.close()
    assert False, 'No empty Redis database found to run tests in.'


def min_redis_version(ver: tuple[int, ...]):
    ver_str = '.'.join(map(str, ver))
    with find_empty_redis_database(can_be_non_empty=True) as conn:
        redis_version = get_version(conn)

    return unittest.skipIf(redis_version < ver, f'Skip if Redis server < {ver_str}')


def slow(f):
    f = pytest.mark.slow(f)
    return unittest.skipUnless(os.environ.get('RUN_SLOW_TESTS_TOO'), 'Slow tests disabled')(f)


def skip_with_ssl_enabled(f):
    f = pytest.mark.skip_with_ssl_enabled(f)
    use_ssl = os.environ.get('RUN_SSL_TESTS') or os.environ.get('ALWAYS_USE_SSL')
    return unittest.skipIf(use_ssl, 'SSL tests disabled')(f)


def ssl_test(f):
    f = pytest.mark.ssl_test(f)
    return unittest.skipUnless(os.environ.get('RUN_SSL_TESTS'), 'SSL tests disabled')(f)


def cluster_test(f):
    f = pytest.mark.cluster_test(f)
    return unittest.skipUnless(get_cluster_host_and_port()[0] is not None, 'No Redis/Valkey cluster host given')(f)


class RQTestCase(unittest.TestCase):
    """Base class to inherit test cases from for RQ.

    It sets up the Redis connection (available via self.connection), turns off
    logging to the terminal and flushes the Redis database before and after
    running each test.
    """

    connection: Redis | RedisCluster

    @classmethod
    def setUpClass(cls):
        # Set up connection to Redis
        cls.connection = find_empty_redis_database()
        cls.uses_ssl = cls.connection.get_connection_kwargs().get('ssl', False)

        # Let tests know when we're connected to a cluster, so that they can
        # prepare accordingly and won't leave a mess (we like our hygiene there too)
        cls.connected_to_cluster = is_cluster(cls.connection)
        # Shut up logging
        #logging.disable(logging.ERROR)

    def setUp(self):
        # Flush beforewards (we like our hygiene)
        self.connection.flushdb()

        url_prefix = 'rediss://' if self.uses_ssl else 'redis://'
        args = '?ssl_cert_reqs=none' if self.uses_ssl else ''
        port = self.connection.get_connection_kwargs().get('port', 6379)
        if not self.connected_to_cluster:
            db_num = self.connection.connection_pool.connection_kwargs['db']
            self.redis_url = f'{url_prefix}127.0.0.1:{port}/{db_num}{args}'
            self.connection: Redis | RedisCluster = Redis.from_url(self.redis_url)
            self.runner_args = []
        else:
            default_node = self.connection.get_default_node()
            self.redis_url = f'{url_prefix}{default_node.host}:{default_node.port}{args}'
            self.connection: Redis | RedisCluster = RedisCluster.from_url(self.redis_url)
            self.runner_args = ["--connection-class", "redis.RedisCluster"]

    def tearDown(self):
        # Flush afterwards
        self.connection.flushdb()

    @classmethod
    def tearDownClass(cls):
        logging.disable(logging.NOTSET)
        cls.connection.close()
