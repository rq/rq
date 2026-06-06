import logging
import os
import unittest

import pytest
from redis import Redis, RedisCluster

from rq.utils import get_version, is_cluster


def find_empty_redis_database(ssl=False, can_be_non_empty=False) -> Redis | RedisCluster:
    """On a single redis instance, tries to connect to a random Redis
    database (starting from 4), and will use/connect it when no keys are
    in there. As Redis clusters do not support database selection, there
    always try to use the database shared by all clusters, but only when it
    is guaranteed to be empty.
    """
    connection_kwargs = {}
    if ssl:
        connection_kwargs['port'] = 9736
        connection_kwargs['ssl'] = True
        # disable certificate validation
        connection_kwargs['ssl_cert_reqs'] = None  # type: ignore

    cluster_host = os.environ.get('REDIS_CLUSTER_HOST', None)
    cluster_port = os.environ.get('REDIS_CLUSTER_PORT', 6379)
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


def ssl_test(f):
    f = pytest.mark.ssl_test(f)
    return unittest.skipUnless(os.environ.get('RUN_SSL_TESTS'), 'SSL tests disabled')(f)


def cluster_test(f):
    f = pytest.mark.cluster_test(f)
    unittest.skipUnless(os.environ.get('REDIS_CLUSTER_HOST'), 'No Redis cluster host given')(f)


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

        # Let tests know when we're connected to a cluster, so that they can
        # prepare accordingly and won't leave a mess (we like our hygiene there too)
        cls.connected_to_cluster = is_cluster(cls.connection)

        # Shut up logging
        logging.disable(logging.ERROR)

    def setUp(self):
        # Flush beforewards (we like our hygiene)
        self.connection.flushdb()

    def tearDown(self):
        # Flush afterwards
        self.connection.flushdb()

    @classmethod
    def tearDownClass(cls):
        logging.disable(logging.NOTSET)
        cls.connection.close()
