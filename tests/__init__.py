import functools
import logging
import os
import unittest

import pytest
from redis import Redis, RedisCluster

from rq.utils import get_version

CA_CERTS_PATH = 'tests/ssl_config/cacerts.pem'


# tests might modify the environment, so make sure to cache all values and
# stay away from well-known environment variables like `REDIS_HOST` just in
# case...
@functools.cache
def get_host_and_port() -> tuple[str, int]:
    host = os.environ.get('TEST_HOST', '127.0.0.1')
    port = int(os.environ.get('TEST_PORT', 6379))

    return host, port


@functools.cache
def run_tests_with_ssl():
    return os.environ.get('TEST_WITH_SSL') in ['1', 'true']


@functools.cache
def run_cluster_tests():
    return os.environ.get('RUN_CLUSTER_TESTS') in ['1', 'true']


@functools.cache
def run_slow_tests():
    return os.environ.get('RUN_SLOW_TESTS_TOO') in ['1', 'true']


def find_empty_redis_database(can_be_non_empty=False) -> Redis | RedisCluster:
    """On a single redis instance, tries to connect to a random Redis
    database (starting from 4), and will use/connect it when no keys are
    in there. As Redis clusters do not support database selection, there
    always try to use the database shared by all clusters, but only when it
    is guaranteed to be empty.
    """
    connection_kwargs = {}
    host, port = get_host_and_port()
    if run_tests_with_ssl():
        connection_kwargs['ssl'] = True
        # use bundled CA certificates to allow certificate validation
        #
        # Redis/Valkey Clusters advertise their nodes using IP addresses, so certificates with just
        # DNS names work won't work for cluster setups. That's a bit annoying. To work around this
        # issue without having to generate certificates on the fly, each of the included certificates
        # also contain IP SANs for all IP addresses in the first /24 block of the default docker subnet.
        # The following commands can be used to generate them:
        #
        # ```
        # ips=$(echo IP:172.17.0.{1..255}, )
        # for host in redis-{1..3} redis; do
        #   openssl req -x509 -newkey rsa:4096 -keyout ${host}.key -out ${host}.pem -sha256 \
        #   -days 36500 -nodes -subj "/CN=${host}" -addext "subjectAltName = ${ips}DNS:${host}"
        # done
        # cat *.pem > cacerts.pem
        # ```

        connection_kwargs['ssl_ca_certs'] = CA_CERTS_PATH

    if run_cluster_tests():
        testconn = RedisCluster(host=host, port=port, **connection_kwargs)  # type: ignore
        if testconn.dbsize() == 0 or can_be_non_empty:
            return testconn
    else:
        for dbnum in range(4, 17):
            db_kwargs = connection_kwargs | {'db': dbnum}
            testconn = Redis(host=host, port=port, **db_kwargs)  # type: ignore
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
    slow_tests = run_slow_tests()
    return unittest.skipUnless(slow_tests, 'Slow tests disabled')(f)


def skip_with_ssl_enabled(f):
    f = pytest.mark.skip_with_ssl_enabled(f)
    use_ssl = run_tests_with_ssl()
    return unittest.skipIf(use_ssl, 'Skipped when SSL is enabled')(f)


def cluster_test(f):
    f = pytest.mark.cluster_test(f)
    use_cluster = run_cluster_tests()
    return unittest.skipUnless(use_cluster, 'Cluster tests disabled')(f)


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

        # Let tests know when we're using SSL or are connected to a cluster, so that
        # they can prepare accordingly and won't leave a mess (we like our hygiene there too)
        cls.uses_ssl = run_tests_with_ssl()
        cls.connected_to_cluster = run_cluster_tests()

        # Shut up logging
        logging.disable(logging.ERROR)

    def setUp(self):
        # Flush beforewards (we like our hygiene)
        self.connection.flushdb()

        url_prefix = 'rediss://' if self.uses_ssl else 'redis://'
        args = f'?ssl_ca_certs={CA_CERTS_PATH}' if self.uses_ssl else ''
        host, port = get_host_and_port()
        if not self.connected_to_cluster:
            db_num = self.connection.connection_pool.connection_kwargs['db']
            self.redis_url = f'{url_prefix}{host}:{port}/{db_num}{args}'
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
