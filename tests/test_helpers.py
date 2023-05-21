from unittest import mock

from rq.cli.helpers import get_redis_from_config
from tests import RQTestCase


class TestHelpers(RQTestCase):
    @mock.patch('rq.cli.helpers.Sentinel')
    def test_get_redis_from_config(self, sentinel_class_mock):
        """Ensure Redis connection params are properly parsed"""
        settings = {'REDIS_URL': 'redis://localhost:1/1'}

        # Ensure REDIS_URL is read
        redis = get_redis_from_config(settings)
        connection_kwargs = redis.connection_pool.connection_kwargs
        self.assertEqual(connection_kwargs['db'], 1)
        self.assertEqual(connection_kwargs['port'], 1)

        settings = {
            'REDIS_URL': 'redis://localhost:1/1',
            'REDIS_HOST': 'foo',
            'REDIS_DB': 2,
            'REDIS_PORT': 2,
            'REDIS_PASSWORD': 'bar',
        }

        # Ensure REDIS_URL is preferred
        redis = get_redis_from_config(settings)
        connection_kwargs = redis.connection_pool.connection_kwargs
        self.assertEqual(connection_kwargs['db'], 1)
        self.assertEqual(connection_kwargs['port'], 1)

        # Ensure fall back to regular connection parameters
        settings['REDIS_URL'] = None
        redis = get_redis_from_config(settings)
        connection_kwargs = redis.connection_pool.connection_kwargs
        self.assertEqual(connection_kwargs['host'], 'foo')
        self.assertEqual(connection_kwargs['db'], 2)
        self.assertEqual(connection_kwargs['port'], 2)
        self.assertEqual(connection_kwargs['password'], 'bar')

        # Add Sentinel to the settings
        settings.update(
            {
                'SENTINEL': {
                    'INSTANCES': [
                        ('remote.host1.org', 26379),
                        ('remote.host2.org', 26379),
                        ('remote.host3.org', 26379),
                    ],
                    'MASTER_NAME': 'master',
                    'DB': 2,
                    'USERNAME': 'redis-user',
                    'PASSWORD': 'redis-secret',
                    'SOCKET_TIMEOUT': None,
                    'CONNECTION_KWARGS': {
                        'ssl_ca_path': None,
                    },
                    'SENTINEL_KWARGS': {
                        'username': 'sentinel-user',
                        'password': 'sentinel-secret',
                    },
                },
            }
        )

        # Ensure SENTINEL is preferred against REDIS_* parameters
        redis = get_redis_from_config(settings)
        sentinel_init_sentinels_args = sentinel_class_mock.call_args[0]
        sentinel_init_sentinel_kwargs = sentinel_class_mock.call_args[1]
        self.assertEqual(
            sentinel_init_sentinels_args,
            ([('remote.host1.org', 26379), ('remote.host2.org', 26379), ('remote.host3.org', 26379)],),
        )
        self.assertDictEqual(
            sentinel_init_sentinel_kwargs,
            {
                'db': 2,
                'ssl': False,
                'username': 'redis-user',
                'password': 'redis-secret',
                'socket_timeout': None,
                'ssl_ca_path': None,
                'sentinel_kwargs': {
                    'username': 'sentinel-user',
                    'password': 'sentinel-secret',
                },
            },
        )
