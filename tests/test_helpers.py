from rq.cli.helpers import get_redis_from_config

from tests import RQTestCase


class TestHelpers(RQTestCase):

    def test_get_redis_from_config(self):
        """Ensure Redis connection params are properly parsed"""
        settings = {
            'REDIS_URL': 'redis://localhost:1/1'
        }

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
            'REDIS_PASSWORD': 'bar'
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
