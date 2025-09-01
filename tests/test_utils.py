import datetime
import os
from unittest.mock import Mock

from redis import Redis

from rq.exceptions import TimeoutFormatError
from rq.job import Job
from rq.queue import Queue
from rq.utils import (
    as_text,
    backend_class,
    ceildiv,
    decode_redis_hash,
    ensure_job_list,
    get_call_string,
    get_version,
    import_attribute,
    import_job_class,
    import_queue_class,
    import_worker_class,
    is_nonstring_iterable,
    normalize_config_path,
    parse_timeout,
    split_list,
    str_to_date,
    truncate_long_string,
    utcparse,
    validate_absolute_path,
)
from rq.worker import SimpleWorker
from tests import RQTestCase, fixtures


class TestUtils(RQTestCase):
    def test_parse_timeout(self):
        """Ensure function parse_timeout works correctly"""
        self.assertEqual(12, parse_timeout(12))
        self.assertEqual(12, parse_timeout('12'))
        self.assertEqual(12, parse_timeout('12s'))
        self.assertEqual(720, parse_timeout('12m'))
        self.assertEqual(3600, parse_timeout('1h'))
        self.assertEqual(3600, parse_timeout('1H'))

    def test_parse_timeout_coverage_scenarios(self):
        """Test parse_timeout edge cases for coverage"""
        timeouts = ['h12', 'h', 'm', 's', '10k']

        self.assertEqual(None, parse_timeout(None))
        with self.assertRaises(TimeoutFormatError):
            for timeout in timeouts:
                parse_timeout(timeout)

    def test_is_nonstring_iterable(self):
        """Ensure function is_nonstring_iterable works correctly"""
        self.assertEqual(True, is_nonstring_iterable([]))
        self.assertEqual(False, is_nonstring_iterable('test'))
        self.assertEqual(True, is_nonstring_iterable({}))
        self.assertEqual(True, is_nonstring_iterable(()))

    def test_as_text(self):
        """Ensure function as_text works correctly"""
        bad_texts = [3, None, 'test\xd0']
        self.assertEqual('test', as_text(b'test'))
        self.assertEqual('test', as_text('test'))
        with self.assertRaises(ValueError):
            for text in bad_texts:
                as_text(text)

    def test_ensure_list(self):
        """Ensure function ensure_list works correctly"""
        self.assertEqual([], ensure_job_list([]))
        self.assertEqual(['test'], ensure_job_list('test'))
        self.assertEqual([], ensure_job_list({}))
        self.assertEqual([], ensure_job_list(()))

    def test_utcparse(self):
        """Ensure function utcparse works correctly"""
        utc_formatted_time = '2017-08-31T10:14:02.123456Z'
        expected_time = datetime.datetime(2017, 8, 31, 10, 14, 2, 123456, tzinfo=datetime.timezone.utc)
        self.assertEqual(expected_time, utcparse(utc_formatted_time))

    def test_utcparse_legacy(self):
        """Ensure function utcparse works correctly"""
        utc_formatted_time = '2017-08-31T10:14:02Z'
        expected_time = datetime.datetime(2017, 8, 31, 10, 14, 2, tzinfo=datetime.timezone.utc)
        self.assertEqual(expected_time, utcparse(utc_formatted_time))

    def test_str_to_date(self):
        """Ensure function str_to_date works correctly"""
        # Test with bytes input
        date_bytes = b'2023-01-01T12:00:00.000000Z'
        expected_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        result_bytes = str_to_date(date_bytes)
        self.assertEqual(expected_time, result_bytes)
        self.assertIsInstance(result_bytes, datetime.datetime)

        # Test with string input
        date_str = '2023-01-01T12:00:00.000000Z'
        result_str = str_to_date(date_str)
        self.assertEqual(expected_time, result_str)
        self.assertIsInstance(result_str, datetime.datetime)

        # Both should return the same result
        self.assertEqual(result_bytes, result_str)

        # Test error cases
        with self.assertRaises(ValueError):
            str_to_date('')  # empty string
        with self.assertRaises(ValueError):
            str_to_date(b'')  # empty bytes

    def test_backend_class(self):
        """Ensure function backend_class works correctly"""
        self.assertEqual(fixtures.DummyQueue, backend_class(fixtures, 'DummyQueue'))
        self.assertNotEqual(fixtures.say_pid, backend_class(fixtures, 'DummyQueue'))
        self.assertEqual(fixtures.DummyQueue, backend_class(fixtures, 'DummyQueue', override=fixtures.DummyQueue))
        self.assertEqual(
            fixtures.DummyQueue, backend_class(fixtures, 'DummyQueue', override='tests.fixtures.DummyQueue')
        )

    def test_get_redis_version(self):
        """Ensure get_version works properly"""
        redis = Redis()
        self.assertIsInstance(get_version(redis), tuple)

        # Parses 3 digit version numbers correctly
        class Redis4(Redis):
            def info(self, *args, **kwargs):
                return {'redis_version': '4.0.8'}

        self.assertEqual(get_version(Redis4()), (4, 0, 8))

        # Parses 3 digit version numbers correctly
        class Redis3(Redis):
            def info(self, *args, **kwargs):
                return {'redis_version': '3.0.7.9'}

        self.assertEqual(get_version(Redis3()), (3, 0, 7))

        # Parses 2 digit version numbers correctly (Seen in AWS ElastiCache Redis)
        class Redis7(Redis):
            def info(self, *args, **kwargs):
                return {'redis_version': '7.1'}

        self.assertEqual(get_version(Redis7()), (7, 1, 0))

        # Parses 2 digit float version numbers correctly (Seen in AWS ElastiCache Redis)
        class DummyRedis(Redis):
            def info(self, *args, **kwargs):
                return {'redis_version': 7.1}

        self.assertEqual(get_version(DummyRedis()), (7, 1, 0))

    def test_get_redis_version_gets_cached(self):
        """Ensure get_version works properly"""
        # Parses 3 digit version numbers correctly
        redis = Mock(spec=['info'])
        redis.info = Mock(return_value={'redis_version': '4.0.8'})
        self.assertEqual(get_version(redis), (4, 0, 8))
        self.assertEqual(get_version(redis), (4, 0, 8))
        redis.info.assert_called_once()

    def test_import_attribute(self):
        """Ensure get_version works properly"""
        self.assertEqual(import_attribute('rq.utils.get_version'), get_version)
        self.assertEqual(import_attribute('rq.worker.SimpleWorker'), SimpleWorker)
        self.assertRaises(ValueError, import_attribute, 'non.existent.module')
        self.assertRaises(ValueError, import_attribute, 'rq.worker.WrongWorker')

    def test_ceildiv_even(self):
        """When a number is evenly divisible by another ceildiv returns the quotient"""
        dividend = 12
        divisor = 4
        self.assertEqual(ceildiv(dividend, divisor), dividend // divisor)

    def test_ceildiv_uneven(self):
        """When a number is not evenly divisible by another ceildiv returns the quotient plus one"""
        dividend = 13
        divisor = 4
        self.assertEqual(ceildiv(dividend, divisor), dividend // divisor + 1)

    def test_split_list(self):
        """Ensure split_list works properly"""
        BIG_LIST_SIZE = 42
        SEGMENT_SIZE = 5

        big_list = ['1'] * BIG_LIST_SIZE
        small_lists = list(split_list(big_list, SEGMENT_SIZE))

        expected_small_list_count = ceildiv(BIG_LIST_SIZE, SEGMENT_SIZE)
        self.assertEqual(len(small_lists), expected_small_list_count)

    def test_truncate_long_string(self):
        """Ensure truncate_long_string works properly"""
        assert truncate_long_string('12', max_length=3) == '12'
        assert truncate_long_string('123', max_length=3) == '123'
        assert truncate_long_string('1234', max_length=3) == '123...'
        assert truncate_long_string('12345', max_length=3) == '123...'

        s = 'long string but no max_length provided so no truncating should occur' * 10
        assert truncate_long_string(s) == s

    def test_get_call_string(self):
        """Ensure a case, when func_name, args and kwargs are not None, works properly"""
        cs = get_call_string('f', ('some', 'args', 42), {'key1': 'value1', 'key2': True})
        assert cs == "f('some', 'args', 42, key1='value1', key2=True)"

    def test_get_call_string_with_max_length(self):
        """Ensure get_call_string works properly when max_length is provided"""
        func_name = 'f'
        args = (1234, 12345, 123456)
        kwargs = {'len4': 1234, 'len5': 12345, 'len6': 123456}
        cs = get_call_string(func_name, args, kwargs, max_length=5)
        assert cs == 'f(1234, 12345, 12345..., len4=1234, len5=12345, len6=12345...)'

    def test_import_job_class(self):
        """Ensure import_job_class works properly"""
        # Test importing a valid job class
        job_class = import_job_class('rq.job.Job')
        self.assertEqual(job_class, Job)

        # Test importing a non-existent module
        with self.assertRaises(ValueError):
            import_job_class('non.existent.module')

        # Test importing a non-class attribute
        with self.assertRaises(ValueError):
            import_job_class('rq.utils.get_version')

        # Test importing a class that's not a Job subclass
        with self.assertRaises(ValueError):
            import_job_class('datetime.datetime')

    def test_import_worker_class(self):
        """Ensure import_worker_class works properly"""
        # Test importing a valid worker class
        worker_class = import_worker_class('rq.worker.SimpleWorker')
        self.assertEqual(worker_class, SimpleWorker)

        # Test importing a non-existent module
        with self.assertRaises(ValueError):
            import_worker_class('non.existent.module')

        # Test importing a non-class attribute
        with self.assertRaises(ValueError):
            import_worker_class('rq.utils.get_version')

        # Test importing a class that's not a Worker subclass
        with self.assertRaises(ValueError):
            import_worker_class('datetime.datetime')

    def test_import_queue_class(self):
        """Ensure import_queue_class works properly"""
        # Test importing a valid queue class
        queue_class = import_queue_class('rq.queue.Queue')
        self.assertEqual(queue_class, Queue)

        # Test importing a non-existent module
        with self.assertRaises(ValueError):
            import_queue_class('non.existent.module')

        # Test importing a non-class attribute
        with self.assertRaises(ValueError):
            import_queue_class('rq.utils.get_version')

        # Test importing a class that's not a Queue subclass
        with self.assertRaises(ValueError):
            import_queue_class('datetime.datetime')

    def test_decode_redis_hash(self):
        """Ensure decode_redis_hash works correctly with various parameters"""
        # Test with decode_values=False
        redis_hash_1 = {b'key1': b'value1', b'key2': 'value2', 'key3': b'value3'}
        result = decode_redis_hash(redis_hash_1, decode_values=False)
        expected = {'key1': b'value1', 'key2': 'value2', 'key3': b'value3'}
        self.assertEqual(result, expected)

        # Test with decode_values=True
        redis_hash_2 = {b'key1': b'value1', b'key2': b'value2', 'key3': 'value3', 'key4': b'value4'}
        result = decode_redis_hash(redis_hash_2, decode_values=True)
        expected = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3', 'key4': 'value4'}
        self.assertEqual(result, expected)

        # Test with empty dict
        result = decode_redis_hash({})
        self.assertEqual(result, {})

        result = decode_redis_hash({}, decode_values=True)
        self.assertEqual(result, {})

    def test_decode_redis_hash_with_invalid_values(self):
        """Ensure decode_redis_hash handles invalid values correctly when decode_values=True"""
        redis_hash = {
            b'key1': b'valid_value',
            'key2': 42,  # This will cause as_text to raise ValueError
        }

        # Should work fine with decode_values=False (default)
        result = decode_redis_hash(redis_hash)
        expected = {'key1': b'valid_value', 'key2': 42}
        self.assertEqual(result, expected)

        # Should raise ValueError when decode_values=True and value is not bytes/str
        with self.assertRaises(ValueError):
            decode_redis_hash(redis_hash, decode_values=True)

    def test_normalize_config_path(self):
        """Ensure normalize_config_path works correctly for all input formats"""

        # Dotted paths should pass through unchanged
        self.assertEqual(normalize_config_path('package.subpackage.module'), 'package.subpackage.module')

        # File paths with .py extension
        self.assertEqual(normalize_config_path('package/subpackage/module.py'), 'package.subpackage.module')

        # File paths without .py extension
        self.assertEqual(normalize_config_path('package/subpackage/module'), 'package.subpackage.module')

        # Absolute paths with .py extension
        self.assertEqual(normalize_config_path('/home/project/config.py'), 'home.project.config')

        # Absolute paths without .py extension
        self.assertEqual(normalize_config_path('/home/project/config'), 'home.project.config')

        # Edge cases
        self.assertEqual(normalize_config_path('app/test.config.py'), 'app.test.config')

        # Platform-specific path separators
        if os.name == 'nt':  # Windows
            self.assertEqual(normalize_config_path('app\\cron_config.py'), 'app.cron_config')
            self.assertEqual(normalize_config_path('C:\\project\\config.py'), 'C.project.config')
            self.assertEqual(normalize_config_path('app\\module\\config.py'), 'app.module.config')
            self.assertEqual(normalize_config_path('C:\\abs\\path\\config.py'), 'C.abs.path.config')
        else:  # Unix-like
            self.assertEqual(normalize_config_path('app/cron_config.py'), 'app.cron_config')
            self.assertEqual(normalize_config_path('/project/config.py'), 'project.config')
            self.assertEqual(normalize_config_path('app/module/config.py'), 'app.module.config')
            self.assertEqual(normalize_config_path('/abs/path/config.py'), 'abs.path.config')

    def test_validate_absolute_path(self):
        """Ensure validate_absolute_path works correctly for all scenarios"""
        import os
        import tempfile

        # Test with valid existing file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_file:
            temp_file.write('# Test config file\n')
            temp_file_path = temp_file.name

        try:
            # Valid file should return the same path
            result = validate_absolute_path(temp_file_path)
            self.assertEqual(result, temp_file_path)

            # Test with non-existent file
            non_existent_path = temp_file_path + '_does_not_exist'
            with self.assertRaises(FileNotFoundError) as cm:
                validate_absolute_path(non_existent_path)
            self.assertIn('Configuration file not found', str(cm.exception))
            self.assertIn(non_existent_path, str(cm.exception))

            # Test with directory instead of file
            with tempfile.TemporaryDirectory() as temp_dir:
                with self.assertRaises(IsADirectoryError) as cm:
                    validate_absolute_path(temp_dir)
                self.assertIn('Configuration path points to a directory', str(cm.exception))
                self.assertIn(temp_dir, str(cm.exception))

        finally:
            # Clean up temp file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
