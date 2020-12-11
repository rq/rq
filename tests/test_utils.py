# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import re
import datetime
import mock

from distutils.version import StrictVersion

from redis import Redis

from tests import RQTestCase, fixtures
from rq.utils import backend_class, ensure_list, first, get_version, is_nonstring_iterable, parse_timeout, utcparse, \
    split_list, ceildiv
from rq.exceptions import TimeoutFormatError


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

    def test_first(self):
        """Ensure function first works correctly"""
        self.assertEqual(42, first([0, False, None, [], (), 42]))
        self.assertEqual(None, first([0, False, None, [], ()]))
        self.assertEqual('ohai', first([0, False, None, [], ()], default='ohai'))
        self.assertEqual('bc', first(re.match(regex, 'abc') for regex in ['b.*', 'a(.*)']).group(1))
        self.assertEqual(4, first([1, 1, 3, 4, 5], key=lambda x: x % 2 == 0))

    def test_is_nonstring_iterable(self):
        """Ensure function is_nonstring_iterable works correctly"""
        self.assertEqual(True, is_nonstring_iterable([]))
        self.assertEqual(False, is_nonstring_iterable('test'))
        self.assertEqual(True, is_nonstring_iterable({}))
        self.assertEqual(True, is_nonstring_iterable(()))

    def test_ensure_list(self):
        """Ensure function ensure_list works correctly"""
        self.assertEqual([], ensure_list([]))
        self.assertEqual(['test'], ensure_list('test'))
        self.assertEqual({}, ensure_list({}))
        self.assertEqual((), ensure_list(()))

    def test_utcparse(self):
        """Ensure function utcparse works correctly"""
        utc_formated_time = '2017-08-31T10:14:02.123456Z'
        self.assertEqual(datetime.datetime(2017, 8, 31, 10, 14, 2, 123456), utcparse(utc_formated_time))

    def test_utcparse_legacy(self):
        """Ensure function utcparse works correctly"""
        utc_formated_time = '2017-08-31T10:14:02Z'
        self.assertEqual(datetime.datetime(2017, 8, 31, 10, 14, 2), utcparse(utc_formated_time))

    def test_backend_class(self):
        """Ensure function backend_class works correctly"""
        self.assertEqual(fixtures.DummyQueue, backend_class(fixtures, 'DummyQueue'))
        self.assertNotEqual(fixtures.say_pid, backend_class(fixtures, 'DummyQueue'))
        self.assertEqual(fixtures.DummyQueue, backend_class(fixtures, 'DummyQueue', override=fixtures.DummyQueue))
        self.assertEqual(fixtures.DummyQueue,
                         backend_class(fixtures, 'DummyQueue', override='tests.fixtures.DummyQueue'))

    def test_get_redis_version(self):
        """Ensure get_version works properly"""
        redis = Redis()
        self.assertTrue(isinstance(get_version(redis), StrictVersion))

        # Parses 3 digit version numbers correctly
        class DummyRedis(Redis):
            def info(*args):
                return {'redis_version': '4.0.8'}
        self.assertEqual(get_version(DummyRedis()), StrictVersion('4.0.8'))

        # Parses 3 digit version numbers correctly
        class DummyRedis(Redis):
            def info(*args):
                return {'redis_version': '3.0.7.9'}
        self.assertEqual(get_version(DummyRedis()), StrictVersion('3.0.7'))

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
