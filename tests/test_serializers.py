# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import json
import queue as queue
import unittest

from rq.serializers import resolve_serializer


class TestSerializers(unittest.TestCase):
    def test_resolve_serializer(self):
        """Ensure function resolve_serializer works correctly"""
        serializer = resolve_serializer(None)
        self.assertIsNotNone(serializer)

        self.assertTrue(hasattr(serializer, 'dumps'))
        self.assertTrue(hasattr(serializer, 'loads'))

        # Test using json serializer
        serializer = resolve_serializer(json)
        self.assertIsNotNone(serializer)

        self.assertTrue(hasattr(serializer, 'dumps'))
        self.assertTrue(hasattr(serializer, 'loads'))

        # Test raise NotImplmentedError
        with self.assertRaises(NotImplementedError):
            resolve_serializer(object)

        # Test raise Exception
        with self.assertRaises(Exception):
            resolve_serializer(queue.Queue())

        # Test using path.to.serializer string
        serializer = resolve_serializer('tests.fixtures.Serializer')
        self.assertIsNotNone(serializer)
