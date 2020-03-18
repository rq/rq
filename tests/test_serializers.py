# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import json
import sys
from rq.serializers import resolve_serializer, MySerializer
from rq.exceptions import DeserializationError

try:
    import unittest
except ImportError:
    import unittest2 as unittest  # noqa

is_py2 = sys.version[0] == '2'
if is_py2:
    import Queue as queue
else:
    import queue as queue


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
        self.assertIsInstance(serializer, MySerializer)

        self.assertTrue(hasattr(serializer, 'dumps'))
        self.assertTrue(hasattr(serializer, 'loads'))

        # Test raise NotImplmentedError
        with self.assertRaises(NotImplementedError):
            resolve_serializer(object)

        # Test raise DeserializationError
        with self.assertRaises(DeserializationError):
            serializer.dumps(queue.Queue())
