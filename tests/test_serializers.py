import json
import pickle
import pickletools
import queue
import unittest

from rq.serializers import DefaultSerializer, JSONSerializer, PickleSerializer, resolve_serializer


class TestSerializers(unittest.TestCase):
    def test_resolve_serializer(self):
        """Ensure function resolve_serializer works correctly"""
        serializer = resolve_serializer(None)
        self.assertIsNotNone(serializer)
        self.assertEqual(serializer, PickleSerializer)
        self.assertIs(DefaultSerializer, PickleSerializer)

        # Test round trip with pickle serializer
        test_data = {'test': 'data'}
        serialized_data = serializer.dumps(test_data)
        self.assertEqual(serializer.loads(serialized_data), test_data)
        self.assertEqual(next(pickletools.genops(serialized_data))[1], pickle.HIGHEST_PROTOCOL)

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

        # Shorthand aliases
        self.assertIs(resolve_serializer('json'), JSONSerializer)
        self.assertIs(resolve_serializer('pickle'), PickleSerializer)
        self.assertIs(resolve_serializer('rq.serializers.PickleSerializer'), PickleSerializer)
        self.assertIs(resolve_serializer('rq.serializers.DefaultSerializer'), PickleSerializer)
