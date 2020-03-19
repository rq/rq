import pickle

from .exceptions import DeserializationError


class MySerializer(object):
    """MySerializer Class that uses the user specified serializer module."""
    def __init__(self, serializer):
        self.serializer = serializer

    def dumps(self, data):
        try:
            serialized_data = self.serializer.dumps(data)
        except Exception as e:
            raise DeserializationError("Unserializable return value", data, e)

        return serialized_data

    def loads(self, serialized_string):
        try:
            obj = self.serializer.loads(serialized_string)
        except Exception as e:
            raise DeserializationError('Could not deserialize', serialized_string, e)

        return obj


def resolve_serializer(serializer):
    """This function checks the user defined serializer for ('dumps', 'loads') methods
    It returns a default pickle serializer if not found else it returns a MySerializer
    The returned serializer objects implement ('dumps', 'loads') methods
    """
    if not serializer:
        return pickle

    default_serializer_methods = ('dumps', 'loads')

    for instance_method in default_serializer_methods:
        if not hasattr(serializer, instance_method):
            raise NotImplementedError('Serializer should have (dumps, loads) methods.')

    return MySerializer(serializer)
