from functools import partial
import pickle

from .compat import string_types
from .utils import import_attribute


class DefaultSerializer:
    dumps = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
    loads = pickle.loads


def resolve_serializer(serializer):
    """This function checks the user defined serializer for ('dumps', 'loads') methods
    It returns a default pickle serializer if not found else it returns a MySerializer
    The returned serializer objects implement ('dumps', 'loads') methods
    Also accepts a string path to serializer that will be loaded as the serializer
    """
    if not serializer:
        return DefaultSerializer

    if isinstance(serializer, string_types):
        serializer = import_attribute(serializer)

    default_serializer_methods = ('dumps', 'loads')

    for instance_method in default_serializer_methods:
        if not hasattr(serializer, instance_method):
            raise NotImplementedError('Serializer should have (dumps, loads) methods.')

    return serializer
