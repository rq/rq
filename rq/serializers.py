import json
import pickle
from functools import partial
from typing import Any, Callable, ClassVar, Optional, Protocol, Union, runtime_checkable

from .utils import import_attribute


@runtime_checkable
class Serializer(Protocol):
    def dumps(self, obj: Any, /) -> bytes: ...  # pragma: no cover

    def loads(self, data: bytes, /) -> Any: ...  # pragma: no cover


class DefaultSerializer:
    dumps: ClassVar[Callable[[Any], bytes]] = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
    loads: ClassVar[Callable[[bytes], Any]] = pickle.loads


class JSONSerializer:
    @staticmethod
    def dumps(*args, **kwargs):
        return json.dumps(*args, **kwargs).encode('utf-8')

    @staticmethod
    def loads(s, *args, **kwargs):
        return json.loads(s.decode('utf-8'), *args, **kwargs)


def resolve_serializer(serializer: Optional[Union[Serializer, str]] = None) -> Serializer:
    """This function checks the user defined serializer for ('dumps', 'loads') methods
    It returns a default pickle serializer if not found else it returns a MySerializer
    The returned serializer objects implement ('dumps', 'loads') methods
    Also accepts a string path to serializer that will be loaded as the serializer.

    Args:
        serializer (Callable): The serializer to resolve.

    Returns:
        serializer (Callable): An object that implements the SerializerProtocol
    """
    if not serializer:
        return DefaultSerializer

    if isinstance(serializer, str):
        serializer = import_attribute(serializer)  # type: ignore[assignment]

    assert not isinstance(serializer, str)

    if not isinstance(serializer, Serializer):
        raise NotImplementedError('Serializer should have (dumps, loads) methods.')

    return serializer
