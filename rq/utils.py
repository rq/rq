"""
Miscellaneous helper functions.

The formatter for ANSI colored console output is heavily based on Pygments
terminal colorizing code, originally by Georg Brandl.
"""

import calendar
import datetime
import importlib
import logging
import numbers
import warnings
from collections.abc import Generator, Iterable, Sequence

# TODO: Change import path to "collections.abc" after we stop supporting Python 3.8
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
    overload,
)

from redis.exceptions import ResponseError

from .exceptions import TimeoutFormatError

if TYPE_CHECKING:
    from redis import Redis

    from .job import Job
    from .queue import Queue
    from .worker import Worker


_T = TypeVar('_T')
_O = TypeVar('_O', bound=object)
ObjOrStr = Union[_O, str]


logger = logging.getLogger(__name__)


def compact(lst: Iterable[Optional[_T]]) -> list[_T]:
    """Excludes `None` values from a list-like object.

    Args:
        lst (list): A list (or list-like) object

    Returns:
        object (list): The list without None values
    """
    return [item for item in lst if item is not None]


def as_text(v: Union[bytes, str]) -> str:
    """Converts a bytes value to a string using `utf-8`.

    Args:
        v (Union[bytes, str]): The value (bytes or string)

    Raises:
        ValueError: If the value is not bytes or string

    Returns:
        value (str): The decoded string
    """
    if isinstance(v, bytes):
        return v.decode('utf-8')
    elif isinstance(v, str):
        return v
    else:
        raise ValueError('Unknown type %r' % type(v))


def decode_redis_hash(h: dict[Union[bytes, str], _T]) -> dict[str, _T]:
    """Decodes the Redis hash, ensuring that keys are strings
    Most importantly, decodes bytes strings, ensuring the dict has str keys.

    Args:
        h (Dict[Any, Any]): The Redis hash

    Returns:
        Dict[str, Any]: The decoded Redis data (Dictionary)
    """
    return dict((as_text(k), v) for k, v in h.items())


def import_attribute(name: str) -> Callable[..., Any]:
    """Returns an attribute from a dotted path name. Example: `path.to.func`.

    When the attribute we look for is a staticmethod, module name in its
    dotted path is not the last-before-end word

    E.g.: package_a.package_b.module_a.ClassA.my_static_method

    Thus we remove the bits from the end of the name until we can import it

    Args:
        name (str): The name (reference) to the path.

    Raises:
        ValueError: If no module is found or invalid attribute name.

    Returns:
        Any: An attribute (normally a Callable)
    """
    name_bits = name.split('.')
    module_name_bits, attribute_bits = name_bits[:-1], [name_bits[-1]]
    module = None
    while len(module_name_bits):
        try:
            module_name = '.'.join(module_name_bits)
            module = importlib.import_module(module_name)
            break
        except ImportError:
            attribute_bits.insert(0, module_name_bits.pop())

    if module is None:
        # maybe it's a builtin
        try:
            return __builtins__[name]  # type: ignore[index]
        except KeyError:
            raise ValueError(f'Invalid attribute name: {name}')

    attribute_name = '.'.join(attribute_bits)
    if hasattr(module, attribute_name):
        return getattr(module, attribute_name)
    # staticmethods
    attribute_name = attribute_bits.pop()
    attribute_owner_name = '.'.join(attribute_bits)
    try:
        attribute_owner = getattr(module, attribute_owner_name)
    except:  # noqa
        raise ValueError('Invalid attribute name: %s' % attribute_name)

    if not hasattr(attribute_owner, attribute_name):
        raise ValueError('Invalid attribute name: %s' % name)
    return getattr(attribute_owner, attribute_name)


def import_worker_class(name: str) -> type['Worker']:
    """Import a worker class from a dotted path name."""
    cls = import_attribute(name)

    if not isinstance(cls, type):
        raise ValueError(f'Invalid worker class: {name}')

    from .worker import Worker

    if not issubclass(cls, Worker):
        raise ValueError(f'Invalid worker class: {name}')

    return cls


def import_job_class(name: str) -> type['Job']:
    """Import a job class from a dotted path name."""
    cls = import_attribute(name)

    if not isinstance(cls, type):
        raise ValueError(f'Invalid job class: {name}')

    from .job import Job

    if not issubclass(cls, Job):
        raise ValueError(f'Invalid job class: {name}')

    return cls


def now() -> datetime.datetime:
    """Return now in UTC"""
    return datetime.datetime.now(datetime.timezone.utc)


_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


def utcformat(dt: datetime.datetime) -> str:
    return dt.strftime(as_text(_TIMESTAMP_FORMAT))


def utcparse(string: str) -> datetime.datetime:
    try:
        parsed = datetime.datetime.strptime(string, _TIMESTAMP_FORMAT)
    except ValueError:
        # This catches any jobs remain with old datetime format
        parsed = datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%SZ')
    return parsed.replace(tzinfo=datetime.timezone.utc)


def is_nonstring_iterable(obj: Any) -> bool:
    """Returns whether the obj is an iterable, but not a string

    Args:
        obj (Any): _description_

    Returns:
        bool: _description_
    """
    return isinstance(obj, Iterable) and not isinstance(obj, str)


@overload
def ensure_job_list(obj: str) -> list[str]: ...
@overload
def ensure_job_list(obj: 'Job') -> list['Job']: ...
@overload
def ensure_job_list(obj: Union['Job', str, Sequence[Union['Job', str]]]) -> list[Union['Job', str]]: ...
@overload
def ensure_job_list(obj: Iterable[_O]) -> list[_O]: ...
@overload
def ensure_job_list(obj: _O) -> list[_O]: ...

def ensure_job_list(obj):
    """When passed an iterable of objects, convert to list, otherwise, it returns
    a list with just that object in it.

    Args:
        obj (Any): _description_

    returns:
        List: _description_
    """
    # Note: To reuse is_nonstring_iterable, we need TypeGuard of Python 3.10+,
    # but we are dragged by Python 3.8.
    return list(obj) if isinstance(obj, Iterable) and not isinstance(obj, str) else [obj]


def current_timestamp() -> int:
    """Returns current UTC timestamp

    Returns:
        int: _description_
    """
    return calendar.timegm(now().utctimetuple())


def backend_class(holder, default_name, override=None) -> type:
    """Get a backend class using its default attribute name or an override

    Args:
        holder (_type_): _description_
        default_name (_type_): _description_
        override (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    if override is None:
        return getattr(holder, default_name)
    elif isinstance(override, str):
        return import_attribute(override)  # type: ignore[return-value]
    else:
        return override


def str_to_date(date_str: Optional[bytes]) -> Optional[datetime.datetime]:
    if not date_str:
        return None
    else:
        return utcparse(date_str.decode())


def parse_timeout(timeout: Optional[Union[int, float, str]]) -> Optional[int]:
    """Transfer all kinds of timeout format to an integer representing seconds"""
    if not isinstance(timeout, numbers.Integral) and timeout is not None:
        try:
            timeout = int(timeout)
            return timeout
        except ValueError:
            assert isinstance(timeout, str)
            digit, unit = timeout[:-1], (timeout[-1:]).lower()
            unit_second = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
            try:
                timeout = int(digit) * unit_second[unit]
            except (ValueError, KeyError):
                raise TimeoutFormatError(
                    'Timeout must be an integer or a string representing an integer, or '
                    'a string with format: digits + unit, unit can be "d", "h", "m", "s", '
                    'such as "1h", "23m".'
                )

    return int(timeout) if timeout is not None else None


def get_version(connection: 'Redis') -> tuple[int, int, int]:
    """
    Returns tuple of Redis server version.
    This function also correctly handles 4 digit redis server versions.

    Args:
        connection (Redis): The Redis connection.

    Returns:
        version (Tuple[int, int, int]): A tuple representing the semantic versioning format (eg. (5, 0, 9))
    """
    try:
        # Getting the connection info for each job tanks performance, we can cache it on the connection object
        if not getattr(connection, '__rq_redis_server_version', None):
            # Cast the version string to a tuple of integers. Some Redis implementations may return a float.
            version_str = str(connection.info('server')['redis_version'])
            version_parts = [int(i) for i in version_str.split('.')[:3]]
            # Ensure the version tuple has exactly three elements
            while len(version_parts) < 3:
                version_parts.append(0)
            setattr(
                connection,
                '__rq_redis_server_version',
                tuple(version_parts),
            )
        return getattr(connection, '__rq_redis_server_version')
    except ResponseError:  # fakeredis doesn't implement Redis' INFO command
        return (5, 0, 9)


def ceildiv(a, b):
    """Ceiling division. Returns the ceiling of the quotient of a division operation

    Args:
        a (_type_): _description_
        b (_type_): _description_

    Returns:
        _type_: _description_
    """
    return -(-a // b)


def split_list(a_list: Sequence[_T], segment_size: int) -> Generator[Sequence[_T], None, None]:
    """Splits a list into multiple smaller lists having size `segment_size`

    Args:
        a_list (Sequence[Any]): A sequence to split
        segment_size (int): The segment size to split into

    Yields:
        list: The splitted listed
    """
    for i in range(0, len(a_list), segment_size):
        yield a_list[i : i + segment_size]


def truncate_long_string(data: str, max_length: Optional[int] = None) -> str:
    """Truncate arguments with representation longer than max_length

    Args:
        data (str): The data to truncate
        max_length (Optional[int], optional): The max length. Defaults to None.

    Returns:
        truncated (str): The truncated string
    """
    if max_length is None:
        return data
    return (data[:max_length] + '...') if len(data) > max_length else data


def get_call_string(
    func_name: Optional[str], args: Any, kwargs: dict[Any, Any], max_length: Optional[int] = None
) -> Optional[str]:
    """
    Returns a string representation of the call, formatted as a regular
    Python function invocation statement. If max_length is not None, truncate
    arguments with representation longer than max_length.

    Args:
        func_name (str): The function name
        args (Any): The function arguments
        kwargs (Dict[Any, Any]): The function kwargs
        max_length (int, optional): The max length. Defaults to None.

    Returns:
        str: A string representation of the function call.
    """
    if func_name is None:
        return None

    arg_list = [as_text(truncate_long_string(repr(arg), max_length)) for arg in args]

    list_kwargs = [f'{k}={as_text(truncate_long_string(repr(v), max_length))}' for k, v in kwargs.items()]
    arg_list += sorted(list_kwargs)
    args = ', '.join(arg_list)

    return f'{func_name}({args})'


def parse_names(queues_or_names: Iterable[Union[str, 'Queue']]) -> list[str]:
    """Given a iterable  of strings or queues, returns queue names"""
    from .queue import Queue

    names = []
    for queue_or_name in queues_or_names:
        if isinstance(queue_or_name, Queue):
            names.append(queue_or_name.name)
        else:
            names.append(str(queue_or_name))
    return names


def get_connection_from_queues(queues_or_names: Iterable[Union[str, 'Queue']]) -> Optional['Redis']:
    """Given a list of strings or queues, returns a connection"""
    from .queue import Queue

    for queue_or_name in queues_or_names:
        if isinstance(queue_or_name, Queue):
            return queue_or_name.connection
    return None


def parse_composite_key(composite_key: str) -> tuple[str, str]:
    """Method returns a parsed composite key.

    Args:
        composite_key (str): the composite key to parse

    Returns:
        tuple[str, str]: tuple of job id and the execution id
    """
    result = composite_key.split(':')
    if len(result) == 1:
        # StartedJobRegistry contains a composite key under the sorted set
        # a single job_id should've never ended up in the set, but
        # just in case there's a regression (tests don't show any)
        warnings.warn(
            f'Composite key must contain job_id:execution_id, got {composite_key}',
            DeprecationWarning,
        )
        return (result[0], '')
    job_id, execution_id = result
    return (job_id, execution_id)
