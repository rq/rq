import zlib
from base64 import b64decode, b64encode
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from redis import Redis

from .defaults import UNSERIALIZABLE_RETURN_VALUE_PAYLOAD
from .job import Job
from .serializers import resolve_serializer
from .utils import decode_redis_hash, now


def get_key(job_id):
    return 'rq:results:%s' % job_id


class Result:
    class Type(Enum):
        SUCCESSFUL = 1
        FAILED = 2
        STOPPED = 3
        RETRIED = 4

    def __init__(
        self,
        job_id: str,
        type: Type,
        connection: Redis,
        id: Optional[str] = None,
        created_at: Optional[datetime] = None,
        return_value: Optional[Any] = None,
        exc_string: Optional[str] = None,
        serializer=None,
    ):
        self.return_value = return_value
        self.exc_string = exc_string
        self.type = type
        self.created_at = created_at if created_at else now()
        self.serializer = resolve_serializer(serializer)
        self.connection = connection
        self.job_id = job_id
        self.id = id

    def __repr__(self):
        return f'Result(id={self.id}, type={self.Type(self.type).name})'

    def __eq__(self, other):
        try:
            return self.id == other.id
        except AttributeError:
            return False

    def __bool__(self):
        return bool(self.id)

    @classmethod
    def create(cls, job, type, ttl, return_value=None, exc_string=None, pipeline=None) -> 'Result':
        result = cls(
            job_id=job.id,
            type=type,
            connection=job.connection,
            return_value=return_value,
            exc_string=exc_string,
            serializer=job.serializer,
        )
        result.save(ttl=ttl, pipeline=pipeline)
        return result

    @classmethod
    def create_failure(cls, job, ttl, exc_string, pipeline=None) -> 'Result':
        result = cls(
            job_id=job.id,
            type=cls.Type.FAILED,
            connection=job.connection,
            exc_string=exc_string,
            serializer=job.serializer,
        )
        result.save(ttl=ttl, pipeline=pipeline)
        return result

    @classmethod
    def create_retried(cls, job, ttl, pipeline=None) -> 'Result':
        result = cls(
            job_id=job.id,
            type=cls.Type.RETRIED,
            connection=job.connection,
            serializer=job.serializer,
        )
        result.save(ttl=ttl, pipeline=pipeline)
        return result

    @classmethod
    def all(cls, job: Job, serializer=None):
        """Returns all results for job"""
        # response = job.connection.zrange(cls.get_key(job.id), 0, 10, desc=True, withscores=True)
        response = job.connection.xrevrange(cls.get_key(job.id), '+', '-')
        results = []
        for result_id, payload in response:
            results.append(
                cls.restore(job.id, result_id.decode(), payload, connection=job.connection, serializer=serializer)
            )

        return results

    @classmethod
    def count(cls, job: Job) -> int:
        """Returns the number of job results"""
        return job.connection.xlen(cls.get_key(job.id))

    @classmethod
    def delete_all(cls, job: Job) -> None:
        """Delete all job results"""
        job.connection.delete(cls.get_key(job.id))

    @classmethod
    def restore(cls, job_id: str, result_id: str, payload: dict, connection: Redis, serializer=None) -> 'Result':
        """Create a Result object from given Redis payload"""
        created_at = datetime.fromtimestamp(int(result_id.split('-')[0]) / 1000, tz=timezone.utc)
        payload = decode_redis_hash(payload)
        # data, timestamp = payload
        # result_data = json.loads(data)
        # created_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        serializer = resolve_serializer(serializer)
        return_value = payload.get('return_value')
        if return_value is not None:
            return_value = serializer.loads(b64decode(return_value.decode()))

        exc_string = payload.get('exc_string')
        if exc_string:
            exc_string = zlib.decompress(b64decode(exc_string)).decode()

        return Result(
            job_id,
            Result.Type(int(payload['type'])),
            connection=connection,
            id=result_id,
            created_at=created_at,
            return_value=return_value,
            exc_string=exc_string,
        )

    @classmethod
    def fetch(cls, job: Job, serializer=None) -> Optional['Result']:
        """Fetch a result that matches a given job ID. The current sorted set
        based implementation does not allow us to fetch a given key by ID
        so we need to iterate through results, deserialize the payload and
        look for a matching ID.

        Future Redis streams based implementation may make this more efficient
        and scalable.
        """
        return None

    @classmethod
    def fetch_latest(cls, job: Job, serializer=None, timeout: int = 0) -> Optional['Result']:
        """Returns the latest result for given job instance or ID.

        If a non-zero timeout is provided, block for a result until timeout is reached.
        """
        if timeout:
            # Unlike blpop, xread timeout is in miliseconds. "0-0" is the special value for the
            # first item in the stream, like '-' for xrevrange.
            timeout_ms = timeout * 1000
            response = job.connection.xread({cls.get_key(job.id): '0-0'}, block=timeout_ms)
            if not response:
                return None
            response = response[0]  # Querying single stream only.
            response = response[1]  # Xread also returns Result.id, which we don't need.
            result_id, payload = response[-1]  # Take most recent result.

        else:
            # If not blocking, use xrevrange to load a single result (as xread will load them all).
            response = job.connection.xrevrange(cls.get_key(job.id), '+', '-', count=1)
            if not response:
                return None
            result_id, payload = response[0]

        res = cls.restore(job.id, result_id.decode(), payload, connection=job.connection, serializer=serializer)
        return res

    @classmethod
    def get_key(cls, job_id):
        return 'rq:results:%s' % job_id

    def save(self, ttl, pipeline=None):
        """Save result data to Redis"""
        key = self.get_key(self.job_id)

        connection = pipeline if pipeline is not None else self.connection
        # result = connection.zadd(key, {self.serialize(): self.created_at.timestamp()})
        result = connection.xadd(key, self.serialize(), maxlen=10)
        # If xadd() is called in a pipeline, it returns a pipeline object instead of stream ID
        if pipeline is None:
            self.id = result.decode()
        if ttl is not None:
            if ttl == -1:
                connection.persist(key)
            else:
                connection.expire(key, ttl)
        return self.id

    def serialize(self) -> dict[str, Any]:
        data: dict[str, Any] = {'type': self.type.value}

        if self.exc_string is not None:
            data['exc_string'] = b64encode(zlib.compress(self.exc_string.encode())).decode()

        try:
            serialized = self.serializer.dumps(self.return_value)
        except:  # noqa
            serialized = self.serializer.dumps(UNSERIALIZABLE_RETURN_VALUE_PAYLOAD)

        if self.return_value is not None:
            data['return_value'] = b64encode(serialized).decode()

        # return json.dumps(data)
        return data
