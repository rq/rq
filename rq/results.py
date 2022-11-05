import json
from typing import Any, Optional
import zlib

from base64 import b64decode, b64encode
from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from redis import Redis
from redis.client import Pipeline

from .job import Job
from .serializers import resolve_serializer
from .utils import now


def get_key(job_id):
    return 'rq:results:%s' % job_id


class Result(object):

    class Type(Enum):
        SUCCESSFUL = 1
        FAILED = 2
        STOPPED = 3

    def __init__(self, job_id: str, type: Type, connection: Redis, id: Optional[str] = None,
                 created_at: Optional[datetime] = None, return_value: Optional[Any] = None,
                 exc_string: Optional[str] = None, serializer=None):
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
    def create(cls, job, type, ttl, return_value=None, exc_string=None, pipeline=None):
        result = cls(job_id=job.id, type=type, connection=job.connection,
                     id=uuid4().hex, return_value=return_value,
                     exc_string=exc_string, serializer=job.serializer)
        result.save(ttl=ttl, pipeline=pipeline)
        cls.trim(job, pipeline=pipeline)
        return result

    @classmethod
    def create_failure(cls, job, ttl, exc_string, pipeline=None):
        result = cls(job_id=job.id, type=cls.Type.FAILED, connection=job.connection,
                     id=uuid4().hex, exc_string=exc_string, serializer=job.serializer)
        result.save(ttl=ttl, pipeline=pipeline)
        cls.trim(job, pipeline=pipeline)
        return result

    @classmethod
    def all(cls, job: Job, serializer=None):
        """Returns all results for job"""
        response = job.connection.zrange(cls.get_key(job.id), 0, 10, desc=True, withscores=True)
        results = []
        for payload in response:
            results.append(cls.restore(job.id, payload, connection=job.connection, serializer=serializer))

        return results

    @classmethod
    def count(cls, job: Job) -> int:
        """Returns the number of job results"""
        return job.connection.zcard(cls.get_key(job.id))

    @classmethod
    def trim(cls, job: Job, length: int = 10, pipeline: Optional[Pipeline] = None) -> int:
        """Trims the results to length"""
        connection = pipeline if pipeline is not None else job.connection
        return connection.zremrangebyrank(cls.get_key(job.id), 0, -1 * (length + 1))

    @classmethod
    def delete_all(cls, job: Job) -> None:
        """Delete all job results"""
        job.connection.delete(cls.get_key(job.id))

    @classmethod
    def restore(cls, job_id, payload, connection, serializer=None):
        """Create a Result object from given Redis payload"""
        data, timestamp = payload
        result_data = json.loads(data)
        created_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        serializer = resolve_serializer(serializer)
        return_value = result_data.get('return_value')
        if return_value is not None:
            return_value = serializer.loads(b64decode(return_value))

        exc_string = result_data.get('exc_string')
        if exc_string:
            exc_string = zlib.decompress(b64decode(exc_string)).decode()

        return Result(job_id, Result.Type(result_data['type']), connection=connection,
                      id=result_data['id'],
                      created_at=created_at,
                      return_value=return_value,
                      exc_string=exc_string)

    @classmethod
    def get_latest(cls, job: Job, serializer=None):
        """Returns the latest result for given job instance or ID"""
        response = job.connection.zrevrangebyscore(cls.get_key(job.id), '+inf', '-inf',
                                                   start=0, num=1, withscores=True)
        if not response:
            return None

        data, timestamp = response[0]
        return cls.restore(job.id, response[0], connection=job.connection, serializer=serializer)

    @classmethod
    def get_key(cls, job_id):
        return 'rq:results:%s' % job_id

    def save(self, ttl, pipeline=None):
        """Save result data to Redis"""
        key = self.get_key(self.job_id)

        connection = pipeline if pipeline is not None else self.connection
        result = connection.zadd(key, {self.serialize(): self.created_at.timestamp()})
        if ttl is not None:
            connection.expire(key, ttl)
        return result

    def serialize(self):
        data = {
            'type': self.type.value,
            'id': self.id
        }

        if self.exc_string is not None:
            data['exc_string'] = b64encode(zlib.compress(self.exc_string.encode())).decode()

        serialized = self.serializer.dumps(self.return_value)
        if self.return_value is not None:
            data['return_value'] = b64encode(serialized).decode()

        return json.dumps(data)
