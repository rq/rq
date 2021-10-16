import calendar
import json
import zlib

from base64 import b64decode, b64encode
from datetime import datetime, timezone
from enum import Enum

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

    def __init__(self, type, connection, created_at=None, return_value=None, exc_string=None, serializer=None):
        self.return_value = return_value
        self.exc_string = exc_string
        self.type = type
        self.created_at = created_at if created_at else now()
        self.serializer = resolve_serializer(serializer)
        self.connection = connection

    @classmethod
    def create(cls, job, type, ttl, return_value=None, exc_string=None, pipeline=None):
        result = cls(type=type, connection=job.connection, return_value=return_value,
                     exc_string=exc_string, serializer=job.serializer)
        result.save(job.id, ttl=ttl, pipeline=pipeline)
        return result

    @classmethod
    def create_failure(cls, job, ttl, exc_string, pipeline=None):
        result = cls(type=cls.Type.FAILED, connection=job.connection,
                     exc_string=exc_string, serializer=job.serializer)
        result.save(job.id, ttl=ttl, pipeline=pipeline)
        return result

    @classmethod
    def get_latest(cls, job, connection, serializer=None):
        """Returns the latest result for given job instance or ID"""
        job_id = job.id if isinstance(job, Job) else job
        response = connection.zrange(cls.get_key(job_id), 0, 0, desc=True, withscores=True)
        if not response:
            return None

        result_data, timestamp = response[0]
        result_data = json.loads(result_data)
        created_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        if result_data:
            serializer = resolve_serializer(serializer)
            return_value = result_data.get('return_value')
            if return_value is not None:
                return_value = serializer.loads(b64decode(return_value))

            exc_string = result_data.get('exc_string')
            if exc_string:
                exc_string = zlib.decompress(b64decode(exc_string)).decode()

            return Result(Result.Type(result_data['type']), connection=connection,
                          created_at=created_at,
                          return_value=return_value,
                          exc_string=exc_string)

    @classmethod
    def get_key(cls, job_id):
        return 'rq:results:%s' % job_id

    def save(self, job, ttl, pipeline=None):
        """Save result data to Redis"""
        job_id = job.id if isinstance(job, Job) else job

        key = self.get_key(job_id)

        connection = pipeline if pipeline is not None else self.connection
        result = connection.zadd(key, {self.serialize(): calendar.timegm(self.created_at.utctimetuple())})
        if ttl is not None:
            connection.expire(key, ttl)
        return result

    def serialize(self):
        data = {
            'type': self.type.value
        }

        if self.exc_string is not None:
            data['exc_string'] = b64encode(zlib.compress(self.exc_string.encode())).decode()

        serialized = self.serializer.dumps(self.return_value)
        if self.return_value is not None:
            data['return_value'] = b64encode(serialized).decode()

        return json.dumps(data)    
