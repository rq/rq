import calendar
import json
import zlib

from base64 import b64decode, b64encode
from datetime import datetime, timezone
from enum import Enum

from .compat import as_text
from .serializers import resolve_serializer
from .utils import now


def get_key(job_id):
    return 'rq:results:%s' % job_id


class ResultType(Enum):
    SUCCESSFUL = 1
    FAILED = 2
    STOPPED = 3


class Result(object):

    class Type(Enum):
        SUCCESSFUL = 1
        FAILED = 2
        STOPPED = 3

    def __init__(self, type, connection, created_at=None, return_value=None, exc_string=None, serializer=None):
        self.return_value = return_value
        self.exc_string = None
        self.type = type
        self.created_at = created_at if created_at else now()
        self.serializer = resolve_serializer(serializer)
        self.connection = connection

    @classmethod
    def create(cls, job, type, return_value=None, exc_string=None, default_ttl=None, pipeline=None):
        result = cls(type=type, connection=job.connection, return_value=return_value,
                     exc_string=exc_string, serializer=job.serializer)
        result.save(job.id, timeout=job.get_result_ttl(default_ttl), pipeline=pipeline)
        return result

    @classmethod
    def get_latest(cls, job_id, connection, serializer=None):
        """Returns the latest result for given job_id"""
        response = connection.zrange(cls.get_key(job_id), 0, 0, desc=True, withscores=True)
        if not response:
            return None

        result_data, timestamp = response[0]
        result_data = json.loads(result_data)
        created_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        if result_data:
            serializer = resolve_serializer(serializer)
            return_value = b64decode(result_data['return_value'])
            return Result(Result.Type(result_data['type']), connection=connection,
                          created_at=created_at,
                          return_value=serializer.loads(return_value),
                          exc_string=result_data.get('exc_string'))

    @classmethod
    def get_key(cls, job_id):
        return 'rq:results:%s' % job_id

    def save(self, job_id, timeout, pipeline=None):
        key = self.get_key(job_id)

        connection = pipeline if pipeline is not None else self.connection
        result = connection.zadd(key, {self.serialize(): calendar.timegm(self.created_at.utctimetuple())})
        if timeout is not None:
            connection.expire(key, timeout)
        return result

    def serialize(self):
        data = {
            'type': self.type.value
        }
        # print('###Return value', self.return_value)
        serialized = self.serializer.dumps(self.return_value)
        # print('###Serialized return value', serialized)
        # compressed = print(str(zlib.compress(serialized))
        if self.exc_string is not None:
            data['exc_string'] = as_text(zlib.compress(self.exc_string))
        if self.return_value is not None:
            data['return_value'] = b64encode(serialized).decode()
        # print(data['return_value']).decode("utf-8")
        return json.dumps(data)    
