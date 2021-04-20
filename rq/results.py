import calendar
import json
import zlib

from enum import Enum

from .compat import as_text
from .serializers import resolve_serializer
from .utils import current_timestamp, now


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

    def __init__(self, type, return_value=None, exc_string=None, serializer=None):
        self.return_value = return_value
        self.exc_string = None
        self.type = type
        self.created_at = now()
        self.serializer = resolve_serializer(serializer)

    @classmethod
    def create(cls, job, type, return_value=None, exc_string=None, default_ttl=None):
        result = cls(type=type, return_value=return_value, exc_string=exc_string, serializer=job.serializer)
        result.save(job.id, timeout=job.get_result_ttl(default_ttl), connection=job.connection)
        return result

    @classmethod
    def get_latest(cls, job_id, connection):
        """Returns the latest result for given job_id"""
        result = connection.zrange(cls.get_key(job_id), 0, 0, desc=True, withscores=True)
        return result

    @classmethod
    def get_key(cls, job_id):
        return 'rq:results:%s' % job_id

    def save(self, job_id, timeout, connection):
        key = self.get_key(job_id)
        result = connection.zadd(key, {self.serialize(): calendar.timegm(self.created_at.utctimetuple())})
        if timeout is not None:
            connection.expire(key, timeout)
        return result

    def serialize(self):
        data = {
            'type': self.type.value
        }
        print('###Return value', self.return_value)
        serialized = self.serializer.dumps((self.return_value))
        print('###Serialized return value', serialized)
        # compressed = print(str(zlib.compress(serialized))
        if self.exc_string is not None:
            data['exc_string'] = as_text(zlib.compress(self.exc_string))
        if self.return_value is not None:
            data['return_value'] = str(serialized)
        # print(data['return_value']).decode("utf-8")
        return json.dumps(data)
