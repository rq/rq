from redis import Redis

from .connections import get_current_connection
from .defaults import (DEFAULT_JOB_CLASS, DEFAULT_QUEUE_CLASS,
                       DEFAULT_SERIALIZER_CLASS, DEFAULT_WORKER_CLASS)
from .serializers import resolve_serializer
from .utils import resolve_class


class Config:
    def __init__(self, template=None, job_class=None, queue_class=None,
                 worker_class=None, serializer=None, connection=None):
        self._template = template
        self._job_class = job_class
        self._queue_class = queue_class
        self._worker_class = worker_class
        self._serializer = serializer
        self._connection = connection

    @property
    def job_class(self):
        if self._job_class is not None:
            return resolve_class(self._job_class)
        elif self._template is not None:
            return self._template.job_class
        else:
            return DEFAULT_CONFIG.job_class

    @property
    def queue_class(self):
        if self._queue_class is not None:
            return resolve_class(self._queue_class)
        elif self._template is not None:
            return self._template.queue_class
        else:
            return DEFAULT_CONFIG.queue_class

    @property
    def worker_class(self):
        if self._worker_class is not None:
            return resolve_class(self._worker_class)
        elif self._template is not None:
            return self._template.worker_class
        else:
            return DEFAULT_CONFIG.worker_class

    @property
    def serializer(self):
        if self._serializer is not None:
            return resolve_serializer(self._serializer)
        elif self._template is not None:
            return self._template.serializer
        else:
            return DEFAULT_CONFIG.serializer

    @property
    def connection(self):
        if self._connection is not None:
            return self._connection
        elif self._template is not None:
            return self._template.connection
        else:  # don't break deprecated connections.py
            current_connection = get_current_connection()
            return Redis() if current_connection is None else current_connection

    def __repr__(self):
        if self == DEFAULT_CONFIG:
            return 'DEFAULT_CONFIG'
        return '%s(\n    job_class=%r\n    queue_class=%r\n    worker_class=%r\n    serializer=%r\n    connection=%r\n)' % (
            self.__class__.__name__,
            self.job_class,
            self.queue_class,
            self.worker_class,
            self.serializer,
            self.connection
        )

    def __str__(self):
        return '%s<%s>' % (
            self.__class__.__name__,
            ' '.join([
                ('job_class=%s' % self.job_class),
                ('queue_class=%s' % self.queue_class),
                ('worker_class=%s' % self.worker_class),
                ('serializer=%s' % self.serializer),
                ('connection=%s' % self.connection)
            ])
           
        )

    def __eq__(self, other):
        if isinstance(other, Config):
            return self.job_class == other.job_class and \
                   self.queue_class == other.queue_class and \
                   self.worker_class == other.worker_class and \
                   self.serializer == other.serializer and \
                   self.connection == other.connection
        else:
            return False


DEFAULT_CONFIG = Config(
    job_class=DEFAULT_JOB_CLASS,
    queue_class=DEFAULT_QUEUE_CLASS,
    worker_class=DEFAULT_WORKER_CLASS,
    serializer=DEFAULT_SERIALIZER_CLASS,
    connection=None
)
