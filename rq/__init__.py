# flake8: noqa

from .connections import (Connection, get_current_connection, pop_connection,
                          push_connection, use_connection)
from .job import cancel_job, get_current_job, requeue_job, Retry
from .queue import Queue
from .version import VERSION
from .worker import SimpleWorker, Worker

__version__ = VERSION
