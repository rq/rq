import traceback

from .connections import get_current_connection
from .queue import get_failed_queue
from .worker import Worker


def move_to_failed_queue(job, *exc_info):
    """Default exception handler: move the job to the failed queue."""
    exc_string = Worker._get_safe_exception_string(traceback.format_exception(*exc_info))
    failed_queue = get_failed_queue(get_current_connection(), job.__class__)
    failed_queue.quarantine(job, exc_info=exc_string)
