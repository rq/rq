from .connections import resolve_connection
from .queue import FailedQueue
from .utils import current_timestamp


class WorkingQueue:
    """
    Registry of currently executing jobs. Each queue maintains a WorkingQueue.
    WorkingQueue contains job keys that are currently being executed.
    Each key is scored by job's expiration time (datetime started + timeout).

    Jobs are added to registry right before they are executed and removed
    right after completion (success or failure).

    Jobs whose score are lower than current time is considered "expired".
    """

    def __init__(self, name, connection=None):
        self.name = name
        self.key = 'rq:wip:%s' % name
        self.connection = resolve_connection(connection)

    def add(self, job, timeout):
        """Adds a job to WorkingQueue with expiry time of now + timeout."""
        return self.connection._zadd(self.key, current_timestamp() + timeout,
                                     job.id)

    def remove(self, job):
        return self.connection.zrem(self.key, job.id)

    def get_expired_job_ids(self):
        """Returns job ids whose score are less than current timestamp."""
        return self.connection.zrangebyscore(self.key, 0, current_timestamp())

    def get_job_ids(self, start=0, end=-1):
        """Returns list of all job ids."""
        return self.connection.zrange(self.key, start, end)

