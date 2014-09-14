from .compat import as_text
from .connections import resolve_connection
from .queue import FailedQueue
from .utils import current_timestamp


class BaseRegistry(object):
    """
    Base implementation of job registry, implemented in Redis sorted set. Each job
    is stored as a key in the registry, scored by expiration time (unix timestamp).

    Jobs with scores are lower than current time is considered "expired" and
    should be cleaned up.
    """

    def __init__(self, name='default', connection=None):
        self.name = name
        self.connection = resolve_connection(connection)

    def __len__(self):
        """Returns the number of jobs in this registry"""
        return self.count

    @property
    def count(self):
        """Returns the number of jobs in this registry"""
        self.cleanup()
        return self.connection.zcard(self.key)

    def add(self, job, timeout, pipeline=None):
        """Adds a job to StartedJobRegistry with expiry time of now + timeout."""
        score = current_timestamp() + timeout
        if pipeline is not None:
            return pipeline.zadd(self.key, score, job.id)

        return self.connection._zadd(self.key, score, job.id)

    def remove(self, job, pipeline=None):
        connection = pipeline if pipeline is not None else self.connection
        return connection.zrem(self.key, job.id)

    def get_expired_job_ids(self):
        """Returns job ids whose score are less than current timestamp."""
        return [as_text(job_id) for job_id in
                self.connection.zrangebyscore(self.key, 0, current_timestamp())]

    def get_job_ids(self, start=0, end=-1):
        """Returns list of all job ids."""
        self.cleanup()
        return [as_text(job_id) for job_id in
                self.connection.zrange(self.key, start, end)]


class StartedJobRegistry(BaseRegistry):
    """
    Registry of currently executing jobs. Each queue maintains a
    StartedJobRegistry. Jobs in this registry are ones that are currently
    being executed.

    Jobs are added to registry right before they are executed and removed
    right after completion (success or failure).

    Jobs whose score are lower than current time is considered "expired".
    """

    def __init__(self, name='default', connection=None):
        super(StartedJobRegistry, self).__init__(name, connection)
        self.key = 'rq:wip:%s' % name

    def cleanup(self):
        """Remove expired jobs from registry and add them to FailedQueue."""
        job_ids = self.get_expired_job_ids()

        if job_ids:
            failed_queue = FailedQueue(connection=self.connection)
            with self.connection.pipeline() as pipeline:
                for job_id in job_ids:
                    failed_queue.push_job_id(job_id, pipeline=pipeline)
                pipeline.zremrangebyscore(self.key, 0, current_timestamp())
                pipeline.execute()

        return job_ids


class FinishedJobRegistry(BaseRegistry):
    """
    Registry of jobs that have been completed. Jobs are added to this
    registry after they have successfully completed for monitoring purposes.
    """

    def __init__(self, name='default', connection=None):
        super(FinishedJobRegistry, self).__init__(name, connection)
        self.key = 'rq:finished:%s' % name

    def cleanup(self):
        """Remove expired jobs from registry."""
        self.connection.zremrangebyscore(self.key, 0, current_timestamp())
