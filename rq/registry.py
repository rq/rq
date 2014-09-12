from .compat import as_text
from .connections import resolve_connection
from .queue import FailedQueue
from .utils import current_timestamp


class StartedJobRegistry:
    """
    Registry of currently executing jobs. Each queue maintains a StartedJobRegistry.
    StartedJobRegistry contains job keys that are currently being executed.
    Each key is scored by job's expiration time (datetime started + timeout).

    Jobs are added to registry right before they are executed and removed
    right after completion (success or failure).

    Jobs whose score are lower than current time is considered "expired".
    """

    def __init__(self, name='default', connection=None):
        self.name = name
        self.key = 'rq:wip:%s' % name
        self.connection = resolve_connection(connection)

    def __len__(self):
        """Returns the number of jobs in this registry"""
        return self.count

    @property
    def count(self):
        """Returns the number of jobs in this registry"""
        self.move_expired_jobs_to_failed_queue()
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
        self.move_expired_jobs_to_failed_queue()
        return [as_text(job_id) for job_id in
                self.connection.zrange(self.key, start, end)]

    def move_expired_jobs_to_failed_queue(self):
        """Remove expired jobs from registry and add them to FailedQueue."""
        job_ids = self.get_expired_job_ids()

        if job_ids:
            failed_queue = FailedQueue(connection=self.connection)
            with self.connection.pipeline() as pipeline:
                for job_id in job_ids:
                    failed_queue.push_job_id(job_id, pipeline=pipeline)
                pipeline.execute()

        return job_ids
