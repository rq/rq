from .compat import as_text
from .connections import resolve_connection
from .exceptions import NoSuchJobError
from .job import Job, JobStatus
from .queue import FailedQueue
from .utils import current_timestamp


class BaseRegistry(object):
    """
    Base implementation of a job registry, implemented in Redis sorted set.
    Each job is stored as a key in the registry, scored by expiration time
    (unix timestamp).
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

    def add(self, job, ttl=0, pipeline=None):
        """Adds a job to a registry with expiry time of now + ttl."""
        score = ttl if ttl < 0 else current_timestamp() + ttl
        if pipeline is not None:
            return pipeline.zadd(self.key, score, job.id)

        return self.connection._zadd(self.key, score, job.id)

    def remove(self, job, pipeline=None):
        connection = pipeline if pipeline is not None else self.connection
        return connection.zrem(self.key, job.id)

    def get_expired_job_ids(self, timestamp=None):
        """Returns job ids whose score are less than current timestamp.

        Returns ids for jobs with an expiry time earlier than timestamp,
        specified as seconds since the Unix epoch. timestamp defaults to call
        time if unspecified.
        """
        score = timestamp if timestamp is not None else current_timestamp()
        return [as_text(job_id) for job_id in
                self.connection.zrangebyscore(self.key, 0, score)]

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
    """

    def __init__(self, name='default', connection=None):
        super(StartedJobRegistry, self).__init__(name, connection)
        self.key = 'rq:wip:{0}'.format(name)

    def cleanup(self, timestamp=None):
        """Remove expired jobs from registry and add them to FailedQueue.

        Removes jobs with an expiry time earlier than timestamp, specified as
        seconds since the Unix epoch. timestamp defaults to call time if
        unspecified. Removed jobs are added to the global failed job queue.
        """
        score = timestamp if timestamp is not None else current_timestamp()
        job_ids = self.get_expired_job_ids(score)

        if job_ids:
            failed_queue = FailedQueue(connection=self.connection)

            with self.connection.pipeline() as pipeline:
                for job_id in job_ids:
                    try:
                        job = Job.fetch(job_id, connection=self.connection)
                        job.status = JobStatus.FAILED
                        job.save(pipeline=pipeline)
                        failed_queue.push_job_id(job_id, pipeline=pipeline)
                    except NoSuchJobError:
                        pass

                pipeline.zremrangebyscore(self.key, 0, score)
                pipeline.execute()

        return job_ids


class FinishedJobRegistry(BaseRegistry):
    """
    Registry of jobs that have been completed. Jobs are added to this
    registry after they have successfully completed for monitoring purposes.
    """

    def __init__(self, name='default', connection=None):
        super(FinishedJobRegistry, self).__init__(name, connection)
        self.key = 'rq:finished:{0}'.format(name)

    def cleanup(self, timestamp=None):
        """Remove expired jobs from registry.

        Removes jobs with an expiry time earlier than timestamp, specified as
        seconds since the Unix epoch. timestamp defaults to call time if
        unspecified.
        """
        score = timestamp if timestamp is not None else current_timestamp()
        self.connection.zremrangebyscore(self.key, 0, score)


class DeferredJobRegistry(BaseRegistry):
    """
    Registry of deferred jobs (waiting for another job to finish).
    """

    def __init__(self, name='default', connection=None):
        super(DeferredJobRegistry, self).__init__(name, connection)
        self.key = 'rq:deferred:{0}'.format(name)

    def cleanup(self):
        """This method is only here to prevent errors because this method is
        automatically called by `count()` and `get_job_ids()` methods
        implemented in BaseRegistry."""
        pass


def clean_registries(queue):
    """Cleans StartedJobRegistry and FinishedJobRegistry of a queue."""
    registry = FinishedJobRegistry(name=queue.name, connection=queue.connection)
    registry.cleanup()
    registry = StartedJobRegistry(name=queue.name, connection=queue.connection)
    registry.cleanup()
