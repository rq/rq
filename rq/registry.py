from .compat import as_text
from .exceptions import NoSuchJobError
from .job import Job, JobStatus
from .queue import FailedQueue
from .utils import current_timestamp, transaction
from .keys import (started_registry_key_from_name,
                   finished_registry_key_from_name,
                   deferred_registry_key_from_name)


class BaseRegistry(object):
    """
    Base implementation of a job registry, implemented in Redis sorted set.
    Each job is stored as a key in the registry, scored by expiration time
    (unix timestamp).
    """

    def __init__(self, name='default', connection=None):
        from .connections import resolve_connection
        self.name = name
        self.connection = resolve_connection(connection)

    def __len__(self):
        """Returns the number of jobs in this registry"""
        return self.count

    @property
    def rq_conn(self):
        return self.connection

    @property
    @transaction
    def count(self):
        """Returns the number of jobs in this registry"""
        count = self.connection._zcard(self.key)
        count -= self.cleanup()
        return count

    @transaction
    def add(self, job_or_id, ttl=0):
        """Adds a job to a registry with expiry time of now + ttl."""
        score = ttl if ttl < 0 else current_timestamp() + ttl
        if isinstance(job_or_id, Job):
            job_id = job_or_id.id
        else:
            job_id = job_or_id
        return self.connection._zadd(self.key, score, job_id)

    @transaction
    def remove(self, job_or_id):
        if isinstance(job_or_id, Job):
            job_id = job_or_id.id
        else:
            job_id = job_or_id
        return self.connection._zrem(self.key, job_id)

    @transaction
    def get_expired_job_ids(self, timestamp=None):
        """Returns job ids whose score are less than current timestamp.

        Returns ids for jobs with an expiry time earlier than timestamp,
        specified as seconds since the Unix epoch. timestamp defaults to call
        time if unspecified.
        """
        score = timestamp if timestamp is not None else current_timestamp()
        return [as_text(job_id) for job_id in
                self.connection._zrangebyscore(self.key, 0, score)]

    @transaction
    def _get_raw_job_ids(self, start=0, end=-1):
        return [as_text(job_id) for job_id in
                self.connection._zrange(self.key, start, end)]

    def get_job_ids(self, start=0, end=-1):
        """
        Returns list of all job ids.
        TODO make this a transaction by doing a zrangebyscore first
        """
        self.cleanup()
        return self._get_raw_job_ids(start, end)

    def cleanup(self):
        """
        Should return the number of removed elements
        """
        raise NotImplemented("BaseRegistry has no cleanup() implemented!")


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
        self.key = started_registry_key_from_name(name)

    @transaction
    def cleanup(self, timestamp=None):
        """Remove expired jobs from registry and add them to FailedQueue.

        Removes jobs with an expiry time earlier than timestamp, specified as
        seconds since the Unix epoch. timestamp defaults to call time if
        unspecified. Removed jobs are added to the global failed job queue.

        :return int: Change in size of registery (combination of cleaned, and
                missing jobs)
        """
        score = timestamp if timestamp is not None else current_timestamp()
        job_ids = self.get_expired_job_ids(score)
        failed_queue = FailedQueue(connection=self.connection)

        for job_id in job_ids:
            try:
                job = self.connection.get_job(job_id)
            except NoSuchJobError:
                continue
            job.set_status(JobStatus.FAILED)
            job.save()
            failed_queue.push_job_id(job_id)

        self.connection._zremrangebyscore(self.key, 0, score)
        return len(job_ids)

class FinishedJobRegistry(BaseRegistry):
    """
    Registry of jobs that have been completed. Jobs are added to this
    registry after they have successfully completed for monitoring purposes.
    """

    def __init__(self, name='default', connection=None):
        super(FinishedJobRegistry, self).__init__(name, connection)
        self.key = finished_registry_key_from_name(name)

    @transaction
    def cleanup(self, timestamp=None):
        """Remove expired jobs from registry.

        Removes jobs with an expiry time earlier than timestamp, specified as
        seconds since the Unix epoch. timestamp defaults to call time if
        unspecified.
        """
        score = timestamp if timestamp is not None else current_timestamp()
        return self.connection._zremrangebyscore(self.key, 0, score)


class DeferredJobRegistry(BaseRegistry):
    """
    Registry of deferred jobs (waiting for another job to finish).
    """

    def __init__(self, name='default', connection=None):
        super(DeferredJobRegistry, self).__init__(name, connection)
        self.key = deferred_registry_key_from_name(name)

    def cleanup(self):
        """This method is only here to prevent errors because this method is
        automatically called by `count()` and `get_job_ids()` methods
        implemented in BaseRegistry."""
        pass


