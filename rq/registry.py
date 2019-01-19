from .compat import as_text
from .connections import resolve_connection
from .defaults import DEFAULT_FAILURE_TTL
from .exceptions import InvalidJobOperation, NoSuchJobError
from .job import Job, JobStatus
from .queue import Queue
from .utils import backend_class, current_timestamp


class BaseRegistry(object):
    """
    Base implementation of a job registry, implemented in Redis sorted set.
    Each job is stored as a key in the registry, scored by expiration time
    (unix timestamp).
    """
    job_class = Job
    key_template = 'rq:registry:{0}'

    def __init__(self, name='default', connection=None, job_class=None,
                 queue=None):
        if queue:
            self.name = queue.name
            self.connection = resolve_connection(queue.connection)
        else:
            self.name = name
            self.connection = resolve_connection(connection)

        self.key = self.key_template.format(self.name)
        self.job_class = backend_class(self, 'job_class', override=job_class)

    def __len__(self):
        """Returns the number of jobs in this registry"""
        return self.count

    def __contains__(self, item):
        """
        Returns a boolean indicating registry contains the given
        job instance or job id.
        """
        job_id = item
        if isinstance(item, self.job_class):
            job_id = item.id
        return self.connection.zscore(self.key, job_id) is not None

    @property
    def count(self):
        """Returns the number of jobs in this registry"""
        self.cleanup()
        return self.connection.zcard(self.key)

    def add(self, job, ttl=0, pipeline=None):
        """Adds a job to a registry with expiry time of now + ttl, unless it's -1 which is set to +inf"""
        score = ttl if ttl < 0 else current_timestamp() + ttl
        if score == -1:
            score = '+inf'
        if pipeline is not None:
            return pipeline.zadd(self.key, {job.id: score})

        return self.connection.zadd(self.key, {job.id: score})

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

    def get_queue(self):
        """Returns Queue object associated with this registry."""
        return Queue(self.name, connection=self.connection)


class StartedJobRegistry(BaseRegistry):
    """
    Registry of currently executing jobs. Each queue maintains a
    StartedJobRegistry. Jobs in this registry are ones that are currently
    being executed.

    Jobs are added to registry right before they are executed and removed
    right after completion (success or failure).
    """
    key_template = 'rq:wip:{0}'

    def cleanup(self, timestamp=None):
        """Remove expired jobs from registry and add them to FailedJobRegistry.

        Removes jobs with an expiry time earlier than timestamp, specified as
        seconds since the Unix epoch. timestamp defaults to call time if
        unspecified. Removed jobs are added to the global failed job queue.
        """
        score = timestamp if timestamp is not None else current_timestamp()
        job_ids = self.get_expired_job_ids(score)

        if job_ids:
            failed_job_registry = FailedJobRegistry(self.name, self.connection)

            with self.connection.pipeline() as pipeline:
                for job_id in job_ids:
                    try:
                        job = self.job_class.fetch(job_id,
                                                   connection=self.connection)
                        job.set_status(JobStatus.FAILED)
                        job.save(pipeline=pipeline, include_meta=False)
                        job.cleanup(ttl=-1, pipeline=pipeline)
                        failed_job_registry.add(job, job.failure_ttl)
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
    key_template = 'rq:finished:{0}'

    def cleanup(self, timestamp=None):
        """Remove expired jobs from registry.

        Removes jobs with an expiry time earlier than timestamp, specified as
        seconds since the Unix epoch. timestamp defaults to call time if
        unspecified.
        """
        score = timestamp if timestamp is not None else current_timestamp()
        self.connection.zremrangebyscore(self.key, 0, score)


class FailedJobRegistry(BaseRegistry):
    """
    Registry of containing failed jobs.
    """
    key_template = 'rq:failed:{0}'

    def cleanup(self, timestamp=None):
        """Remove expired jobs from registry.

        Removes jobs with an expiry time earlier than timestamp, specified as
        seconds since the Unix epoch. timestamp defaults to call time if
        unspecified.
        """
        score = timestamp if timestamp is not None else current_timestamp()
        self.connection.zremrangebyscore(self.key, 0, score)

    def add(self, job, ttl=None, exc_string='', pipeline=None):
        """
        Adds a job to a registry with expiry time of now + ttl.
        `ttl` defaults to DEFAULT_FAILURE_TTL if not specified.
        """
        if ttl is None:
            ttl = DEFAULT_FAILURE_TTL
        score = ttl if ttl < 0 else current_timestamp() + ttl

        if pipeline:
            p = pipeline
        else:
            p = self.connection.pipeline()

        job.exc_info = exc_string
        job.save(pipeline=p, include_meta=False)
        job.cleanup(ttl=-1, pipeline=p)  # failed job won't expire
        p.zadd(self.key, {job.id: score})

        if not pipeline:
            p.execute()

    def requeue(self, job_or_id):
        """Requeues the job with the given job ID."""
        if isinstance(job_or_id, self.job_class):
            job = job_or_id
        else:
            job = self.job_class.fetch(job_or_id, connection=self.connection)

        result = self.connection.zrem(self.key, job.id)
        if not result:
            raise InvalidJobOperation

        queue = Queue(job.origin, connection=self.connection,
                      job_class=self.job_class)

        return queue.enqueue_job(job)


class DeferredJobRegistry(BaseRegistry):
    """
    Registry of deferred jobs (waiting for another job to finish).
    """
    key_template = 'rq:deferred:{0}'

    def cleanup(self):
        """This method is only here to prevent errors because this method is
        automatically called by `count()` and `get_job_ids()` methods
        implemented in BaseRegistry."""
        pass


def clean_registries(queue):
    """Cleans StartedJobRegistry and FinishedJobRegistry of a queue."""
    registry = FinishedJobRegistry(name=queue.name,
                                   connection=queue.connection,
                                   job_class=queue.job_class)
    registry.cleanup()
    registry = StartedJobRegistry(name=queue.name,
                                  connection=queue.connection,
                                  job_class=queue.job_class)
    registry.cleanup()

    registry = FailedJobRegistry(name=queue.name,
                                 connection=queue.connection,
                                 job_class=queue.job_class)
    registry.cleanup()
