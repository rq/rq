from typing import List, Optional
from uuid import uuid4

from redis import Redis
from redis.client import Pipeline

from .exceptions import NoSuchBatchError
from .job import Job
from .utils import as_text


class Batch:
    """A Batch is a container for tracking multiple jobs with a single identifier."""

    REDIS_BATCH_NAME_PREFIX = 'rq:batch:'
    REDIS_BATCH_KEY = 'rq:batches'

    def __init__(self, connection: Redis, id: str = None):
        self.id = id if id else str(uuid4())
        self.connection = connection
        self.key = '{0}{1}'.format(self.REDIS_BATCH_NAME_PREFIX, self.id)

    def __repr__(self):
        return "Batch(id={})".format(self.id)

    def _add_jobs(self, jobs: List[Job], pipeline: Optional['Pipeline'] = None):
        """Add jobs to the batch"""
        pipe = pipeline if pipeline else self.connection.pipeline()
        pipe.sadd(self.key, *[job.id for job in jobs])
        pipe.sadd(self.REDIS_BATCH_KEY, self.id)
        if pipeline is None:
            pipe.execute()

    def cleanup(self, pipeline: Optional['Pipeline'] = None):
        """Delete jobs from the batch's job registry that have been deleted or expired from Redis.
        We assume while running this that alive jobs have all been fetched from Redis in fetch_jobs method"""

        pipe = pipeline if pipeline else self.connection.pipeline()
        job_ids = [as_text(job) for job in list(self.connection.smembers(self.key))]
        expired_jobs = []
        with self.connection.pipeline() as p:
            for job in job_ids:
                p.exists(Job.key_for(job))
            results = p.execute()

        for i, key_exists in enumerate(results):
            if not key_exists:
                expired_jobs.append(job_ids[i])

        for job in expired_jobs:
            job_ids.remove(job)
            pipe.srem(self.key, job)

        if not job_ids:
            self.delete()
        if pipeline is None:
            pipe.execute()

    def get_jobs(self) -> list:
        """Retrieve list of job IDs from the batch key in Redis"""
        job_ids = [as_text(job) for job in self.connection.smembers(self.key)]
        return [job for job in Job.fetch_many(job_ids, self.connection) if job is not None]

    def delete(self):
        self.connection.delete(self.key)

    @classmethod
    def create(cls, connection: Redis, id: Optional[str] = None):
        return cls(id=id, connection=connection)

    @classmethod
    def fetch(cls, id: str, connection: Redis):
        """Fetch an existing batch from Redis"""
        batch = cls(id=id, connection=connection)
        batch.cleanup()
        if not connection.exists(Batch.get_key(batch.id)):
            raise NoSuchBatchError
        return batch

    @classmethod
    def all(cls, connection: 'Redis') -> List['Batch']:
        "Returns an iterable of all Batches."
        batch_keys = [as_text(key) for key in connection.smembers(cls.REDIS_BATCH_KEY)]
        return [Batch.fetch(key, connection=connection) for key in batch_keys]

    @classmethod
    def get_key(cls, id: str) -> str:
        """Return the Redis key of the set containing a batch's jobs"""
        return cls.REDIS_BATCH_NAME_PREFIX + id
