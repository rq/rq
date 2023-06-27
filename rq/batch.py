import logging
from typing import List, Optional, Union
from uuid import uuid4

from redis import Redis
from redis.client import Pipeline

from .exceptions import NoSuchBatchError
from .job import Job
from .utils import as_text

logger = logging.getLogger("rq.job")


class Batch:
    """A Batch is a container for tracking multiple jobs with a single identifier."""

    REDIS_BATCH_NAME_PREFIX = 'rq:batch:'
    REDIS_BATCH_KEY = 'rq:batches'

    def __init__(self, id: str = None, jobs: List[Job] = None, connection: Optional['Redis'] = None):
        self.id = id if id else str(uuid4())[-12:]
        self.connection = connection
        self.key = '{0}{1}'.format(self.REDIS_BATCH_NAME_PREFIX, self.id)

        self.jobs = []
        if jobs:
            with connection.pipeline() as pipeline:
                self.add_jobs(jobs, pipeline)
                pipeline.execute()

    def add_jobs(self, jobs: List[Job], pipeline: Optional['Pipeline'] = None):
        """Add jobs to the batch"""
        pipe = pipeline if pipeline else self.connection.pipeline()
        pipe.sadd(self.key, *[job.id for job in jobs])
        pipe.sadd(self.REDIS_BATCH_KEY, self.id)
        self.jobs += jobs
        for job in jobs:
            job.set_batch_id(self.id, pipeline=pipe)
            job.save(pipeline=pipe)
        if pipeline is None:
            pipe.execute()

    def delete_expired_jobs(self, pipeline: Optional['Pipeline'] = None):
        """Delete jobs from the batch's job registry that have been deleted or expired from Redis."""

        pipe = pipeline if pipeline else self.connection.pipeline()
        job_ids = {as_text(job) for job in self.connection.smembers(self.key)}
        expired_jobs = job_ids - set([job.id for job in self.jobs if job])  # Return jobs that can't be fetched
        for job in expired_jobs:
            pipe.srem(self.key, job)
        if pipeline is None:
            pipe.execute()

    def fetch_jobs(self) -> list:
        job_ids = [as_text(job) for job in self.connection.smembers(self.key)]
        self.jobs = [job for job in Job.fetch_many(job_ids, self.connection) if job is not None]

    def refresh(self, pipeline: Optional['Pipeline'] = None):
        pipe = pipeline if pipeline else self.connection.pipeline()
        self.fetch_jobs()
        self.delete_expired_jobs(pipeline=pipe)
        if not self.jobs:  # This batch's jobs have all expired
            self.delete()
            raise NoSuchBatchError
        if pipeline is None:
            pipe.execute()

    def cancel_jobs(self, pipeline: Optional['Pipeline'] = None):
        pipe = pipeline if pipeline else self.connection.pipeline()
        for job in self.jobs:
            job.cancel(pipeline=pipe)
        if pipeline is None:
            pipe.execute()

    def delete(self):
        self.connection.delete(self.key)

    @classmethod
    def fetch(cls, id: str, connection: Redis, fetch_jobs=False):
        batch = cls(id, connection=connection)
        batch.refresh()
        return batch
