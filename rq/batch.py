import logging
from typing import List, Union, Optional
from uuid import uuid4

from redis import Redis
from redis.client import Pipeline

from .job import Job
from .utils import as_text

logger = logging.getLogger("rq.job")


class Batch:
    REDIS_BATCH_NAME_PREFIX = 'rq:batch:'

    def __init__(
        self, id: str = str(uuid4())[-12:], jobs: List[Job] = None, connection: Optional['Redis'] = None, ttl=500
    ):
        self.id = id
        self.connection = connection
        self.key = '{0}{1}'.format(self.REDIS_BATCH_NAME_PREFIX, self.id)
        self.jobs_key = self.key + ":jobs"
        self.ttl = ttl

        self.jobs = []
        if jobs:
            self.add_jobs(jobs)

    def add_jobs(self, jobs: List[Union['Job', str]], pipeline=None):
        pipe = pipeline if pipeline else self.connection.pipeline
        self.job_ids = [job.id for job in jobs]
        self.connection.sadd(self.jobs_key, *self.job_ids)
        self.jobs += jobs
        for job in jobs:
            job.set_batch_id(self.id)
            job.save(pipeline=self.connection)
        self.renew_ttl()
        self.save()

    def fetch_jobs(self) -> list:
        self.job_ids = [as_text(job) for job in self.connection.smembers(self.key + ":jobs")]
        self.jobs = Job.fetch_many(self.job_ids, self.connection)

    def renew_ttl(self, pipeline=None):
        pipe = pipeline if pipeline else self.connection
        pipe.expire(self.key, self.ttl)
        pipe.expire(self.key + ":jobs", self.ttl)
        for job in self.jobs:
            pipe.expire(job.key, self.ttl)

    def suspend_ttl(self, pipeline=None):
        pipe = pipeline if pipeline else self.connection
        pipe.persist(self.key)
        pipe.persist(self.key + ":jobs")
        for job in self.jobs:
            pipe.persist(job.key)

    def save(self, pipeline=None):
        pipe = pipeline if pipeline else self.connection
        pipe.hmset(self.key, {"id": self.id, "ttl": self.ttl})

    def refresh(self):
        data = {key.decode(): value.decode() for key, value in self.connection.hgetall(self.key).items()}
        self.fetch_jobs()
        self.ttl = int(data["ttl"])

    def cleanup(self, pipeline=None):
        pass

    def get_status(self):
        pass

    def cancel_jobs(self):
        pass

    def expire_jobs(self):
        for job in self.jobs:
            self.connection.expire(job.key, self.ttl)
            
    @classmethod
    def fetch(cls, id: str, connection: Optional['Redis'] = None):
        batch = cls(id, connection=connection)
        batch.refresh()
        return batch
