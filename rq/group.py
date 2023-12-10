from typing import List, Optional, Union
from uuid import uuid4

from redis import Redis
from redis.client import Pipeline

from . import Queue
from .exceptions import NoSuchGroupError
from .job import Job
from .queue import EnqueueData
from .utils import as_text


class Group:
    """A Group is a container for tracking multiple jobs with a single identifier."""

    REDIS_GROUP_NAME_PREFIX = 'rq:group:'
    REDIS_GROUP_KEY = 'rq:groups'

    def __init__(self, connection: Redis, id: str = None):
        self.id = id if id else str(uuid4())
        self.connection = connection
        self.key = '{0}{1}'.format(self.REDIS_GROUP_NAME_PREFIX, self.id)

    def __repr__(self):
        return "Group(id={})".format(self.id)

    def _add_jobs(self, jobs: List[Job], pipeline: Optional['Pipeline'] = None):
        """Add jobs to the group"""
        pipe = pipeline if pipeline else self.connection.pipeline()
        pipe.sadd(self.key, *[job.id for job in jobs])
        pipe.sadd(self.REDIS_GROUP_KEY, self.id)
        if pipeline is None:
            pipe.execute()

    def cleanup(self, pipeline: Optional['Pipeline'] = None):
        """Delete jobs from the group's job registry that have been deleted or expired from Redis.
        We assume while running this that alive jobs have all been fetched from Redis in fetch_jobs method"""

        pipe = pipeline if pipeline else self.connection.pipeline()
        job_ids = [as_text(job) for job in list(self.connection.smembers(self.key))]
        expired_jobs = []
        with self.connection.pipeline() as p:
            p.exists(*[Job.key_for(job) for job in job_ids])
            results = p.execute()

        for i, key_exists in enumerate(results):
            if not key_exists:
                expired_jobs.append(job_ids[i])

        if expired_jobs:
            job_ids = [job for job in job_ids if job not in expired_jobs]
            pipe.srem(self.key, *expired_jobs)

        if pipeline is None:
            pipe.execute()

    def enqueue_many(
        self, queue: Union[str, Queue], job_datas: List['EnqueueData'], pipeline: Optional['Pipeline'] = None
    ):
        if isinstance(queue, str):
            queue = Queue(queue, connection=self.connection)
        pipe = pipeline if pipeline else self.connection.pipeline()

        jobs = queue.enqueue_many(job_datas, group_id=self.id, pipeline=pipe)

        self._add_jobs(jobs, pipeline=pipe)

        if pipeline is None:
            pipe.execute()

        return jobs

    def get_jobs(self) -> list:
        """Retrieve list of job IDs from the group key in Redis"""
        job_ids = [as_text(job) for job in self.connection.smembers(self.key)]
        return [job for job in Job.fetch_many(job_ids, self.connection) if job is not None]

    def delete(self):
        self.connection.delete(self.key)

    def delete_job(self, job_id: str, pipeline: Optional['Pipeline'] = None):
        pipe = pipeline if pipeline else self.connection.pipeline()
        pipe.srem(self.key, job_id)
        if pipeline is None:
            pipe.execute()

    @classmethod
    def create(cls, connection: Redis, id: Optional[str] = None):
        return cls(id=id, connection=connection)

    @classmethod
    def fetch(cls, id: str, connection: Redis):
        """Fetch an existing group from Redis"""
        group = cls(id=id, connection=connection)
        group.cleanup()
        if not connection.exists(Group.get_key(group.id)):
            raise NoSuchGroupError
        return group

    @classmethod
    def all(cls, connection: 'Redis') -> List['Group']:
        "Returns an iterable of all Groupes."
        group_keys = [as_text(key) for key in connection.smembers(cls.REDIS_GROUP_KEY)]
        return [Group.fetch(key, connection=connection) for key in group_keys]

    @classmethod
    def get_key(cls, id: str) -> str:
        """Return the Redis key of the set containing a group's jobs"""
        return cls.REDIS_GROUP_NAME_PREFIX + id

    @classmethod
    def clean_registries(cls, connection: 'Redis'):
        """Loop through groups and delete those that have been deleted.
        If group still has jobs in its registry, delete those that have expired"""
        groups = connection.smembers(Group.REDIS_GROUP_KEY)
        for group in groups:
            try:
                group = Group.fetch(as_text(group), connection)
            except NoSuchGroupError:
                connection.srem(Group.REDIS_GROUP_KEY, as_text(group))
