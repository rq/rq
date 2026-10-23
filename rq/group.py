from __future__ import annotations

from collections.abc import Iterable
from uuid import uuid4

from redis import Redis
from redis.client import Pipeline

from . import Queue
from .defaults import RQ_KEY_PREFIX
from .exceptions import NoSuchGroupError
from .job import Job
from .queue import EnqueueData
from .utils import as_text, missing_keys


class Group:
    """A Group is a container for tracking multiple jobs with a single identifier."""

    REDIS_GROUP_NAME_PREFIX = RQ_KEY_PREFIX + ':group:'
    REDIS_GROUP_KEY = RQ_KEY_PREFIX + ':groups'
    REDIS_GROUP_CLEANING_KEY = RQ_KEY_PREFIX + ':clean_groups'

    def __init__(self, connection: Redis, name: str | None = None):
        self.name = name if name else str(uuid4().hex)
        self.connection = connection
        self.key = f'{self.REDIS_GROUP_NAME_PREFIX}{self.name}'

    def __repr__(self):
        return f'Group(id={self.name})'

    def _add_jobs(self, jobs: Iterable[Job], pipeline: Pipeline):
        """Add jobs to the group"""
        pipeline.sadd(self.key, *[job.id for job in jobs])
        pipeline.sadd(self.REDIS_GROUP_KEY, self.name)
        pipeline.execute()

    def cleanup(self):
        """Delete jobs from the group's job registry that have been deleted or expired from Redis.
        We assume while running this that alive jobs have all been fetched from Redis in fetch_jobs method"""
        job_ids_by_key = {Job.key_for(as_text(job)): as_text(job) for job in self.connection.smembers(self.key)}
        expired_job_ids = [job_ids_by_key[key] for key in missing_keys(self.connection, list(job_ids_by_key))]
        if expired_job_ids:
            self.connection.srem(self.key, *expired_job_ids)

    def enqueue_many(self, queue: Queue, job_datas: Iterable[EnqueueData], pipeline: Pipeline | None = None):
        pipe = pipeline if pipeline else self.connection.pipeline()

        jobs = queue.enqueue_many(job_datas, group_id=self.name, pipeline=pipe)

        self._add_jobs(jobs, pipeline=pipe)

        if pipeline is None:
            pipe.execute()

        return jobs

    def get_jobs(self) -> list:
        """Retrieve list of job IDs from the group key in Redis"""
        self.cleanup()
        job_ids = [as_text(job) for job in self.connection.smembers(self.key)]
        return [job for job in Job.fetch_many(job_ids, self.connection) if job is not None]

    def delete_job(self, job_id: str, pipeline: Pipeline | None = None):
        pipe = pipeline if pipeline else self.connection.pipeline()
        pipe.srem(self.key, job_id)
        if pipeline is None:
            pipe.execute()

    @classmethod
    def create(cls, connection: Redis, name: str | None = None):
        return cls(name=name, connection=connection)

    @classmethod
    def fetch(cls, name: str, connection: Redis):
        """Fetch an existing group from Redis"""
        group = cls(name=name, connection=connection)
        if not connection.exists(Group.get_key(group.name)):
            raise NoSuchGroupError
        return group

    @classmethod
    def all(cls, connection: Redis) -> list[Group]:
        "Returns an iterable of all Groups."
        group_keys = [as_text(key) for key in connection.smembers(cls.REDIS_GROUP_KEY)]
        groups = []
        for key in group_keys:
            try:
                groups.append(cls.fetch(key, connection=connection))
            except NoSuchGroupError:
                connection.srem(cls.REDIS_GROUP_KEY, key)
        return groups

    @classmethod
    def get_key(cls, name: str) -> str:
        """Return the Redis key of the set containing a group's jobs"""
        return cls.REDIS_GROUP_NAME_PREFIX + name

    @classmethod
    def acquire_maintenance_lock(cls, connection: Redis, timeout: int) -> bool:
        """Returns a boolean indicating whether a lock to clean all groups
        is acquired. The lock is never released explicitly. It expires after
        `timeout` seconds so groups are cleaned at most once per window
        across all workers.

        Returns:
            lock_acquired (bool)
        """
        return bool(connection.set(cls.REDIS_GROUP_CLEANING_KEY, 1, nx=True, ex=timeout))

    @classmethod
    def clean_registries(cls, connection: Redis):
        """Loop through groups and delete those that have been deleted.
        If group still has jobs in its registry, delete those that have expired"""
        groups = Group.all(connection=connection)
        with connection.pipeline() as p:
            # Remove expired jobs from groups
            for group in groups:
                group.cleanup()
            p.execute()
            # Remove empty groups from group registry
            for group in groups:
                p.exists(group.key)
            results = p.execute()
            expired_group_ids = []
            for i, key_exists in enumerate(results):
                if not key_exists:
                    expired_group_ids.append(groups[i].name)
            if expired_group_ids:
                p.srem(cls.REDIS_GROUP_KEY, *expired_group_ids)
            p.execute()
