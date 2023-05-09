from .job import Job, JobStatus
from redis.client import Pipeline
from typing import List
from redis.exceptions import WatchError


class Dependency:
    @classmethod
    def get_ready_jobs(cls, jobs: List['Job'], pipeline: Pipeline):
        pipe = pipeline if pipeline else cls.connection
        ready_jobs = []
        for job in jobs:
            if not job._dependency_ids:
                pass
            while True:
                try:
                    pipe.watch(*[Job.key_for(dependency_id) for dependency_id in job._dependency_ids])
                    job.register_dependency(pipeline=pipe)
                    if job.dependencies_are_met(pipeline=pipe):
                        ready_jobs.append(job)
                    else:
                        pass
                    pipe.execute()
                except WatchError:
                    continue
                break
        return ready_jobs
