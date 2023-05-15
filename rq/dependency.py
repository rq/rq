from .job import Job
from redis.client import Pipeline
from typing import List
from redis.exceptions import WatchError


class Dependency:
    @classmethod
    def get_jobs_with_dependencies_met(cls, jobs: List['Job'], pipeline: Pipeline):
        pipe = pipeline if pipeline else cls.connection
        jobs_with_dependencies_met = []
        jobs_with_dependencies_unmet = []
        for job in jobs:
            while True:
                try:
                    pipe.watch(*[Job.key_for(dependency_id) for dependency_id in job._dependency_ids])
                    job.register_dependency(pipeline=pipe)
                    if job.dependencies_are_met(pipeline=pipe):
                        jobs_with_dependencies_met.append(job)
                    else:
                        jobs_with_dependencies_unmet.append(job)
                    pipe.execute()
                except WatchError:
                    continue
                break
        return jobs_with_dependencies_met, jobs_with_dependencies_unmet
