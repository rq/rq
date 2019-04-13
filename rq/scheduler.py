import os

from .job import Job
from .queue import Queue
from .registry import ScheduledJobRegistry
from .utils import current_timestamp


SCHEDULER_KEY_TEMPLATE = 'rq:scheduler:%s'
SCHEDULER_LOCKING_KEY_TEMPLATE = 'rq:scheduler-lock:%s'


class RQScheduler(object):

    def __init__(self, queues, connection, interval=1):
        self._queue_names = parse_names(queues)
        self._scheduled_job_registries = []
        for name in self._queue_names:
            self._scheduled_job_registries.append(
                ScheduledJobRegistry(name, connection=connection)
            )
        self.connection = connection
        self.interval = 1

    @classmethod
    def acquire_lock(cls, name, connection):
        """Returns True if lock is successfully acquired"""
        return connection.set(
            cls.get_locking_key(name), os.getpid(), nx=True, ex=5
        )

    @classmethod
    def get_locking_key(self, name):
        """Returns scheduler key for a given queue name"""
        return SCHEDULER_LOCKING_KEY_TEMPLATE % name

    def get_key(self, name):
        """Returns scheduler key for a given queue name"""
        return SCHEDULER_KEY_TEMPLATE % name

    def enqueue_scheduled_jobs(self):
        """Enqueue jobs whose timestamp is in the past"""
        for registry in self._scheduled_job_registries:
            timestamp = current_timestamp()
            job_ids = registry.get_jobs_to_schedule(timestamp)

            if not job_ids:
                continue

            queue = Queue(registry.name, connection=self.connection)

            with self.connection.pipeline() as pipeline:
                # This should be done in bulk
                for job_id in job_ids:
                    job = Job.fetch(job_id, connection=self.connection)
                    queue.enqueue_job(job, pipeline=pipeline)
                registry.remove_jobs(timestamp)
                pipeline.execute()

    def heartbeart(self):
        """Updates the TTL on scheduler keys"""
        if len(self._queue_names) > 1:
            with self.connection.pipeline() as pipeline:
                for name in self._queue_names:
                    pipeline.expire(self.interval + 5)
                pipeline.execute()
        else:
            self.connection.expire(self.interval + 5)


def parse_names(queues_or_names):
    """Given a list of strings or queues, returns queue names"""
    names = []
    for queue_or_name in queues_or_names:
        if isinstance(queue_or_name, Queue):
            names.append(queue_or_name.name)
        else:
            names.append(str(queue_or_name))
    return names
