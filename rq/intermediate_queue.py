from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from redis import Redis

from rq.exceptions import UnsupportedCompressionError
from rq.utils import now

if TYPE_CHECKING:
    from .queue import Queue
    from .worker import BaseWorker


_REQUEUE_JOB_SCRIPT = """
local job_ids = redis.call('LRANGE', KEYS[1], 0, -1)
for _, queued_job_id in ipairs(job_ids) do
    if queued_job_id == ARGV[1] then
        redis.call('LREM', KEYS[2], 1, ARGV[1])
        return 0
    end
end

redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('LPUSH', KEYS[1], ARGV[1])
return 1
"""


class IntermediateQueue:
    def __init__(self, queue_key: str, connection: Redis):
        self.queue_key = queue_key
        self.key = self.get_intermediate_queue_key(queue_key)
        self.connection = connection

    @classmethod
    def get_intermediate_queue_key(cls, queue_key: str) -> str:
        """Returns the intermediate queue key for a given queue key.

        Args:
            queue_key (str): The queue key

        Returns:
            str: The intermediate queue key
        """
        return f'{queue_key}:intermediate'

    def get_first_seen_key(self, job_id: str) -> str:
        """Returns the first seen key for a given job ID.

        Args:
            job_id (str): The job ID

        Returns:
            str: The first seen key
        """
        return f'{self.key}:first_seen:{job_id}'

    def set_first_seen(self, job_id: str) -> bool:
        """Sets the first seen timestamp for a job.

        Args:
            job_id (str): The job ID
        """
        # TODO: job_id should be changed to execution ID in 2.0
        return bool(self.connection.set(self.get_first_seen_key(job_id), now().timestamp(), nx=True, ex=3600 * 24))

    def get_first_seen(self, job_id: str) -> datetime | None:
        """Returns the first seen timestamp for a job.

        Args:
            job_id (str): The job ID

        Returns:
            Optional[datetime]: The timestamp
        """
        timestamp = self.connection.get(self.get_first_seen_key(job_id))
        if timestamp:
            return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
        return None

    def should_be_cleaned_up(self, job_id: str) -> bool:
        """Returns whether a job should be cleaned up.
        A job in intermediate queue should be cleaned up if it has been there for more than 1 minute.

        Args:
            job_id (str): The job ID

        Returns:
            bool: Whether the job should be cleaned up
        """
        # TODO: should be changed to execution ID in 2.0
        first_seen = self.get_first_seen(job_id)
        if not first_seen:
            return False
        return now() - first_seen > timedelta(minutes=1)

    def get_job_ids(self) -> list[str]:
        """Returns the job IDs in the intermediate queue.

        Returns:
            List[str]: The job IDs
        """
        return [job_id.decode() for job_id in self.connection.lrange(self.key, 0, -1)]

    def remove(self, job_id: str) -> None:
        """Removes a job from the intermediate queue.

        Args:
            job_id (str): The job ID
        """
        self.connection.lrem(self.key, 1, job_id)

    def requeue(self, job_id: str) -> None:
        """Return a dequeued job ID to its original queue atomically and at most once."""
        script = self.connection.register_script(_REQUEUE_JOB_SCRIPT)
        script(keys=[self.queue_key, self.key], args=[job_id])

    def cleanup(self, worker: BaseWorker, queue: Queue) -> None:
        job_ids = self.get_job_ids()

        for job_id in job_ids:
            try:
                job = queue.fetch_job(job_id)
            except UnsupportedCompressionError:
                # A job that cannot be restored (for example, a Zstandard job on
                # a worker without Zstandard support) must not remain stuck here.
                self.requeue(job_id)
                continue

            if job_id not in queue.started_job_registry:
                if not job:
                    # If the job doesn't exist in the queue, we can safely remove it from the intermediate queue.
                    self.remove(job_id)
                    continue

                # If this is the first time we've seen this job, do nothing.
                # `set_first_seen` will return `True` if the key was set, `False` if it already existed.
                if self.set_first_seen(job_id):
                    continue

                if self.should_be_cleaned_up(job_id):
                    worker.handle_job_failure(job, queue, exc_string='Job was stuck in intermediate queue.')
                    self.remove(job_id)
