from datetime import datetime, timedelta, timezone
from typing import Optional

from redis import Redis

from rq.utils import now


class IntermediateQueue(object):

    def __init__(self, queue_key: str, connection: Redis):
        self.queue_key = queue_key
        self.key = self.get_intermediate_queue_key(queue_key)
        self.connection = connection

    @classmethod
    def get_intermediate_queue_key(cls, queue_key: str) -> str:
        """Returns the intermediate queue key for a given queue key.

        Args:
            key (str): The queue key

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
            timestamp (float): The timestamp
        """
        # TODO: job_id should be changed to execution ID in 2.0
        return bool(self.connection.set(self.get_first_seen_key(job_id), now().timestamp(), nx=True, ex=3600 * 24))

    def get_first_seen(self, job_id: str) -> Optional[datetime]:
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
