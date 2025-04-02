from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Iterable, List, Optional, Union

if TYPE_CHECKING:
    from redis.client import Pipeline

    from .job import Job
    from .queue import Queue


@dataclass
class Repeat:
    """Defines repeat behavior for scheduled jobs.

    Attributes:
        times (int): The number of times to repeat the job. Must be greater than 0.
        intervals (Union[int, List[int]]): The intervals between job executions in seconds.
            Can be a single integer value or a list of intervals. If a list is provided and it's
            shorter than (times-1), the last interval will be reused for remaining repeats.
    """
    times: int
    intervals: List[int]

    def __init__(self, times: int, interval: Optional[Union[int, Iterable[int]]] = 0):
        """Initialize a Repeat instance.

        Args:
            times (int): The number of times to repeat the job. Must be greater than 0.
            interval (Optional[Union[int, Iterable[int]]], optional): The intervals between job executions in seconds.
                Can be a single integer value or a list of intervals. Defaults to 0 (immediately repeated).

        Raises:
            ValueError: If times is less than 1 or if intervals contains negative values.
        """
        if times < 1:
            raise ValueError('times: please enter a value greater than 0')

        if isinstance(interval, int):
            if interval < 0:
                raise ValueError('intervals: negative numbers are not allowed')
            self.intervals = [interval]
        elif isinstance(interval, Iterable):
            interval_list = list(interval)
            for i in interval_list:
                if i < 0:
                    raise ValueError('intervals: negative numbers are not allowed')
            self.intervals = interval_list
        else:
            raise TypeError('intervals must be an int or iterable of ints')

        self.times = times

    @classmethod
    def get_interval(cls, count: int, intervals: List[int]) -> int:
        """Returns the appropriate interval based on the repeat count.

        Args:
            count (int): Current repeat count (0-based)
            intervals (List[int]): List of intervals

        Returns:
            int: The interval to use
        """

        if count >= len(intervals):
            return intervals[-1]  # Use the last interval if we've run out

        return intervals[count]

    @classmethod
    def schedule(cls, job: 'Job', queue: 'Queue', pipeline: Optional['Pipeline'] = None):
        """Schedules a job to repeat based on its repeat configuration.

        This decrements the job's repeats_left counter and either enqueues
        it immediately (if interval is 0) or schedules it to run after the
        specified interval.

        Args:
            job (Job): The job to repeat
            queue (Queue): The queue to enqueue/schedule the job on
            pipeline (Optional[Pipeline], optional): Redis pipeline to use. Defaults to None.

        Returns:
            scheduled_time (Optional[datetime]): When the job was scheduled to run, or None if not scheduled
        """

        if job.repeats_left is None or job.repeats_left <= 0:
            raise ValueError(f"Cannot schedule job {job.id}: no repeats left")

        pipe = pipeline if pipeline is not None else job.connection.pipeline()

        # Get the interval for this repeat based on remaining repeats
        repeat_count = job.repeats_left - 1  # Count from the end (0-indexed)
        interval = 0

        if job.repeat_intervals:
            interval = cls.get_interval(repeat_count, job.repeat_intervals)

        # Decrement repeats_left
        job.repeats_left = job.repeats_left - 1
        job.save(pipeline=pipe)

        if interval == 0:
            # Enqueue the job immediately
            queue._enqueue_job(job, pipeline=pipe)
        else:
            # Schedule the job to run after the interval
            scheduled_time = datetime.now() + timedelta(seconds=interval)
            queue.schedule_job(job, scheduled_time, pipeline=pipe)

        # Execute the pipeline if we created it
        if pipeline is None:
            pipe.execute()
