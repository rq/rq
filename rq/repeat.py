from dataclasses import dataclass
from typing import List, Union, Iterable


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

    def __init__(self, times: int, interval: Union[int, Iterable[int]]):
        """Initialize a Repeat instance.

        Args:
            times (int): The number of times to repeat the job. Must be greater than 0.
            interval (Union[int, Iterable[int]]): The intervals between job executions in seconds.
                Can be a single integer value or a list of intervals.

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
