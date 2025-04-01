from rq.job import Job
from rq.repeat import Repeat
from tests import RQTestCase
from tests.fixtures import say_hello


class TestRepeat(RQTestCase):
    """Tests for the Repeat class"""

    def test_repeat_class_initialization(self):
        """Repeat correctly parses `times` and `interval` parameters"""
        # Test with single interval value
        repeat = Repeat(times=3, interval=5)
        self.assertEqual(repeat.times, 3)
        self.assertEqual(repeat.intervals, [5])

        # Test with list of intervals
        repeat = Repeat(times=4, interval=[5, 10, 15])
        self.assertEqual(repeat.times, 4)
        self.assertEqual(repeat.intervals, [5, 10, 15])

        # Test validation errors
        # times must be at least 1
        self.assertRaises(ValueError, Repeat, times=0, interval=5)
        self.assertRaises(ValueError, Repeat, times=-1, interval=5)

        # interval can't be negative
        self.assertRaises(ValueError, Repeat, times=1, interval=-5)
        self.assertRaises(ValueError, Repeat, times=3, interval=[5, -10])

        # interval must be int or iterable
        self.assertRaises(TypeError, Repeat, times=3, interval="not_a_number")

    def test_get_interval(self):
        """get_interval() returns the right repeat interval"""
        # Test with intervals list shorter than needed
        intervals = [5, 10, 15]

        # First interval
        self.assertEqual(Repeat.get_interval(0, intervals), 5)

        # Second interval
        self.assertEqual(Repeat.get_interval(1, intervals), 10)

        # Third interval
        self.assertEqual(Repeat.get_interval(2, intervals), 15)

        # Beyond the list length, should return the last interval
        self.assertEqual(Repeat.get_interval(3, intervals), 15)

        # Test with single interval
        intervals = [7]
        self.assertEqual(Repeat.get_interval(0, intervals), 7)
        self.assertEqual(Repeat.get_interval(1, intervals), 7)

        # Test with empty intervals list (edge case that shouldn't happen in practice)
        # This would cause an IndexError in get_interval
        with self.assertRaises(IndexError):
            Repeat.get_interval(0, [])

    def test_persistence_of_repeat_data(self):
        """Repeat related data is stored and restored properly"""
        # Create a job with repeat settings
        job = Job.create(func=say_hello, connection=self.connection)
        job.repeats_left = 3
        job.repeat_intervals = [5, 10, 15]
        job.save()

        # Clear the job's local state and reload from Redis
        job.repeats_left = None
        job.repeat_intervals = None
        job.refresh()

        # Verify the repeat settings were restored correctly
        self.assertEqual(job.repeats_left, 3)
        self.assertEqual(job.repeat_intervals, [5, 10, 15])
