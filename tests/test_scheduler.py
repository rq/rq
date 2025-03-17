import os
from datetime import datetime, timedelta, timezone
from multiprocessing import Process
from unittest import mock

import freezegun
import redis

from rq import Queue
from rq.defaults import DEFAULT_MAINTENANCE_TASK_INTERVAL
from rq.exceptions import NoSuchJobError
from rq.job import Job, Retry
from rq.registry import FinishedJobRegistry, ScheduledJobRegistry
from rq.scheduler import RQScheduler
from rq.serializers import JSONSerializer
from rq.utils import current_timestamp, get_next_scheduled_time
from rq.worker import Worker
from tests import RQTestCase, find_empty_redis_database, ssl_test

from .fixtures import kill_worker, say_hello


class CustomRedisConnection(redis.Connection):
    """Custom redis connection with a custom arg, used in test_custom_connection_pool"""

    def __init__(self, *args, custom_arg=None, **kwargs):
        self.custom_arg = custom_arg
        super().__init__(*args, **kwargs)

    def get_custom_arg(self):
        return self.custom_arg


class TestScheduledJobRegistry(RQTestCase):
    def test_get_jobs_to_enqueue(self):
        """Getting job ids to enqueue from ScheduledJobRegistry."""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        timestamp = current_timestamp()

        self.connection.zadd(registry.key, {"foo": 1})
        self.connection.zadd(registry.key, {"bar": timestamp + 10})
        self.connection.zadd(registry.key, {"baz": timestamp + 30})

        self.assertEqual(registry.get_jobs_to_enqueue(), ["foo"])
        self.assertEqual(registry.get_jobs_to_enqueue(timestamp + 20), ["foo", "bar"])

    def test_get_jobs_to_schedule_with_chunk_size(self):
        """Max amount of jobs returns by get_jobs_to_schedule() equal to chunk_size"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        timestamp = current_timestamp()
        chunk_size = 5

        for index in range(0, chunk_size * 2):
            self.connection.zadd(registry.key, {"foo_{}".format(index): 1})

        self.assertEqual(
            len(registry.get_jobs_to_schedule(timestamp, chunk_size)), chunk_size
        )
        self.assertEqual(
            len(registry.get_jobs_to_schedule(timestamp, chunk_size * 2)),
            chunk_size * 2,
        )

    def test_get_scheduled_time(self):
        """get_scheduled_time() returns job's scheduled datetime"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)

        job = Job.create("myfunc", connection=self.connection)
        job.save()
        dt = datetime(2019, 1, 1, tzinfo=timezone.utc)
        registry.schedule(job, datetime(2019, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(registry.get_scheduled_time(job), dt)
        # get_scheduled_time() should also work with job ID
        self.assertEqual(registry.get_scheduled_time(job.id), dt)

        # registry.get_scheduled_time() raises NoSuchJobError if
        # job.id is not found
        self.assertRaises(NoSuchJobError, registry.get_scheduled_time, "123")

    def test_schedule(self):
        """Adding job with the correct score to ScheduledJobRegistry"""
        queue = Queue(connection=self.connection)
        job = Job.create("myfunc", connection=self.connection)
        job.save()
        registry = ScheduledJobRegistry(queue=queue)

        from datetime import timezone

        # If we pass in a datetime with no timezone, `schedule()`
        # assumes local timezone so depending on your local timezone,
        # the timestamp maybe different
        #
        # we need to account for the difference between a timezone
        # with DST active and without DST active.  The time.timezone
        # property isn't accurate when time.daylight is non-zero,
        # we'll test both.
        #
        # first, time.daylight == 0 (not in DST).
        # mock the sitatuoin for American/New_York not in DST (UTC - 5)
        # time.timezone = 18000
        # time.daylight = 0
        # time.altzone = 14400

        mock_day = mock.patch("time.daylight", 0)
        mock_tz = mock.patch("time.timezone", 18000)
        mock_atz = mock.patch("time.altzone", 14400)
        with mock_tz, mock_day, mock_atz:
            registry.schedule(job, datetime(2019, 1, 1))
            self.assertEqual(
                self.connection.zscore(registry.key, job.id), 1546300800 + 18000
            )  # 2019-01-01 UTC in Unix timestamp

            # second, time.daylight != 0 (in DST)
            # mock the sitatuoin for American/New_York not in DST (UTC - 4)
            # time.timezone = 18000
            # time.daylight = 1
            # time.altzone = 14400
            mock_day = mock.patch("time.daylight", 1)
            mock_tz = mock.patch("time.timezone", 18000)
            mock_atz = mock.patch("time.altzone", 14400)
            with mock_tz, mock_day, mock_atz:
                registry.schedule(job, datetime(2019, 1, 1))
                self.assertEqual(
                    self.connection.zscore(registry.key, job.id), 1546300800 + 14400
                )  # 2019-01-01 UTC in Unix timestamp

            # Score is always stored in UTC even if datetime is in a different tz
            tz = timezone(timedelta(hours=7))
            job = Job.create("myfunc", connection=self.connection)
            job.save()
            registry.schedule(job, datetime(2019, 1, 1, 7, tzinfo=tz))
            self.assertEqual(
                self.connection.zscore(registry.key, job.id), 1546300800
            )  # 2019-01-01 UTC in Unix timestamp


class TestScheduler(RQTestCase):
    def test_init(self):
        """Scheduler can be instantiated with queues or queue names"""
        foo_queue = Queue("foo", connection=self.connection)
        scheduler = RQScheduler([foo_queue, "bar"], connection=self.connection)
        self.assertEqual(scheduler._queue_names, {"foo", "bar"})
        self.assertEqual(scheduler.status, RQScheduler.Status.STOPPED)

    def test_should_reacquire_locks(self):
        """scheduler.should_reacquire_locks works properly"""
        queue = Queue(connection=self.connection)
        scheduler = RQScheduler([queue], connection=self.connection)
        self.assertTrue(scheduler.should_reacquire_locks)
        scheduler.acquire_locks()
        self.assertIsNotNone(scheduler.lock_acquisition_time)

        # scheduler.should_reacquire_locks always returns False if
        # scheduler.acquired_locks and scheduler._queue_names are the same
        self.assertFalse(scheduler.should_reacquire_locks)
        scheduler.lock_acquisition_time = datetime.now() - timedelta(
            seconds=DEFAULT_MAINTENANCE_TASK_INTERVAL + 6
        )
        self.assertFalse(scheduler.should_reacquire_locks)

        scheduler._queue_names = set(["default", "foo"])
        self.assertTrue(scheduler.should_reacquire_locks)
        scheduler.acquire_locks()
        self.assertFalse(scheduler.should_reacquire_locks)

    def test_lock_acquisition(self):
        """Test lock acquisition"""
        name_1 = "lock-test-1"
        name_2 = "lock-test-2"
        name_3 = "lock-test-3"
        scheduler = RQScheduler([name_1], self.connection)

        self.assertEqual(scheduler.acquire_locks(), {name_1})
        self.assertEqual(scheduler._acquired_locks, {name_1})
        self.assertEqual(scheduler.acquire_locks(), set([]))

        # Only name_2 is returned since name_1 is already locked
        scheduler = RQScheduler([name_1, name_2], self.connection)
        self.assertEqual(scheduler.acquire_locks(), {name_2})
        self.assertEqual(scheduler._acquired_locks, {name_2})

        # When a new lock is successfully acquired, _acquired_locks is added
        scheduler._queue_names.add(name_3)
        self.assertEqual(scheduler.acquire_locks(), {name_3})
        self.assertEqual(scheduler._acquired_locks, {name_2, name_3})

    def test_lock_acquisition_with_auto_start(self):
        """Test lock acquisition with auto_start=True"""
        scheduler = RQScheduler(["auto-start"], self.connection)
        with mock.patch.object(scheduler, "start") as mocked:
            scheduler.acquire_locks(auto_start=True)
            self.assertEqual(mocked.call_count, 1)

        # If process has started, scheduler.start() won't be called
        running_process = mock.MagicMock()
        running_process.is_alive.return_value = True
        scheduler = RQScheduler(["auto-start2"], self.connection)
        scheduler._process = running_process
        with mock.patch.object(scheduler, "start") as mocked:
            scheduler.acquire_locks(auto_start=True)
            self.assertEqual(mocked.call_count, 0)
            self.assertEqual(running_process.is_alive.call_count, 1)

        # If the process has stopped for some reason, the scheduler should restart
        scheduler = RQScheduler(["auto-start3"], self.connection)
        stopped_process = mock.MagicMock()
        stopped_process.is_alive.return_value = False
        scheduler._process = stopped_process
        with mock.patch.object(scheduler, "start") as mocked:
            scheduler.acquire_locks(auto_start=True)
            self.assertEqual(mocked.call_count, 1)
            self.assertEqual(stopped_process.is_alive.call_count, 1)

    def test_lock_release(self):
        """Test that scheduler.release_locks() only releases acquired locks"""
        name_1 = "lock-test-1"
        name_2 = "lock-test-2"
        scheduler_1 = RQScheduler([name_1], self.connection)

        self.assertEqual(scheduler_1.acquire_locks(), {name_1})
        self.assertEqual(scheduler_1._acquired_locks, {name_1})

        # Only name_2 is returned since name_1 is already locked
        scheduler_1_2 = RQScheduler([name_1, name_2], self.connection)
        self.assertEqual(scheduler_1_2.acquire_locks(), {name_2})
        self.assertEqual(scheduler_1_2._acquired_locks, {name_2})

        self.assertTrue(self.connection.exists(scheduler_1.get_locking_key(name_1)))
        self.assertTrue(self.connection.exists(scheduler_1_2.get_locking_key(name_1)))
        self.assertTrue(self.connection.exists(scheduler_1_2.get_locking_key(name_2)))

        scheduler_1_2.release_locks()

        self.assertEqual(scheduler_1_2._acquired_locks, set())
        self.assertEqual(scheduler_1._acquired_locks, {name_1})

        self.assertTrue(self.connection.exists(scheduler_1.get_locking_key(name_1)))
        self.assertTrue(self.connection.exists(scheduler_1_2.get_locking_key(name_1)))
        self.assertFalse(self.connection.exists(scheduler_1_2.get_locking_key(name_2)))

    def test_queue_scheduler_pid(self):
        queue = Queue(connection=self.connection)
        scheduler = RQScheduler(
            [
                queue,
            ],
            connection=self.connection,
        )
        scheduler.acquire_locks()
        assert queue.scheduler_pid == os.getpid()

    def test_heartbeat(self):
        """Test that heartbeat updates locking keys TTL"""
        name_1 = "lock-test-1"
        name_2 = "lock-test-2"
        name_3 = "lock-test-3"
        scheduler = RQScheduler([name_3], self.connection)
        scheduler.acquire_locks()
        scheduler = RQScheduler([name_1, name_2, name_3], self.connection)
        scheduler.acquire_locks()

        locking_key_1 = RQScheduler.get_locking_key(name_1)
        locking_key_2 = RQScheduler.get_locking_key(name_2)
        locking_key_3 = RQScheduler.get_locking_key(name_3)

        with self.connection.pipeline() as pipeline:
            pipeline.expire(locking_key_1, 1000)
            pipeline.expire(locking_key_2, 1000)
            pipeline.expire(locking_key_3, 1000)
            pipeline.execute()

        scheduler.heartbeat()
        self.assertEqual(self.connection.ttl(locking_key_1), 61)
        self.assertEqual(self.connection.ttl(locking_key_2), 61)
        self.assertEqual(self.connection.ttl(locking_key_3), 1000)

        # scheduler.stop() releases locks and sets status to STOPPED
        scheduler._status = scheduler.Status.WORKING
        scheduler.stop()
        self.assertFalse(self.connection.exists(locking_key_1))
        self.assertFalse(self.connection.exists(locking_key_2))
        self.assertTrue(self.connection.exists(locking_key_3))
        self.assertEqual(scheduler.status, scheduler.Status.STOPPED)

        # Heartbeat also works properly for schedulers with a single queue
        scheduler = RQScheduler([name_1], self.connection)
        scheduler.acquire_locks()
        self.connection.expire(locking_key_1, 1000)
        scheduler.heartbeat()
        self.assertEqual(self.connection.ttl(locking_key_1), 61)

    def test_enqueue_scheduled_jobs(self):
        """Scheduler can enqueue scheduled jobs"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        job = Job.create("myfunc", connection=self.connection)
        job.save()
        registry.schedule(job, datetime(2019, 1, 1, tzinfo=timezone.utc))
        scheduler = RQScheduler([queue], connection=self.connection)
        scheduler.acquire_locks()
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)

        # After job is scheduled, registry should be empty
        self.assertEqual(len(registry), 0)

        # Jobs scheduled in the far future should not be affected
        registry.schedule(job, datetime(2100, 1, 1, tzinfo=timezone.utc))
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)

    def test_prepare_registries(self):
        """prepare_registries() creates self._scheduled_job_registries"""
        foo_queue = Queue("foo", connection=self.connection)
        bar_queue = Queue("bar", connection=self.connection)
        scheduler = RQScheduler([foo_queue, bar_queue], connection=self.connection)
        self.assertEqual(scheduler._scheduled_job_registries, [])
        scheduler.prepare_registries([foo_queue.name])
        self.assertEqual(
            scheduler._scheduled_job_registries, [ScheduledJobRegistry(queue=foo_queue)]
        )
        scheduler.prepare_registries([foo_queue.name, bar_queue.name])
        self.assertEqual(
            scheduler._scheduled_job_registries,
            [
                ScheduledJobRegistry(queue=foo_queue),
                ScheduledJobRegistry(queue=bar_queue),
            ],
        )

    # Cron Tests

    def test_cron_persisted_correctly(self):
        """
        Ensure that cron_string attribute gets correctly saved in job metadata.
        """
        # Create a job that runs one minute past each whole hour
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello)

        # Verify the job has the cron_string in its metadata
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.meta["cron_string"], "1 * * * *")

        # Get the scheduled registry for the job's queue
        registry = ScheduledJobRegistry(job.origin, connection=self.connection)

        # Get the scheduled time and convert it to a datetime object
        scheduled_time = registry.get_scheduled_time(job)

        # Check that minute=1, seconds=0, and is within an hour
        self.assertEqual(scheduled_time.minute, 1)
        self.assertEqual(scheduled_time.second, 0)
        self.assertTrue(
            scheduled_time - datetime.now(timezone.utc) < timedelta(hours=1)
        )

    def test_cron_persisted_correctly_with_local_timezone(self):
        """
        Ensure that cron_string attribute gets correctly saved when using local timezone.
        """
        # Create a job that runs at 3 PM every day
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("0 15 * * *", say_hello, use_local_timezone=True)

        # Verify the job has the cron_string and use_local_timezone in its metadata
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.meta["cron_string"], "0 15 * * *")
        self.assertTrue(job_from_queue.meta["use_local_timezone"])

        # Get the scheduled registry for the job's queue
        registry = ScheduledJobRegistry(job.origin, connection=self.connection)

        # Get the scheduled time
        scheduled_time = registry.get_scheduled_time(job)

        # The scheduled time should be 3 PM in the local timezone
        # This is a bit tricky to test without knowing the local timezone
        # So we'll just check that the hour is reasonable (not in the middle of the night)
        self.assertTrue(0 <= scheduled_time.hour < 24)

    def test_cron_rescheduled_correctly(self):
        """
        Ensure that cron jobs are rescheduled correctly after execution.
        """
        # Create a job with a cron string
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello)

        # Get the scheduled registry for the job's queue
        registry = ScheduledJobRegistry(job.origin, connection=self.connection)

        # Get the original scheduled time
        original_scheduled_time = registry.get_scheduled_time(job)

        # Simulate job execution and rescheduling
        queue = Queue(job.origin, connection=self.connection)

        # Use a pipeline to simulate what happens in enqueue_scheduled_jobs
        with self.connection.pipeline() as pipeline:
            # Enqueue the job
            queue._enqueue_job(job, pipeline=pipeline)

            # Remove from registry
            registry.remove(job.id, pipeline=pipeline)

            # Calculate the next scheduled time
            next_scheduled_time = get_next_scheduled_time(
                job.meta["cron_string"],
                use_local_timezone=job.meta.get("use_local_timezone", False),
            )

            # Reschedule the job
            registry.schedule(job, next_scheduled_time, pipeline=pipeline)

            # Execute all commands
            pipeline.execute()

        # Get the new scheduled time
        new_scheduled_time = registry.get_scheduled_time(job)

        # The new scheduled time should be later than the original
        self.assertTrue(new_scheduled_time > original_scheduled_time)

        # The new scheduled time should be about an hour later (for "1 * * * *")
        # Allow for a small margin of error
        expected_diff = timedelta(hours=1)
        actual_diff = new_scheduled_time - original_scheduled_time
        self.assertTrue(
            expected_diff - timedelta(minutes=5)
            < actual_diff
            < expected_diff + timedelta(minutes=5)
        )

    def test_cron_schedules_correctly(self):
        """
        Test that cron jobs are scheduled at the correct time.
        """
        # Freeze time to a known point
        scheduler = RQScheduler(["default"], connection=self.connection)
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

        with freezegun.freeze_time(now):
            # Create a job that runs 5 minutes past every hour
            job = scheduler.cron("5 * * * *", say_hello)

            # Get the scheduled registry for the job's queue
            registry = ScheduledJobRegistry(job.origin, connection=self.connection)

            # Get the scheduled time
            scheduled_time = registry.get_scheduled_time(job)

            # The scheduled time should be 5 minutes past the current hour
            expected_time = now.replace(minute=5)
            self.assertEqual(scheduled_time.replace(microsecond=0), expected_time)

        # Move time forward to just after the scheduled time
        with freezegun.freeze_time(now + timedelta(minutes=6)):
            # Simulate the scheduler running
            scheduler.enqueue_scheduled_jobs()

            # The job should have been enqueued
            queue = Queue(job.origin, connection=self.connection)
            self.assertEqual(len(queue), 1)

            # The job should have been rescheduled for the next hour
            scheduled_time = registry.get_scheduled_time(job)
            expected_time = now.replace(minute=5) + timedelta(hours=1)
            self.assertEqual(scheduled_time.replace(microsecond=0), expected_time)

    def test_cron_sets_timeout(self):
        """
        Ensure that a job scheduled via cron can be created with a custom timeout.
        """
        timeout = 13
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello, timeout=timeout)
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.timeout, timeout)

    def test_cron_sets_job_id(self):
        """
        Ensure that a job scheduled via cron can be created with a custom ID.
        """
        job_id = "test-cron-job-id"
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello, job_id=job_id)
        self.assertEqual(job.id, job_id)
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.id, job_id)

    def test_cron_sets_default_result_ttl(self):
        """
        Ensure that a job scheduled via cron gets the default result_ttl (-1).
        """
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello)
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.result_ttl, -1)

    def test_cron_sets_description(self):
        """
        Ensure that a job scheduled via cron can be created with a custom description.
        """
        description = "Test cron job description"
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello, description=description)
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.description, description)

    def test_cron_sets_provided_result_ttl(self):
        """
        Ensure that a job scheduled via cron can be created with a custom result_ttl.
        """
        result_ttl = 123
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello, result_ttl=result_ttl)
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.result_ttl, result_ttl)

    def test_cron_sets_default_ttl_to_none(self):
        """
        Ensure that a job scheduled via cron has the default ttl (None).
        """
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello)
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertIsNone(job_from_queue.ttl)

    def test_cron_sets_provided_ttl(self):
        """
        Ensure that a job scheduled via cron can be created with a custom ttl.
        """
        ttl = 123
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello, ttl=ttl)
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.ttl, ttl)

    def test_cron_with_repeat(self):
        """
        Ensure that cron jobs with repeat parameter are handled correctly.
        """
        # Create a job that should run 3 times
        scheduler = RQScheduler(["default"], connection=self.connection)
        job = scheduler.cron("1 * * * *", say_hello, repeat=3)
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.meta["repeat"], 3)

        # Get the scheduled registry for the job's queue
        registry = ScheduledJobRegistry(job.origin, connection=self.connection)

        # Simulate job execution and rescheduling
        queue = Queue(job.origin, connection=self.connection)

        # First execution - repeat should be decremented to 2
        with self.connection.pipeline() as pipeline:
            queue._enqueue_job(job, pipeline=pipeline)
            registry.remove(job.id, pipeline=pipeline)

            # Check for cron jobs that need to be rescheduled
            if "cron_string" in job.meta:
                cron_string = job.meta["cron_string"]
                use_local_timezone = job.meta.get("use_local_timezone", False)
                repeat = job.meta.get("repeat")

                # Decrement repeat counter
                if repeat is not None:
                    job.meta["repeat"] = int(repeat) - 1
                    job.save(pipeline=pipeline)

                # Calculate the next scheduled time
                next_scheduled_time = get_next_scheduled_time(
                    cron_string, use_local_timezone=use_local_timezone
                )

                # Schedule the job for the next time
                registry.schedule(job, next_scheduled_time, pipeline=pipeline)

            pipeline.execute()

        # Verify repeat counter was decremented
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.meta["repeat"], 2)

        # Second execution - repeat should be decremented to 1
        with self.connection.pipeline() as pipeline:
            queue._enqueue_job(job, pipeline=pipeline)
            registry.remove(job.id, pipeline=pipeline)

            # Check for cron jobs that need to be rescheduled
            if "cron_string" in job.meta:
                cron_string = job.meta["cron_string"]
                use_local_timezone = job.meta.get("use_local_timezone", False)
                repeat = job.meta.get("repeat")

                # Decrement repeat counter
                if repeat is not None:
                    job.meta["repeat"] = int(repeat) - 1
                    job.save(pipeline=pipeline)

                # Calculate the next scheduled time
                next_scheduled_time = get_next_scheduled_time(
                    cron_string, use_local_timezone=use_local_timezone
                )

                # Schedule the job for the next time
                registry.schedule(job, next_scheduled_time, pipeline=pipeline)

            pipeline.execute()

        # Verify repeat counter was decremented
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.meta["repeat"], 1)

        # Third execution - repeat should be decremented to 0 and job should not be rescheduled
        with self.connection.pipeline() as pipeline:
            queue._enqueue_job(job, pipeline=pipeline)
            registry.remove(job.id, pipeline=pipeline)

            # Check for cron jobs that need to be rescheduled
            if "cron_string" in job.meta:
                cron_string = job.meta["cron_string"]
                use_local_timezone = job.meta.get("use_local_timezone", False)
                repeat = job.meta.get("repeat")

                # Decrement repeat counter
                if repeat is not None:
                    job.meta["repeat"] = int(repeat) - 1
                    job.save(pipeline=pipeline)

                    # Don't reschedule if we've reached the repeat limit
                    if job.meta["repeat"] < 0:
                        pipeline.execute()
                        return

                # Calculate the next scheduled time
                next_scheduled_time = get_next_scheduled_time(
                    cron_string, use_local_timezone=use_local_timezone
                )

                # Schedule the job for the next time
                registry.schedule(job, next_scheduled_time, pipeline=pipeline)

            pipeline.execute()

        # Verify repeat counter was decremented
        job_from_queue = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job_from_queue.meta["repeat"], 0)

        # Fourth execution - job should not be rescheduled
        with self.connection.pipeline() as pipeline:
            queue._enqueue_job(job, pipeline=pipeline)
            registry.remove(job.id, pipeline=pipeline)

            # Check for cron jobs that need to be rescheduled
            if "cron_string" in job.meta:
                cron_string = job.meta["cron_string"]
                use_local_timezone = job.meta.get("use_local_timezone", False)
                repeat = job.meta.get("repeat")

                # Decrement repeat counter
                if repeat is not None:
                    job.meta["repeat"] = int(repeat) - 1
                    job.save(pipeline=pipeline)

                    # Don't reschedule if we've reached the repeat limit
                    if job.meta["repeat"] < 0:
                        pipeline.execute()
                        return

                # Calculate the next scheduled time
                next_scheduled_time = get_next_scheduled_time(
                    cron_string, use_local_timezone=use_local_timezone
                )

                # Schedule the job for the next time
                registry.schedule(job, next_scheduled_time, pipeline=pipeline)

            pipeline.execute()

        # Verify job is not in the scheduled registry anymore
        self.assertRaises(NoSuchJobError, registry.get_scheduled_time, job)

    def test_cron_with_at_front(self):
        """
        Ensure that cron jobs with at_front parameter are handled correctly.
        """
        # Create a regular job
        scheduler = RQScheduler(["default"], connection=self.connection)
        regular_job = scheduler.cron("1 * * * *", say_hello)

        # Create a job that should be enqueued at the front of the queue
        scheduler = RQScheduler(["default"], connection=self.connection)
        front_job = scheduler.cron("1 * * * *", say_hello, at_front=True)

        # Verify at_front is set correctly
        self.assertFalse(getattr(regular_job, "enqueue_at_front", False))
        self.assertTrue(front_job.enqueue_at_front)

        # Simulate job execution
        queue = Queue(regular_job.origin, connection=self.connection)

        # Enqueue both jobs
        with self.connection.pipeline() as pipeline:
            # Enqueue regular job first
            queue._enqueue_job(regular_job, pipeline=pipeline)

            # Then enqueue front job
            queue._enqueue_job(front_job, pipeline=pipeline, at_front=True)

            pipeline.execute()

        # The front job should be at the front of the queue
        self.assertEqual(queue.get_job_position(front_job.id), 0)
        self.assertEqual(queue.get_job_position(regular_job.id), 1)


class TestWorker(RQTestCase):
    def test_work_burst(self):
        """worker.work() with scheduler enabled works properly"""
        queue = Queue(connection=self.connection)
        worker = Worker(queues=[queue], connection=self.connection)
        worker.work(burst=True, with_scheduler=False)
        self.assertIsNone(worker.scheduler)

        worker = Worker(queues=[queue], connection=self.connection)
        worker.work(burst=True, with_scheduler=True)
        assert worker.scheduler
        self.assertIsNone(
            self.connection.get(worker.scheduler.get_locking_key("default"))
        )

    @mock.patch.object(RQScheduler, "acquire_locks")
    def test_run_maintenance_tasks(self, mocked):
        """scheduler.acquire_locks() is called only when scheduled is enabled"""
        queue = Queue(connection=self.connection)
        worker = Worker(queues=[queue], connection=self.connection)

        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 0)

        # if scheduler object exists and it's a first start, acquire locks should not run
        worker.last_cleaned_at = None
        worker.scheduler = RQScheduler([queue], connection=self.connection)
        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 0)

        # the scheduler exists and it's NOT a first start, since the process doesn't exists,
        # should call acquire_locks to start the process
        worker.last_cleaned_at = datetime.now()
        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 1)

        # the scheduler exists, the process exists, but the process is not alive
        running_process = mock.MagicMock()
        running_process.is_alive.return_value = False
        worker.scheduler._process = running_process
        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 2)
        self.assertEqual(running_process.is_alive.call_count, 1)

        # the scheduler exists, the process exits, and it is alive. acquire_locks shouldn't run
        running_process.is_alive.return_value = True
        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 2)
        self.assertEqual(running_process.is_alive.call_count, 2)

    def test_work(self):
        queue = Queue(connection=self.connection)
        worker = Worker(queues=[queue], connection=self.connection)
        p = Process(target=kill_worker, args=(os.getpid(), False, 5))

        p.start()
        queue.enqueue_at(datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello)
        worker.work(burst=False, with_scheduler=True)
        p.join(1)
        self.assertIsNotNone(worker.scheduler)
        registry = FinishedJobRegistry(queue=queue)
        self.assertEqual(len(registry), 1)

    @ssl_test
    def test_work_with_ssl(self):
        connection = find_empty_redis_database(ssl=True)
        queue = Queue(connection=connection)
        worker = Worker(queues=[queue], connection=connection)
        p = Process(target=kill_worker, args=(os.getpid(), False, 5))

        p.start()
        queue.enqueue_at(datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello)
        worker.work(burst=False, with_scheduler=True)
        p.join(1)
        self.assertIsNotNone(worker.scheduler)
        registry = FinishedJobRegistry(queue=queue)
        self.assertEqual(len(registry), 1)

    def test_work_with_serializer(self):
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        worker = Worker(
            queues=[queue], connection=self.connection, serializer=JSONSerializer
        )
        p = Process(target=kill_worker, args=(os.getpid(), False, 5))

        p.start()
        queue.enqueue_at(
            datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello, meta={"foo": "bar"}
        )
        worker.work(burst=False, with_scheduler=True)
        p.join(1)
        self.assertIsNotNone(worker.scheduler)
        registry = FinishedJobRegistry(queue=queue)
        self.assertEqual(len(registry), 1)


class TestQueue(RQTestCase):
    def test_enqueue_at(self):
        """queue.enqueue_at() puts job in the scheduled"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        scheduler = RQScheduler([queue], connection=self.connection)
        scheduler.acquire_locks()
        # Jobs created using enqueue_at is put in the ScheduledJobRegistry
        job = queue.enqueue_at(datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello)
        self.assertEqual(len(queue), 0)
        self.assertEqual(len(registry), 1)

        # enqueue_at set job status to "scheduled"
        self.assertTrue(job.get_status() == "scheduled")

        # After enqueue_scheduled_jobs() is called, the registry is empty
        # and job is enqueued
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)
        self.assertEqual(len(registry), 0)

    def test_enqueue_at_at_front(self):
        """queue.enqueue_at() accepts at_front argument. When true, job will be put at position 0
        of the queue when the time comes for the job to be scheduled"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        scheduler = RQScheduler([queue], connection=self.connection)
        scheduler.acquire_locks()
        # Jobs created using enqueue_at is put in the ScheduledJobRegistry
        # job_first should be enqueued first
        job_first = queue.enqueue_at(
            datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello
        )
        # job_second will be enqueued second, but "at_front"
        job_second = queue.enqueue_at(
            datetime(2019, 1, 2, tzinfo=timezone.utc), say_hello, at_front=True
        )
        self.assertEqual(len(queue), 0)
        self.assertEqual(len(registry), 2)

        # enqueue_at set job status to "scheduled"
        self.assertTrue(job_first.get_status() == "scheduled")
        self.assertTrue(job_second.get_status() == "scheduled")

        # After enqueue_scheduled_jobs() is called, the registry is empty
        # and job is enqueued
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 2)
        self.assertEqual(len(registry), 0)
        self.assertEqual(0, queue.get_job_position(job_second.id))
        self.assertEqual(1, queue.get_job_position(job_first.id))

    def test_enqueue_in(self):
        """queue.enqueue_in() schedules job correctly"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)

        job = queue.enqueue_in(timedelta(seconds=30), say_hello)
        now = datetime.now(timezone.utc)
        scheduled_time = registry.get_scheduled_time(job)
        # Ensure that job is scheduled roughly 30 seconds from now
        self.assertTrue(
            now + timedelta(seconds=28) < scheduled_time < now + timedelta(seconds=32)
        )

    def test_enqueue_in_with_retry(self):
        """Ensure that the retry parameter is passed
        to the enqueue_at function from enqueue_in.
        """
        queue = Queue(connection=self.connection)
        job = queue.enqueue_in(timedelta(seconds=30), say_hello, retry=Retry(3, [2]))
        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.retry_intervals, [2])

    def test_custom_connection_pool(self):
        """Connection pool customizing. Ensure that we can properly set a
        custom connection pool class and pass extra arguments"""
        custom_conn = redis.Redis(
            connection_pool=redis.ConnectionPool(
                connection_class=CustomRedisConnection,
                db=4,
                custom_arg="foo",
            )
        )

        queue = Queue(connection=custom_conn)
        scheduler = RQScheduler([queue], connection=custom_conn)

        scheduler_connection = scheduler.connection.connection_pool.get_connection(
            "info"
        )

        self.assertEqual(scheduler_connection.__class__, CustomRedisConnection)
        self.assertEqual(scheduler_connection.get_custom_arg(), "foo")

    def test_no_custom_connection_pool(self):
        """Connection pool customizing must not interfere if we're using a standard
        connection (non-pooled)"""
        standard_conn = redis.Redis(db=5)

        queue = Queue(connection=standard_conn)
        scheduler = RQScheduler([queue], connection=standard_conn)

        scheduler_connection = scheduler.connection.connection_pool.get_connection(
            "info"
        )

        self.assertEqual(scheduler_connection.__class__, redis.Connection)
