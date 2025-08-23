from rq import cron_scheduler_registry
from rq.cron import CronScheduler
from rq.exceptions import DuplicateSchedulerError, SchedulerNotFound
from tests import RQTestCase


class TestCronSchedulerRegistry(RQTestCase):
    """Tests for the cron scheduler registry functions"""

    def setUp(self):
        super().setUp()
        # Clean up registry before each test
        registry_key = cron_scheduler_registry.get_registry_key()
        self.connection.delete(registry_key)

    def tearDown(self):
        # Clean up registry after each test
        registry_key = cron_scheduler_registry.get_registry_key()
        self.connection.delete(registry_key)
        super().tearDown()

    def test_register_scheduler(self):
        """Test registering a single CronScheduler"""
        scheduler = CronScheduler(connection=self.connection, name='scheduler-1')

        cron_scheduler_registry.register(scheduler)
        self.assertEqual(cron_scheduler_registry.get_keys(self.connection), ['scheduler-1'])

        # Test registering a scheduler using a Redis pipeline
        pipeline_scheduler = CronScheduler(connection=self.connection, name='pipeline')

        # Use pipeline for registration
        with self.connection.pipeline() as pipeline:
            cron_scheduler_registry.register(pipeline_scheduler, pipeline)
            pipeline.execute()

        # Verify it's in the registry
        keys = cron_scheduler_registry.get_keys(self.connection)
        self.assertEqual(len(keys), 2)
        self.assertIn(pipeline_scheduler.name, keys)

    def test_unregister_scheduler(self):
        """Test unregistering a CronScheduler"""
        scheduler1 = CronScheduler(connection=self.connection, name='test-scheduler-1')
        scheduler2 = CronScheduler(connection=self.connection, name='test-scheduler-2')

        # Register both schedulers
        cron_scheduler_registry.register(scheduler1)
        cron_scheduler_registry.register(scheduler2)

        # Verify both are registered
        self.assertEqual(len(cron_scheduler_registry.get_keys(self.connection)), 2)

        # Unregister one scheduler
        cron_scheduler_registry.unregister(scheduler1)

        # Verify only one remains
        keys = cron_scheduler_registry.get_keys(self.connection)
        self.assertEqual(keys, [scheduler2.name])

        # Unregister using pipeline
        with self.connection.pipeline() as pipeline:
            cron_scheduler_registry.unregister(scheduler2, pipeline)
            pipeline.execute()

        # Verify it's removed
        keys = cron_scheduler_registry.get_keys(self.connection)
        self.assertEqual(keys, [])

        scheduler = CronScheduler(connection=self.connection, name='nonexistent')

        # Unregistering a non existen registry raises SchedulerNotFound
        with self.assertRaises(SchedulerNotFound):
            cron_scheduler_registry.unregister(scheduler)

    def test_register_same_scheduler_twice(self):
        """Test registering the same scheduler twice raises DuplicateSchedulerError"""
        scheduler = CronScheduler(connection=self.connection, name='test-scheduler-duplicate')

        # Register scheduler first time
        cron_scheduler_registry.register(scheduler)

        # Register same scheduler again should raise DuplicateSchedulerError
        with self.assertRaises(DuplicateSchedulerError):
            cron_scheduler_registry.register(scheduler)

        # Should still have only one entry
        keys = cron_scheduler_registry.get_keys(self.connection)
        self.assertEqual(len(keys), 1)
        self.assertIn('test-scheduler-duplicate', keys)
