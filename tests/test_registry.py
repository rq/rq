from tests import RQTestCase
from tests.fixtures import Calculator, say_hello

from rq.queue import Queue
from rq.registry import Registry
from rq.scheduler import Scheduler

import times


class TestRegistry(RQTestCase):

    def test_register_unregister(self):
        """Basic sanity checks for Registry."""
        registry = Registry('default', self.testconn)
        queue = Queue('default', self.testconn)
        job = queue.enqueue(say_hello)
        self.assertEqual(registry.get_key_from_job(job),
                         'rq:registry:default:tests.fixtures.say_hello')
        registry.register(job)
        self.assertTrue(registry.connection.sismember(registry.get_key_from_job(job), job.id))
        registry.unregister(job)
        self.assertFalse(registry.connection.sismember(registry.get_key_from_job(job), job.id))

    def test_is_registered(self):
        """is_registered returns True when a job with callable is registered"""
        registry = Registry('default', self.testconn)
        self.assertFalse(registry.is_registered(say_hello))
        queue = Queue('default', self.testconn)
        job = queue.enqueue(say_hello)
        registry.register(job)
        self.assertTrue(registry.is_registered(say_hello))

    def test_scheduler_registry(self):
        """ Ensure that scheduler properly registers and unregisters jobs"""
        registry = Registry('scheduler', self.testconn)
        scheduler = Scheduler('default', self.testconn)
        job = scheduler.enqueue_at(times.now(), say_hello)
        self.assertTrue(registry.is_registered(say_hello))

    def test_key_generation(self):
        """Ensure proper keys are generated for both functions and methods"""
        registry = Registry('default', self.testconn)
        queue = Queue('default', self.testconn)
        
        job = queue.enqueue(say_hello)
        self.assertEqual(registry.get_key_from_job(job),
                         'rq:registry:default:tests.fixtures.say_hello')
        self.assertEqual(registry.get_key_from_callable(say_hello),
                         'rq:registry:default:tests.fixtures.say_hello')
        
        c = Calculator(2)
        job = queue.enqueue(c.calculate, 3, 4)
        self.assertEqual(registry.get_key_from_job(job),
                         'rq:registry:default:tests.fixtures.Calculator.calculate')
        self.assertEqual(registry.get_key_from_callable(c.calculate),
                         'rq:registry:default:tests.fixtures.Calculator.calculate')

        
