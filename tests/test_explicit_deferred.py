# -*- coding: utf-8 -*-
"""
=======================
test_explicit_edeferred
=======================

Test the changes of this branch (glenfant-gate_readyness)
"""


from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from tests import RQTestCase
from tests.fixtures import (div_by_zero, echo, Number, say_hello,
                            some_calculation)

from rq import get_failed_queue, Queue
from rq.exceptions import InvalidJobOperationError
from rq.job import Job, JobStatus
from rq.registry import DeferredJobRegistry
from rq.worker import Worker


class TestExplicitDeferredJob(RQTestCase):
    def test_create_deferred_job(self):
        """Creating a deferred Job (expliocit)"""
        job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2), deferred=True)
        self.assertTrue(job.deferred)

    def test_enqueue_deferred(self):
        """Enqueuing a deferred job"""
        q = Queue('example', async=False)
        j = q.enqueue(say_hello, args=('John',), deferred=True)

        # Job is deferred
        self.assertEqual(j.get_status(), JobStatus.DEFERRED)

        # Job is registered in appropriate registry
        registry = DeferredJobRegistry(q.name, connection=self.testconn)
        self.assertEqual(registry.get_job_ids(), [j.id])

        # Job result isn't available though we're using a sync queue
        self.assertIsNone(j.result)

        # Freeing the job
        j.perform()
