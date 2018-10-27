# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from rq import Queue, Worker
from rq.contrib.sentry import register_sentry
from rq.registry import FailedJobRegistry

from tests import RQTestCase


class FakeSentry(object):
    servers = []

    def captureException(self, *args, **kwds):  # noqa
        pass  # we cannot check this, because worker forks


class TestSentry(RQTestCase):

    def test_work_fails(self):
        """Non importable jobs should be put on the failed queue event with sentry"""
        q = Queue()

        # Action
        job = q.enqueue('_non.importable.job')
        self.assertEqual(q.count, 1)

        w = Worker([q])
        register_sentry(FakeSentry(), w)

        w.work(burst=True)

        # Postconditions
        registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in registry)
        self.assertEqual(q.count, 0)
