# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys
from rq import get_failed_queue, Queue
from rq.contrib.sentry import register_sentry

from tests import RQTestCase

if sys.platform == 'win32':
    from rq.worker import WindowsWorker as Worker
else:
    from rq.worker import Worker


class FakeSentry(object):
    servers = []

    def captureException(self, *args, **kwds):  # noqa
        pass  # we cannot check this, because worker forks


class TestSentry(RQTestCase):

    def test_work_fails(self):
        """Non importable jobs should be put on the failed queue event with sentry"""
        q = Queue()
        failed_q = get_failed_queue()

        # Action
        q.enqueue('_non.importable.job')
        self.assertEqual(q.count, 1)

        w = Worker([q])
        register_sentry(FakeSentry(), w)

        w.work(burst=True)

        # Postconditions
        self.assertEqual(failed_q.count, 1)
        self.assertEqual(q.count, 0)
