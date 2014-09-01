# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from click.testing import CliRunner
from rq import get_failed_queue
from rq.compat import is_python_version
from rq.job import Job
from rq.scripts import read_config_file, rq_cli

from tests import RQTestCase
from tests.fixtures import div_by_zero

if is_python_version((2, 7), (3, 2)):
    from unittest import TestCase
else:
    from unittest2 import TestCase  # noqa


class TestScripts(TestCase):
    def test_config_file(self):
        settings = read_config_file("tests.dummy_settings")
        self.assertIn("REDIS_HOST", settings)
        self.assertEqual(settings['REDIS_HOST'], "testhost.example.com")


class TestRQCli(RQTestCase):
    """Test rq_cli script"""
    def setUp(self):
        super(TestRQCli, self).setUp()
        db_num = self.testconn.connection_pool.connection_kwargs['db']
        self.redis_url = 'redis://127.0.0.1:6379/%d' % db_num

        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.save()
        get_failed_queue().quarantine(job, Exception('Some fake error'))  # noqa

    def test_empty(self):
        """rq -u <url> empty -y"""
        runner = CliRunner()
        result = runner.invoke(rq_cli.main,
                ['-u', self.redis_url, 'empty', "-y"])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output, '1 jobs removed from failed queue\n')

    def test_requeue(self):
        """rq -u <url> requeue"""
        runner = CliRunner()
        result = runner.invoke(rq_cli.main, ['-u', self.redis_url, 'requeue', '-a'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('Requeueing 1 jobs from FailedQueue', result.output)
        self.assertIn('Unable to requeue 0 jobs from FailedQueue',
                result.output)

    def test_info(self):
        """rq -u <url> info -i 0"""
        runner = CliRunner()
        result = runner.invoke(rq_cli.main,
                ['-u', self.redis_url, 'info', '-i 0'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('1 queues, 1 jobs total', result.output)
