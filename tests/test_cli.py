# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from click.testing import CliRunner
from rq import get_failed_queue
from rq.compat import is_python_version
from rq.job import Job
from rq.cli import main
from rq.scripts import read_config_file

from tests import RQTestCase
from tests.fixtures import div_by_zero

if is_python_version((2, 7), (3, 2)):
    from unittest import TestCase
else:
    from unittest2 import TestCase  # noqa


class TestCommandLine(TestCase):
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
        result = runner.invoke(main, ['empty', '-u', self.redis_url, 'failed'])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output.strip(), '1 jobs removed from failed queue')

    def test_requeue(self):
        """rq -u <url> requeue"""
        runner = CliRunner()
        result = runner.invoke(main, ['requeue', '-u', self.redis_url, '--all'])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output.strip(), 'Requeueing 1 jobs from failed queue')

    def test_info(self):
        """rq -u <url> info -i 0"""
        runner = CliRunner()
        result = runner.invoke(main, ['info', '-u', self.redis_url])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('1 queues, 1 jobs total', result.output)
