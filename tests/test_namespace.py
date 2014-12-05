# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


from rq import namespace
from rq.compat import as_text
from rq.job import Job
from rq.namespace import rq_key, set_rq_key_prefix
from rq.queue import Queue
from tests import RQTestCase


class TestNamespace(RQTestCase):
    def setUp(self):
        super(TestNamespace, self).setUp()
        set_rq_key_prefix(None)

    def tearDown(self):
        set_rq_key_prefix(None)
        super(TestNamespace, self).tearDown()

    def test_default_namespace(self):
        """Never change the default namespace"""
        self.assertEqual("rq:", namespace.KeyNamespace.DEFAULT_PREFIX)

    def test_default_key(self):
        """Keys generated correctly for default namespace"""
        self.assertEqual("rq:foo", rq_key("foo"))
        self.assertEqual("rq:foo:bar", rq_key("foo:bar"))

    def test_custom_key(self):
        """Custom namespace is respected on key creation"""
        set_rq_key_prefix("custom")
        self.assertEqual("custom:rq:foo", rq_key("foo"))
        self.assertEqual("custom:rq:foo:bar", rq_key("foo:bar"))

    def test_custom_reset(self):
        """Custom namespace an be reset"""
        set_rq_key_prefix("custom")
        set_rq_key_prefix(None)
        self.assertEqual("rq:foo", rq_key("foo"))
        self.assertEqual("rq:foo:bar", rq_key("foo:bar"))

    def test_keys(self):
        """Keys for RQ Classes are generated correctly for the namespace"""
        job_key_parts = as_text(Job().key).split(":")
        self.assertEqual("rq", job_key_parts[0])
        queue_key_parts = as_text(Queue().key).split(":")
        self.assertEqual("rq", queue_key_parts[0])

        set_rq_key_prefix("custom")

        job_key_parts = as_text(Job().key).split(":")
        self.assertEqual("custom", job_key_parts[0])
        self.assertEqual("rq", job_key_parts[1])

        queue_key_parts = as_text(Queue().key).split(":")
        self.assertEqual("custom", queue_key_parts[0])
        self.assertEqual("rq", queue_key_parts[1])
