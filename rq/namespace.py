# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from .compat import as_text


class KeyNamespace(object):
    DEFAULT_PREFIX = "rq:"

    def __init__(self, prefix=None):
        self._prefix = self._generate_prefix(prefix)

    @property
    def prefix(self):
        return self._prefix

    def generate_key(self, key):
        return self.prefix + key

    def set_prefix(self, prefix):
        self._prefix = self._generate_prefix(prefix)

    @classmethod
    def _generate_prefix(cls, prefix):
        if prefix:
            return "%s:%s" % (as_text(prefix), cls.DEFAULT_PREFIX)
        else:
            return cls.DEFAULT_PREFIX


_key_namespace = KeyNamespace()


def rq_key(key):
    """
    Get the full key.
    """
    return _key_namespace.generate_key(key)


def set_rq_key_prefix(prefix):
    """
    Override the default RQ key namespace. This allows RQ in different
    processes to be completely separated.
    :param prefix: The prefix to use for all RQ keys. Passing None resets
                   it to the default.
    """

    _key_namespace.set_prefix(prefix)
