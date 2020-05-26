# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys


def is_python_version(*versions):
    for version in versions:
        if (sys.version_info[0] == version[0] and
                sys.version_info >= version):
            return True
    return False


try:
    from functools import total_ordering
except ImportError:
    def total_ordering(cls):  # noqa
        """Class decorator that fills in missing ordering methods"""
        convert = {
            '__lt__': [('__gt__', lambda self, other: other < self),
                       ('__le__', lambda self, other: not other < self),
                       ('__ge__', lambda self, other: not self < other)],
            '__le__': [('__ge__', lambda self, other: other <= self),
                       ('__lt__', lambda self, other: not other <= self),
                       ('__gt__', lambda self, other: not self <= other)],
            '__gt__': [('__lt__', lambda self, other: other > self),
                       ('__ge__', lambda self, other: not other > self),
                       ('__le__', lambda self, other: not self > other)],
            '__ge__': [('__le__', lambda self, other: other >= self),
                       ('__gt__', lambda self, other: not other >= self),
                       ('__lt__', lambda self, other: not self >= other)]
        }
        roots = set(dir(cls)) & set(convert)
        if not roots:
            raise ValueError('must define at least one ordering operation: < > <= >=')  # noqa
        root = max(roots)  # prefer __lt__ to __le__ to __gt__ to __ge__
        for opname, opfunc in convert[root]:
            if opname not in roots:
                opfunc.__name__ = str(opname)
                opfunc.__doc__ = getattr(int, opname).__doc__
                setattr(cls, opname, opfunc)
        return cls


PY2 = sys.version_info[0] == 2
if not PY2:
    # Python 3.x and up
    text_type = str
    string_types = (str,)

    def as_text(v):
        if v is None:
            return None
        elif isinstance(v, bytes):
            return v.decode('utf-8')
        elif isinstance(v, str):
            return v
        else:
            raise ValueError('Unknown type %r' % type(v))

    def decode_redis_hash(h):
        return dict((as_text(k), h[k]) for k in h)
else:
    # Python 2.x
    def text_type(v):
        try:
            return unicode(v)  # noqa
        except Exception:
            return unicode(v, "utf-8", errors="ignore")  # noqa

    string_types = (str, unicode)  # noqa

    def as_text(v):
        if v is None:
            return None
        elif isinstance(v, str):
            return v.decode('utf-8')
        elif isinstance(v, unicode):  # noqa
            return v
        else:
            raise Exception("Input cannot be decoded into literal thing.")

    def decode_redis_hash(h):
        return h


try:
    from datetime import timezone
    utc = timezone.utc
except ImportError:
    # Python 2.x workaround
    from datetime import timedelta, tzinfo

    class UTC(tzinfo):
        def utcoffset(self, dt):
            return timedelta(0)

        def tzname(self, dt):
            return "UTC"

        def dst(self, dt):
            return timedelta(0)

    utc = UTC()


def hmset(pipe_or_connection, name, mapping):
    # redis-py versions 3.5.0 and above accept a mapping parameter for hset
    # This requires Redis server >= 4.0 so this is temporarily commented out
    # and will be re-enabled at a later date
    # try:
    #    return pipe_or_connection.hset(name, mapping=mapping)
    # earlier versions require hmset to be used
    # except TypeError:
    return pipe_or_connection.hmset(name, mapping)
