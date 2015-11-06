# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from functools import partial

from redis import Redis, StrictRedis


def fix_return_type(func):
    # deliberately no functools.wraps() call here, since the function being
    # wrapped is a partial, which has no module
    def _inner(*args, **kwargs):
        value = func(*args, **kwargs)
        if value is None:
            value = -1
        return value
    return _inner


PATCHED_METHODS = ['_setex', '_lrem', '_zadd', '_pipeline', '_ttl']


def patch_connection(connection):
    if not isinstance(connection, StrictRedis):
        raise ValueError('A StrictRedis or Redis connection is required.')

    # Don't patch already patches objects
    if all([hasattr(connection, attr) for attr in PATCHED_METHODS]):
        return connection

    if isinstance(connection, Redis):
        connection._setex = partial(StrictRedis.setex, connection)
        connection._lrem = partial(StrictRedis.lrem, connection)
        connection._zadd = partial(StrictRedis.zadd, connection)
        connection._pipeline = partial(StrictRedis.pipeline, connection)
        connection._ttl = fix_return_type(partial(StrictRedis.ttl, connection))
        if hasattr(connection, 'pttl'):
            connection._pttl = fix_return_type(partial(StrictRedis.pttl, connection))

    elif isinstance(connection, StrictRedis):
        connection._setex = connection.setex
        connection._lrem = connection.lrem
        connection._zadd = connection.zadd
        connection._pipeline = connection.pipeline
        connection._ttl = connection.ttl
        if hasattr(connection, 'pttl'):
            connection._pttl = connection.pttl
    else:
        raise ValueError('Unanticipated connection type: {}. Please report this.'.format(type(connection)))

    return connection
