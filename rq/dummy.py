# -*- coding: utf-8 -*-
"""
Some dummy tasks that are well-suited for generating load for testing purposes.
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import gevent
import random


def do_nothing():
    pass


def sleep(secs):
    gevent.sleep(secs)


def endless_loop():
    while True:
        gevent.sleep(1)


def div_by_zero():
    1 / 0


def fib(n):
    if n <= 1:
        return 1
    else:
        return fib(n - 2) + fib(n - 1)


def random_failure():
    if random.choice([True, False]):
        class RandomError(Exception):
            pass
        raise RandomError('Ouch!')
    return 'OK'
