# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import time

from rq import Connection, Queue

from rnd_delay import do_it, print_finish, last_one


def main():
    # Kick off the tasks asynchronously
    async_results = {}
    q = Queue()
    q2 = Queue("Queue2")
    for x in range(10):
        async_results[x] = q.enqueue(do_it, 5)
    async_results[10] = q.enqueue(last_one)
    print('Done enqueing')
    q2.enqueue(print_finish, depends_on=async_results.values())
    print("Enqueued depends_on")


if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    with Connection():
        main()
