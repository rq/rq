# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import time

from rq import Connection, Queue

from fib import slow_fib


def main():
    # Range of Fibonacci numbers to compute
    fib_range = range(20, 34)

    # Kick off the tasks asynchronously
    async_results = {}
    q = Queue()
    for x in fib_range:
        async_results[x] = q.enqueue(slow_fib, x)

    start_time = time.time()
    done = False
    while not done:
        os.system('clear')
        print('Asynchronously: (now = %.2f)' % (time.time() - start_time,))
        done = True
        for x in fib_range:
            result = async_results[x].return_value
            if result is None:
                done = False
                result = '(calculating)'
            print('fib(%d) = %s' % (x, result))
        print('')
        print('To start the actual in the background, run a worker:')
        print('    python examples/run_worker.py')
        time.sleep(0.2)

    print('Done')


if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    with Connection():
        main()
