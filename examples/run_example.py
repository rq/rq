import os
import time

from fib import slow_fib
from redis import Redis

from rq import Queue


def enqueue(connection):
    # Range of Fibonacci numbers to compute
    fib_range = range(20, 34)

    # Kick off the tasks asynchronously
    q = Queue(connection=connection)
    jobs = [q.enqueue(slow_fib, arg) for arg in fib_range]
    return jobs


def wait_for_all(jobs):
    start_time = time.time()
    done = False
    while not done:
        os.system('clear')
        print('Asynchronously: (now = %.2f)' % (time.time() - start_time,))
        done = True
        for job in jobs:
            result = job.return_value()
            if result is None:
                done = False
                status = job.get_status(refresh=False)
                result = '(%s)' % status.name
            (arg,) = job.args
            print('fib(%d) = %s' % (arg, result))
        print('')
        print('To start the actual in the background, run a worker:')
        print('    $ rq worker')
        time.sleep(0.2)

    print('Done')


if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    with Redis() as redis_conn:
        jobs = enqueue(redis_conn)
        wait_for_all(jobs)
