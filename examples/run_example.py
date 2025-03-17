import os
import time

from fib import slow_fib
from redis import Redis

from rq import Queue


def main(connection):
    # Range of Fibonacci numbers to compute
    fib_range = range(20, 34)

    # Kick off the tasks asynchronously
    jobs = {}
    q = Queue(connection=connection)
    for x in fib_range:
        jobs[x] = q.enqueue(slow_fib, x)

    start_time = time.time()
    done = False
    while not done:
        os.system('clear')
        print('Asynchronously: (now = %.2f)' % (time.time() - start_time,))
        done = True
        for x in fib_range:
            job = jobs[x]
            result = job.return_value()
            if result is None:
                done = False
                status = job.get_status(refresh=False)
                result = '(%s)' % status.name
            print('fib(%d) = %s' % (x, result))
        print('')
        print('To start the actual in the background, run a worker:')
        print('    $ rq worker')
        time.sleep(0.2)

    print('Done')


if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    with Redis() as redis_conn:
        main(redis_conn)
