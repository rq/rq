import os
import time
from rq import push_connection
from redis import Redis
from fib import slow_fib

push_connection(Redis())

sync = False
if sync:
    print 'Synchronously:'
    for x in range(22, 33):
        print 'fib(%d) = %d' % (x, slow_fib(x))
    print 'Done'
else:
    # Kick off the tasks asynchronously
    async_results = {}
    for x in range(22, 33):
        async_results[x] = slow_fib.delay(x)

    done = False
    while not done:
        os.system('clear')
        print 'Asynchronously: (now = %s)' % time.time()
        done = True
        for x in range(22, 33):
            result = async_results[x].return_value
            if result is None:
                done = False
                result = '(calculating)'
            print 'fib(%d) = %s' % (x, result)
        print ''
        print 'To start the actual in the background, run a worker:'
        print '    python examples/run_worker.py'
        time.sleep(1)

    print 'Done'
