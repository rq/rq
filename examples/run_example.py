import os
import time
from rq import conn
from redis import Redis
from fib import slow_fib

# Tell rq what Redis connection to use
conn.push(Redis())

# Kick off the tasks asynchronously
async_results = {}
for x in range(20, 30):
    async_results[x] = slow_fib.delay(x)

done = False
while not done:
    os.system('clear')
    print 'Asynchronously: (now = %s)' % time.time()
    done = True
    for x in range(20, 30):
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
