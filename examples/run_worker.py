from redis import Redis
from rq import conn
from rq.daemon import run_daemon

# Tell rq what Redis connection to use
conn.push(Redis())

listen_on_queues = ['default']
run_daemon(listen_on_queues)
