from redis import Redis
from rq import push_connection
from rq.daemon import run_daemon

push_connection(Redis())

run_daemon(['default'])
