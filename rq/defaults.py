import sys

DEFAULT_JOB_CLASS = 'rq.job.Job'
DEFAULT_QUEUE_CLASS = 'rq.Queue'
DEFAULT_WORKER_CLASS = 'rq.Worker'
DEFAULT_CONNECTION_CLASS = 'redis.StrictRedis'
DEFAULT_WORKER_TTL = 420
DEFAULT_RESULT_TTL = 500

if sys.platform == 'win32':
    DEFAULT_WORKER_CLASS = 'rq.WindowsWorker'
