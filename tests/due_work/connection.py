import os

from redis import Redis

#: The contract's own Redis database, emptied around every case.
CONNECTION = Redis(db=int(os.environ.get('RQ_DUE_WORK_REDIS_DB', '15')))
