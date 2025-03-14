from redis import Redis
from rq import Queue, Worker

if __name__ == '__main__':
    # Tell rq what Redis connection to use
    with Redis() as connection:
        q = Queue(connection=connection)
        Worker(q).work()
