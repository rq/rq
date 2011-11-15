from rq import Queue, Worker

# Tell rq what Redis connection to use
from redis import Redis
from rq import conn
conn.push(Redis())

if __name__ == '__main__':
    q = Queue()
    Worker(q).work_forever()
