from rq import Queue, use_connection
from rq.worker import GeventWorker as Worker

# Tell rq what Redis connection to use
use_connection()

if __name__ == '__main__':
    q = Queue()
    Worker(q, slaves=8).work()
