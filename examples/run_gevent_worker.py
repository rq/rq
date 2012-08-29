from rq import Queue, Connection
from rq.worker import GeventWorker


if __name__ == '__main__':
    # Tell rq what Redis connection to use
    with Connection():
        q = Queue()
        GeventWorker(q, slaves=8).work()
