from rq import Queue, Worker, Connection


if __name__ == '__main__':
    # Tell rq what Redis connection to use
    with Connection():
        q = Queue()
        Worker(q).work()
