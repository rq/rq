from rq import Queue, Worker, use_connection

# Tell rq what Redis connection to use
use_connection()

if __name__ == '__main__':
    q = Queue()
    Worker(q).work()
