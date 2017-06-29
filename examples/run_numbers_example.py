from rq import Connection, Queue
from file_numbers import write_number, merge, cleanup_partial_files
from time import sleep

def main():
    with Connection():
        queue = Queue()
        job_numbers = [queue.enqueue(write_number, x, on_cancel=cleanup_partial_files) for x in range(10)]
        job_merge = queue.enqueue(merge, 10, depends_on=job_numbers,
                                  on_cancel=cleanup_partial_files)
        sleep(1)
        job_merge.cancel()
        job_numbers[6].cancel()

if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    main()