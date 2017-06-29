from rq import Connection, Queue
from video_encoding import ffmpeg_fragment, ffmpeg_concat, cleanup_partial_files
from time import sleep


def no_failure(queue):
    job_encoders = [queue.enqueue(ffmpeg_fragment, i) for i in range(15)]
    queue.enqueue(ffmpeg_concat, len(job_encoders), depends_on=job_encoders)


def failure(queue):
    job_encoders= [queue.enqueue(ffmpeg_fragment, i, on_cancel=cleanup_partial_files) for i in range(15)]
    job_merge = queue.enqueue(ffmpeg_concat, len(job_encoders), depends_on=job_encoders,
                              on_cancel=cleanup_partial_files)
    sleep(1)
    job_encoders[6].cancel()

def main():
    with Connection():
        queue = Queue()
        #no_failure(queue)
        failure(queue)

if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    main()