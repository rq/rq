import time
from datetime import datetime

from .connections import get_current_connection
from .job import Job
from .queue import Queue


class Scheduler(object):
    prefix = 'rq:scheduler:'

    def __init__(self, name='default', interval=60, connection=None):
        if connection is None:
            connection = get_current_connection()
        self.connection = connection
        self.name = name
        self._key = '{0}{1}'.format(self.prefix, name)
        self._interval = interval

    def schedule(self, time, func, *args, **kwargs):
        """
        Pushes a job to the scheduler queue. The scheduled queue is a Redis sorted
        set ordered by timestamp - which in this case is job's scheduled execution time.
        """
        if func.__module__ == '__main__':
            raise ValueError(
                    'Functions from the __main__ module cannot be processed '
                    'by workers.')

        job = Job.create(func, *args, connection=self.connection, **kwargs)
        job.origin = self.name
        job.save()
        self.connection.zadd(self._key, job.id, int(time.strftime('%s')))

    def queue_jobs(self):
        """
        Push scheduled jobs into queues (score lower than current timestamp). 
        """
        job_ids = self.connection.zrangebyscore(self._key, 0, datetime.now().strftime('%s'))
        for job_id in job_ids:
            queue = Queue.from_queue_key('rq:queue:{0}'.format(self.name), connection=self.connection).push_job_id(job_id)
            self.connection.zrem(self._key, job_id)

    def run(self):
        """
        Periodically check whether there's any job that should be put in the queue (score 
        lower than current time).
        """
        while True:
            self.queue_jobs()
            time.sleep(self._interval)
