---
title: "RQ: Documentation"
layout: docs
---

For some use cases it might be useful have access to the current job ID or
instance from within the job function itself.  Or to store arbitrary data on
jobs.


## Accessing the "current" job

_New in version 0.3.3._

Since job functions are regular Python functions, you have to ask RQ for the
current job ID, if any.  To do this, you can use:

{% highlight python %}
from rq import get_current_job

def add(x, y):
    job = get_current_job()
    print('Current job: %s' % (job.id,))
    return x + y
{% endhighlight %}


## Storing arbitrary data on jobs

_Improved in 0.8.0._

To add/update custom status information on this job, you have access to the
`meta` property, which allows you to store arbitrary pickleable data on the job
itself:

{% highlight python %}
import socket

def add(x, y):
    job = get_current_job()
    job.meta['handled_by'] = socket.gethostname()
    job.save_meta()

    # do more work
    time.sleep(1)
    return x + y
{% endhighlight %}


## Time to live for job in queue

_New in version 0.4.7._

A job has two TTLs, one for the job result and one for the job itself. This means that if you have
job that shouldn't be executed after a certain amount of time, you can define a TTL as such:

{% highlight python %}
# When creating the job:
job = Job.create(func=say_hello, ttl=43)

# or when queueing a new job:
job = q.enqueue(count_words_at_url, 'http://nvie.com', ttl=43)
{% endhighlight %}


## Failed Jobs

If a job fails during execution, the worker will put the job in a FailedJobRegistry.
On the Job instance, the `is_failed` property will be true. FailedJobRegistry
can be accessed through `queue.failed_job_registry`.

{% highlight python %}
from redis import StrictRedis
from rq import Queue
from rq.job import Job


def div_by_zero(x):
    return x / 0


connection = StrictRedis()
queue = Queue(connection=connection)
job = queue.enqueue(div_by_zero, 1)
registry = queue.failed_job_registry

worker = Worker([queue])
worker.work(burst=True)

assert len(registry) == 1  # Failed jobs are kept in FailedJobRegistry

registry.requeue(job)  # Puts job back in its original queue

assert len(registry) == 0

assert queue.count == 1
{% endhighlight %}

## Requeueing Failed Jobs

RQ also provides a CLI tool that makes requeueing failed jobs easy.

{% highlight console %}
# This will requeue foo_job_id and bar_job_id from myqueue's failed job registry
rq requeue --queue myqueue -u redis://localhost:6379 foo_job_id bar_job_id

# This command will requeue all jobs in myqueue's failed job registry
rq requeue --queue myqueue -u redis://localhost:6379 --all
{% endhighlight %}
