---
title: "RQ: Documentation"
layout: docs
---

For some use cases it might be useful have access to the current job ID or
instance from within the job function itself.  Or to store arbitrary data on
jobs.


## Retrieving Job from Redis

All job information is stored in Redis. You can inspect a job and its attributes
by using `Job.fetch()`.

{% highlight python %}
from redis import Redis
from rq.job import Job

connection = Redis()
job = Job.fetch('my_job_id', connection=redis)
print('Status: %s' $ job.get_status())
{% endhighlight %}

Some interesting job attributes include:
* `job.status`
* `job.func_name`
* `job.args`
* `job.kwargs`
* `job.result`
* `job.enqueued_at`
* `job.started_at`
* `job.ended_at`
* `job.exc_info`

## Accessing the "current" job

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

If a job fails and raises an exception, the worker will put the job in a failed job queue.
On the Job instance, the `is_failed` property will be true. To fetch all failed jobs, scan
through the `get_failed_queue()` queue.

{% highlight python %}
from redis import StrictRedis
from rq import push_connection, get_failed_queue, Queue
from rq.job import Job


con = StrictRedis()
push_connection(con)

def div_by_zero(x):
    return x / 0

job = Job.create(func=div_by_zero, args=(1, 2, 3))
job.origin = 'fake'
job.save()
fq = get_failed_queue()
fq.quarantine(job, Exception('Some fake error'))
assert fq.count == 1

fq.requeue(job.id)

assert fq.count == 0
assert Queue('fake').count == 1
{% endhighlight %}
