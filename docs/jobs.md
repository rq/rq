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
    print 'Current job: %s' % (job.id,)
    return x + y
{% endhighlight %}


## Storing arbitrary data on jobs

_New in version 0.3.3._

To add/update custom status information on this job, you have access to the
`meta` property, which allows you to store arbitrary pickleable data on the job
itself:

{% highlight python %}
import socket

def add(x, y):
    job = get_current_job()
    job.meta['handled_by'] = socket.gethostname()
    job.save()
    return x + y
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
assert(fq.count == 1)

fq.requeue(job.id)

assert(fq.count == 0)
assert(Queue('fake').count == 1)
{% endhighlight %}