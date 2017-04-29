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
    job.update_meta()
    
    # do more work
    time.sleep(1)
    return x + y
{% endhighlight %}

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="{{site.baseurl}}img/warning.png" />
    <strong>Warning!</strong>
    <p>
The `.update_meta()` was introduced in RQ 0.7.2. You can use the `.save()` method instead 
for RQ <= 0.7.1. For that case you also have to set the job `ttl` greater than the job `timeout`, otherwise the job may dissappear from the queue during execution.
    </p>
</div>
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
