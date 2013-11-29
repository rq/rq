---
title: "RQ: Documentation"
layout: docs
---

Enqueueing jobs is delayed execution of function calls.  This means we're
solving a problem, but are getting back a few in return.


## Dealing with results

Python functions may have return values, so jobs can have them, too.  If a job
returns a non-`None` return value, the worker will write that return value back
to the job's Redis hash under the `result` key.  The job's Redis hash itself
will expire after 500 seconds by default after the job is finished.

The party that enqueued the job gets back a `Job` instance as a result of the
enqueueing itself.  Such a `Job` object is a proxy object that is tied to the
job's ID, to be able to poll for results.


**On the return value's TTL**  

Return values are written back to Redis with a limited lifetime (via a Redis
expiring key), which is merely to avoid ever-growing Redis databases.

From RQ >= 0.3.1, The TTL value of the job result can be specified using the
`result_ttl` keyword argument to `enqueue_call()` calls.  It can also be used
to disable the expiry altogether.  You then are responsible for cleaning up
jobs yourself, though, so be careful to use that.

You can do the following:

{% highlight python %}
q.enqueue_call(func=foo)  # result expires after 500 secs (the default)
q.enqueue_call(func=foo, result_ttl=86400)  # result expires after 1 day
q.enqueue_call(func=foo, result_ttl=0)  # result gets deleted immediately
q.enqueue_call(func=foo, result_ttl=-1)  # result never expires--you should delete jobs manually
{% endhighlight %}

Additionally, you can use this for keeping around finished jobs without return
values, which would be deleted immediately by default.

{% highlight python %}
q.enqueue_call(func=func_without_rv, result_ttl=500)  # job kept explicitly
{% endhighlight %}


## Dealing with exceptions

Jobs can fail and throw exceptions.  This is a fact of life.  RQ deals with
this in the following way.

Job failure is too important not to be noticed and therefore the job's return
value should never expire.  Furthermore, it should be possible to retry failed
jobs.  Typically, this is something that needs manual interpretation, since
there is no automatic or reliable way of letting RQ judge whether it is safe
for certain tasks to be retried or not.

When an exception is thrown inside a job, it is caught by the worker,
serialized and stored under the job's Redis hash's `exc_info` key.  A reference
to the job is put on the `failed` queue.

The job itself has some useful properties that can be used to aid inspection:

* the original creation time of the job
* the last enqueue date
* the originating queue
* a textual description of the desired function invocation
* the exception information

This makes it possible to inspect and interpret the problem manually and
possibly resubmit the job.


## Dealing with interruption

When workers get killed in the polite way (Ctrl+C or `kill`), RQ tries hard not
to lose any work.  The current work is finished after which the worker will
stop further processing of jobs.  This ensures that jobs always get a fair
change to finish themselves.

However, workers can be killed forcefully by `kill -9`, which will not give the
workers a chance to finish the job gracefully or to put the job on the `failed`
queue.  Therefore, killing a worker forcefully could potentially lead to
damage.

Just sayin'.


## Dealing with job timeouts

By default, jobs should execute within 180 seconds.  After that, the worker
kills the work horse and puts the job onto the `failed` queue, indicating the
job timed out.

If a job requires more (or less) time to complete, the default timeout period
can be loosened (or tightened), by specifying it as a keyword argument to the
`Queue.enqueue()` call, like so:

{% highlight python %}
q = Queue()
q.enqueue_call(func=mytask, args=(foo,), kwargs={'bar': qux}, timeout=600)  # 10 mins
{% endhighlight %}

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="{{site.baseurl}}img/warning.png" />
    <strong>Warning!</strong>
    <p>
Before RQ 0.3.0, the `.enqueue()` method accepted a `timeout` keyword argument,
but this introduced an ambiguity as it was impossible to pass that keyword
argument to the target function.  So, from RQ >= 0.3.0, you can use
`.enqueue_call()` for that.
    </p>
</div>

You can also change the default timeout for jobs that are enqueued via specific
queue instances at once, which can be useful for patterns like this:

{% highlight python %}
# High prio jobs should end in 8 secs, while low prio
# work may take up to 10 mins
high = Queue('high', default_timeout=8)  # 8 secs
low = Queue('low', default_timeout=600)  # 10 mins

# Individual jobs can still override these defaults
low.enqueue_call(really_really_slow, timeout=3600)  # 1 hr
{% endhighlight %}

Individual jobs can still specify an alternative timeout, as workers will
respect these.
