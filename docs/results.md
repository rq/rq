---
title: "RQ: Documentation"
layout: docs
---

Enqueueing jobs means delayed execution of function calls.  This means we're
solving a problem, but are getting back a few in return.


## Dealing with results

Python functions may have return values, so jobs can have them, too.  If a job
returns a return value, the worker will write that return value back to a Redis
key with a limited lifetime (_500 seconds by default_).

The key it uses for this purpose is specified in the
<a href="{{site.baseurl}}docs/internals/#job-tuple">job tuple</a>, as the
second element.  That key is generated upon job enqueuement and is a random
UUID, for example:

    rq:result:e3f3d953-8910-4ace-9a5f-7d361221a590

The party that enqueued the job gets back a `DelayedResult` object as a result
of the enqueueing itself.  Such a `DelayedResult` object is tightly coupled
with that UUID, to be able to poll for results.


### On the return value's TTL

Return values are written back to Redis with a limited lifetime (via a Redis
expiring key), which is merely to avoid ever-growing Redis databases.

The TTL of these keys cannot be changed currently, nor can the expiration be
disabled.  If this introduces any real life problems, we might support
specifying it in the future.  For now, we're keeping it simple.


## Dealing with exceptions

Jobs can fail and throw exceptions.  This is a fact of life.  RQ deals with
this in the following way.

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px; height: 100px;" src="http://a.dryicons.com/images/icon_sets/colorful_stickers_icons_set/png/256x256/warning.png" />
    <strong>Be warned!</strong>
    <p>This is the current implementation, not the desired future implementation.</p>
</div>

When an exception is thrown inside a job, it is caught by the worker,
serialized and stored in the return value key.  In that sense, the only way to
distinguish from a failed job or a job with a return value is to check the type
of the returned value.  If it is an exception, it was a failed job.

This is not the desirable final implementation.


### The desired implementation

Job failure is too important not to be noticed and therefore the job's return
value should never expire.  Furthermore, it should be possible to retry failed
jobs.  Typically, this is something that needs manual interpretation, since
there is no automatic or reliable way of letting RQ judge whether it is safe
for certain tasks to be retried or not.

Therefore, when an exception is thrown, the following info is serialized to
a special RQ queue called `failed`:

    (worker, queue, exc_info, job_tuple)

This tuple holds the worker name, originating queue name, exception information
and the original <a href="{{site.baseurl}}docs/internals/#job-tuple">job
tuple</a>, which makes it possible to interpret the problem and possibly
resubmit the job.


## Dealing with interruption

When workers get killed in the polite way (Ctrl+C or `kill`), RQ tries hard not
to lose any work.  The current work is finished after which the worker will
stop further processing of jobs.  This ensures that jobs always get a fair
change to finish themselves.

However, workers can be killed forcefully by `kill -9`, which will not give the
workers a change to finish the job gracefully or to put the job on the `failed`
queue.  Therefore, killing a worker forcefully could potentially lead to
damage.

Just sayin'.


## Dealing with job timeouts

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px; height: 100px;" src="http://a.dryicons.com/images/icon_sets/colorful_stickers_icons_set/png/256x256/warning.png" />
    <strong>Be warned!</strong>
    <p>This timeout stuff does not exist yet.</p>
</div>

By default, jobs should execute within 180 seconds.  After that, the worker
kills the work horse and puts the job onto the `failed` queue, indicating the
job timed out.

If a job requires more (or less) time to complete, the default timeout period
can be loosened (or tightened), by specifying it as a keyword argument to the
`Queue.enqueue()` call, like so:

{% highlight python %}
q = Queue()
q.enqueue(mytask, foo, bar=qux, timeout='10m')
{% endhighlight console %}

You can also change the default timeout for a whole queue at once, which can be
useful for patterns like this:

{% highlight python %}
# High prio jobs should end in 8 secs, while low prio
# work may take up to 10 mins
high = Queue('high', timeout=8)
low = Queue('low', timeout='10m')

# Individual jobs can still override these defaults
low.enqueue(really_really_slow, timeout='1h')
{% endhighlight console %}

Individual jobs can still specify an alternative timeout, as workers will
respect these.
