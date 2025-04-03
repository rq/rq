---
title: "RQ: Results"
layout: docs
---

Enqueueing jobs is delayed execution of function calls. This means we're offloading work
to be processed later, which solves one problem but introduces others - namely,
how do we handle the results and output of these delayed function calls?

In RQ, the return value of a job is stored in Redis. You can inspect each execution via the
`Result` object.

## Job Results
_New in version 1.12.0._

Job executions results are stored in a `Result` object. If a job is executed multiple times,
you can access its execution history by calling `job.results()`.
RQ will store up to 10 latest execution results.

Calling `job.latest_result()` will return the latest `Result` object, which has the
following attributes:
* `type` - an enum of `SUCCESSFUL`, `FAILED`, `RETRIED` or `STOPPED`
* `created_at` - the time at which result is created
* `return_value` - job's return value, only present if result type is `SUCCESSFUL`
* `exc_string` - the exception raised by job, only present if result type is `FAILED`
* `job_id`

```python
job = Job.fetch(id='my_id', connection=redis)
result = job.latest_result()  #  returns Result(id=uid, type=SUCCESSFUL)
if result.type == result.Type.SUCCESSFUL:
    print(result.return_value)
elif result.type == result.Type.FAILED:
    print(result.exc_string)
```

Alternatively, you can also use `job.return_value()` as a shortcut to accessing
the return value of the latest result. Note that `job.return_value` will only
return a not-`None` object if the latest result is a successful execution.

```python
job = Job.fetch(id='my_id', connection=redis)
print(job.return_value())  # Shortcut for job.latest_result().return_value
```

To access multiple results, use `job.results()`.

```python
job = Job.fetch(id='my_id', connection=redis)
for result in job.results():
    print(result.created_at, result.type)
```

_New in version 1.16.0._
To block until a result arrives, you can pass a timeout in seconds to `job.latest_result()`. If any results already exist, the latest result is returned immediately. If the timeout is reached without a result arriving, a `None` object is returned.

```python
job = queue.enqueue(sleep_for_10_seconds)
result = job.latest_result(timeout=60)  # Will hang for about 10 seconds.
```

### Result TTL
Results are written back to Redis with a limited lifetime (via a Redis
expiring key), which is merely to avoid ever-growing Redis databases.

The TTL value of the job result can be specified using the
`result_ttl` keyword argument to `enqueue()` call.  It
can also be used to disable the expiry altogether.  You then are responsible
for cleaning up jobs yourself, though, so be careful to use that.

You can do the following:

    q.enqueue(foo)  # result expires after 500 secs (the default)
    q.enqueue(foo, result_ttl=86400)  # result expires after 1 day
    q.enqueue(foo, result_ttl=0)  # result gets deleted immediately
    q.enqueue(foo, result_ttl=-1)  # result never expires--you should delete jobs manually

## Asking RQ to Retry
_New in version 2.1.0._

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="/img/warning.png" />
    <strong>Note:</strong>
    <p>This feature is still in beta, use at your own risk!</p>
</div>

RQ lets you easily retry jobs by returning a special `Retry` result from your job function.

```python
from rq import Retry
import requests

def count_words_at_url(url, max=1, interval=60):
    try:
        resp = requests.get(url)
    except requests.exceptions.ConnectionError:
        return Retry(max=max, interval=interval)
    return len(resp.text.split())

job = queue.enqueue(count_words_at_url, 'https://python-rq.org', max=3, interval=60)
```

The above job will be retried up to 3 times, with 60 seconds interval in between executions.
Please note that the retry count returned by job executions will be treated differently from the
`Retry` parameter passed to `enqueue()`, which will be used in cases where jobs fail due to
[exceptions](/docs/exceptions/#retrying-failed-jobs).


## Dealing with Exceptions

Jobs can fail due to exceptions occurring. RQ provides several ways to handle failed jobs.
For detailed information about exceptions and retries, see [Exceptions & Retries](/docs/exceptions/#retrying-failed-jobs).


## Dealing with Interruptions

When workers get killed in the polite way (Ctrl+C or `kill`), RQ tries hard not
to lose any work.  The current work is finished after which the worker will
stop further processing of jobs.  This ensures that jobs always get a fair
chance to finish themselves.

However, workers can be killed forcefully by `kill -9`, which will not give the
workers a chance to finish the job gracefully or to put the job on the `failed`
queue.  Therefore, killing a worker forcefully could potentially lead to
damage. Just sayin'.

If the worker gets killed while a job is running, it will eventually end up in
`FailedJobRegistry` because a cleanup task will raise an `AbandonedJobError`.


## Dealing with Job Timeouts

By default, jobs should execute within 180 seconds.  After that, the worker
kills the work horse and puts the job onto the `failed` queue, indicating the
job timed out.

If a job requires more (or less) time to complete, the default timeout period
can be loosened (or tightened), by specifying it as a keyword argument to the
`enqueue()` call, like so:

```python
q = Queue(connection=Redis())
q.enqueue(mytask, args=(foo,), kwargs={'bar': qux}, job_timeout=600)  # 10 mins
```

You can also change the default timeout for jobs that are enqueued via specific
queue instances at once, which can be useful for patterns like this:

```python
# High prio jobs should end in 8 secs, while low prio
# work may take up to 10 mins
high = Queue('high', connection=Redis(), default_timeout=8)  # 8 secs
low = Queue('low', connection=Redis(), default_timeout=600)  # 10 mins

# Individual jobs can still override these defaults
low.enqueue(really_really_slow, job_timeout=3600)  # 1 hr
```

Individual jobs can still specify an alternative timeout, as workers will
respect these.
