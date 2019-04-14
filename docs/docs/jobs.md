---
title: "RQ: Jobs"
layout: docs
---

For some use cases it might be useful have access to the current job ID or
instance from within the job function itself.  Or to store arbitrary data on
jobs.


## Retrieving Job from Redis

All job information is stored in Redis. You can inspect a job and its attributes
by using `Job.fetch()`.

```python
from redis import Redis
from rq.job import Job

redis = Redis()
job = Job.fetch('my_job_id', connection=redis)
print('Status: %s' $ job.get_status())
```

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

If you want to efficiently fetch a large number of jobs, use `Job.fetch_many()`.

```python
jobs = Job.fetch_many(['foo_id', 'bar_id'], connection=redis)
for job in jobs:
    print('Job %s: %s' % (job.id, job.func_name))
```

## Accessing The "current" Job

Since job functions are regular Python functions, you have to ask RQ for the
current job ID, if any.  To do this, you can use:

```python
from rq import get_current_job

def add(x, y):
    job = get_current_job()
    print('Current job: %s' % (job.id,))
    return x + y
```


## Storing arbitrary data on jobs

_Improved in 0.8.0._

To add/update custom status information on this job, you have access to the
`meta` property, which allows you to store arbitrary pickleable data on the job
itself:

```python
import socket

def add(x, y):
    job = get_current_job()
    job.meta['handled_by'] = socket.gethostname()
    job.save_meta()

    # do more work
    time.sleep(1)
    return x + y
```


## Time to live for job in queue

_New in version 0.4.7._

A job has two TTLs, one for the job result and one for the job itself. This means that if you have
job that shouldn't be executed after a certain amount of time, you can define a TTL as such:

```python
# When creating the job:
job = Job.create(func=say_hello, ttl=43)

# or when queueing a new job:
job = q.enqueue(count_words_at_url, 'http://nvie.com', ttl=43)
```


## Failed Jobs

If a job fails during execution, the worker will put the job in a FailedJobRegistry.
On the Job instance, the `is_failed` property will be true. FailedJobRegistry
can be accessed through `queue.failed_job_registry`.

```python
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
```

By default, failed jobs are kept for 1 year. You can change this by specifying
`failure_ttl` (in seconds) when enqueueing jobs.

```python
job = queue.enqueue(foo_job, failure_ttl=300)  # 5 minutes in seconds
```

## Requeueing Failed Jobs

RQ also provides a CLI tool that makes requeueing failed jobs easy.

```console
# This will requeue foo_job_id and bar_job_id from myqueue's failed job registry
rq requeue --queue myqueue -u redis://localhost:6379 foo_job_id bar_job_id

# This command will requeue all jobs in myqueue's failed job registry
rq requeue --queue myqueue -u redis://localhost:6379 --all
```
