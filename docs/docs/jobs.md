---
title: "RQ: Jobs"
layout: docs
---

For some use cases it might be useful have access to the current job ID or
instance from within the job function itself.  Or to store arbitrary data on
jobs.


## Job Creation

When you enqueue a function, the job will be returned.  You may then access the 
id property, which can later be used to retrieve the job.

```python
from rq import Queue
from redis import Redis
from somewhere import count_words_at_url

redis_conn = Redis()
q = Queue(connection=redis_conn)  # no args implies the default queue

# Delay execution of count_words_at_url('http://nvie.com')
job = q.enqueue(count_words_at_url, 'http://nvie.com')
print('Job id: %s' % job.id)
```

Or if you want a predetermined job id, you may specify it when creating the job.

```python
job = q.enqueue(count_words_at_url, 'http://nvie.com', job_id='my_job_id')
```

A job can also be created directly with `Job.create()`.

```python
from rq.job import Job

job = Job.create(count_words_at_url, 'http://nvie.com')
print('Job id: %s' % job.id)
q.enqueue_job(job)

# create a job with a predetermined id
job = Job.create(count_words_at url, 'http://nvie.com', id='my_job_id')
```

The keyword arguments accepted by `create()` are:

* `timeout` specifies the maximum runtime of the job before it's interrupted
  and marked as `failed`. Its default unit is seconds and it can be an integer 
  or a string representing an integer(e.g.  `2`, `'2'`). Furthermore, it can 
  be a string with specify unit including hour, minute, second
  (e.g. `'1h'`, `'3m'`, `'5s'`).
* `result_ttl` specifies how long (in seconds) successful jobs and their
  results are kept. Expired jobs will be automatically deleted. Defaults to 500 seconds.
* `ttl` specifies the maximum queued time (in seconds) of the job before it's discarded.
  This argument defaults to `None` (infinite TTL).
* `failure_ttl` specifies how long (in seconds) failed jobs are kept (defaults to 1 year)
* `depends_on` specifies another job (or job id) that must complete before this
  job will be queued.
* `id` allows you to manually specify this job's id
* `description` to add additional description to the job
* `connection`
* `status`
* `origin`
* `meta` a dictionary holding custom status information on this job
* `args` and `kwargs`: use these to explicitly pass arguments and keyword to the
  underlying job function. This is useful if your function happens to have
  conflicting argument names with RQ, for example `description` or `ttl`.
  
In the last case, if you want to pass `description` and `ttl` keyword arguments
to your job and not to RQ's enqueue function, this is what you do:

```python
job = Job.create(count_words_at_url,
          ttl=30,  # This ttl will be used by RQ
          args=('http://nvie.com',),
          kwargs={
              'description': 'Function description', # This is passed on to count_words_at_url
              'ttl': 15  # This is passed on to count_words_at_url function
          })
```

## Job / Queue Creation with Custom Serializer

When creating a job or queue, you can pass in a custom serializer that will be used for serializing / de-serializing job arguments.
Serializers used should have at least `loads` and `dumps` method.
The default serializer used is `pickle`

```python
import json
from rq import Job, Queue

job = Job(connection=connection, serializer=json)
queue = Queue(connection=connection, serializer=json)
```

## Retrieving a Job from Redis

All job information is stored in Redis. You can inspect a job and its attributes
by using `Job.fetch()`.

```python
from redis import Redis
from rq.job import Job

redis = Redis()
job = Job.fetch('my_job_id', connection=redis)
print('Status: %s' % job.get_status())
```

Some interesting job attributes include:
* `job.get_status()` Possible values are `queued`, `started`, `deferred`, `finished`, and `failed`
* `job.func_name`
* `job.args` arguments passed to the underlying job function
* `job.kwargs` key word arguments passed to the underlying job function
* `job.result` stores the return value of the job being executed, will return `None` prior to job execution. Results are kept according to the `result_ttl` parameter (500 seconds by default).
* `job.enqueued_at`
* `job.started_at`
* `job.ended_at`
* `job.exc_info` stores exception information if job doesn't finish successfully.

If you want to efficiently fetch a large number of jobs, use `Job.fetch_many()`.

```python
jobs = Job.fetch_many(['foo_id', 'bar_id'], connection=redis)
for job in jobs:
    print('Job %s: %s' % (job.id, job.func_name))
```

## Accessing The "current" Job from within the job function
 
Since job functions are regular Python functions, you must retrieve the 
job in order to inspect or update the job's attributes.  To do this from within
the function, you can use:

```python
from rq import get_current_job

def add(x, y):
    job = get_current_job()
    print('Current job: %s' % (job.id,))
    return x + y
```

Note that calling get_current_job() outside of the context of a job function will return `None`.


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

A job has two TTLs, one for the job result, `result_ttl`, and one for the job itself, `ttl`.  
The latter is used if you have a job that shouldn't be executed after a certain amount of time.

```python
# When creating the job:
job = Job.create(func=say_hello, 
                 result_ttl=600,  # how long (in seconds) to keep the job (if successful) and its results
                 ttl=43,  # maximum queued time (in seconds) of the job before it's discarded.
                )

# or when queueing a new job:
job = q.enqueue(count_words_at_url, 
                'http://nvie.com', 
                result_ttl=600,  # how long to keep the job (if successful) and its results
                ttl=43  # maximum queued time
               )
```

## Job position in queue

For user feedback or debuging it is possible to get the position of a job
within the work queue. This allows to track the job processing through the
queue.

This function iterates over all jobs within the queue and therefore does
perform poorly on very large job queues.

```python
from rq import Queue
from redis import Redis
from hello import say_hello

redis_conn = Redis()
q = Queue(connection=redis_conn)

job = q.enqueue(say_hello)
job2 = q.enqueue(say_hello)

job2.get_position()
# returns 1

q.get_job_position(job)
# return 0
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
