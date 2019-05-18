---
title: "RQ: Documentation Overview"
layout: docs
---

A _job_ is a Python object, representing a function that is invoked
asynchronously in a worker (background) process.  Any Python function can be
invoked asynchronously, by simply pushing a reference to the function and its
arguments onto a queue.  This is called _enqueueing_.


## Enqueueing Jobs

To put jobs on queues, first declare a function:

```python
import requests

def count_words_at_url(url):
    resp = requests.get(url)
    return len(resp.text.split())
```

Noticed anything?  There's nothing special about this function!  Any Python
function call can be put on an RQ queue.

To put this potentially expensive word count for a given URL in the background,
simply do this:

```python
from rq import Queue
from redis import Redis
from somewhere import count_words_at_url
import time

# Tell RQ what Redis connection to use
redis_conn = Redis()
q = Queue(connection=redis_conn)  # no args implies the default queue

# Delay execution of count_words_at_url('http://nvie.com')
job = q.enqueue(count_words_at_url, 'http://nvie.com')
print(job.result)   # => None

# Now, wait a while, until the worker is finished
time.sleep(2)
print(job.result)   # => 889
```

If you want to put the work on a specific queue, simply specify its name:

```python
q = Queue('low', connection=redis_conn)
q.enqueue(count_words_at_url, 'http://nvie.com')
```

Notice the `Queue('low')` in the example above?  You can use any queue name, so
you can quite flexibly distribute work to your own desire.  A common naming
pattern is to name your queues after priorities (e.g.  `high`, `medium`,
`low`).

In addition, you can add a few options to modify the behaviour of the queued
job. By default, these are popped out of the kwargs that will be passed to the
job function.

* `job_timeout` specifies the maximum runtime of the job before it's interrupted
    and marked as `failed`. Its default unit is second and it can be an integer or a string representing an integer(e.g.  `2`, `'2'`). Furthermore, it can be a string with specify unit including hour, minute, second(e.g. `'1h'`, `'3m'`, `'5s'`).
* `result_ttl` specifies how long (in seconds) successful jobs and their
results are kept. Expired jobs will be automatically deleted. Defaults to 500 seconds.
* `ttl` specifies the maximum queued time of the job before it's discarded.
  If you specify a value of `-1` you indicate an infinite job ttl and it will run indefinitely
* `failure_ttl` specifies how long failed jobs are kept (defaults to 1 year)
* `depends_on` specifies another job (or job id) that must complete before this
  job will be queued.
* `job_id` allows you to manually specify this job's `job_id`
* `at_front` will place the job at the *front* of the queue, instead of the
  back
* `description` to add additional description to enqueued jobs.
* `args` and `kwargs`: use these to explicitly pass arguments and keyword to the
  underlying job function. This is useful if your function happens to have
  conflicting argument names with RQ, for example `description` or `ttl`.

In the last case, if you want to pass `description` and `ttl` keyword arguments
to your job and not to RQ's enqueue function, this is what you do:

```python
q = Queue('low', connection=redis_conn)
q.enqueue(count_words_at_url,
          ttl=30,  # This ttl will be used by RQ
          args=('http://nvie.com',),
          kwargs={
              'description': 'Function description', # This is passed on to count_words_at_url
              'ttl': 15  # This is passed on to count_words_at_url function
          })
```

For cases where the web process doesn't have access to the source code running
in the worker (i.e. code base X invokes a delayed function from code base Y),
you can pass the function as a string reference, too.

```python
q = Queue('low', connection=redis_conn)
q.enqueue('my_package.my_module.my_func', 3, 4)
```


## Working with Queues

Besides enqueuing jobs, Queues have a few useful methods:

```python
from rq import Queue
from redis import Redis

redis_conn = Redis()
q = Queue(connection=redis_conn)

# Getting the number of jobs in the queue
print(len(q))

# Retrieving jobs
queued_job_ids = q.job_ids # Gets a list of job IDs from the queue
queued_jobs = q.jobs # Gets a list of enqueued job instances
job = q.fetch_job('my_id') # Returns job having ID "my_id"

# Emptying a queue, this will delete all jobs in this queue
q.empty()

# Deleting a queue
q.delete(delete_jobs=True) # Passing in `True` will remove all jobs in the queue
# queue is now unusable. It can be recreated by enqueueing jobs to it.
```


### On the Design

With RQ, you don't have to set up any queues upfront, and you don't have to
specify any channels, exchanges, routing rules, or whatnot.  You can just put
jobs onto any queue you want.  As soon as you enqueue a job to a queue that
does not exist yet, it is created on the fly.

RQ does _not_ use an advanced broker to do the message routing for you.  You
may consider this an awesome advantage or a handicap, depending on the problem
you're solving.

Lastly, it does not speak a portable protocol, since it depends on [pickle][p]
to serialize the jobs, so it's a Python-only system.


## The delayed result

When jobs get enqueued, the `queue.enqueue()` method returns a `Job` instance.
This is nothing more than a proxy object that can be used to check the outcome
of the actual job.

For this purpose, it has a convenience `result` accessor property, that
will return `None` when the job is not yet finished, or a non-`None` value when
the job has finished (assuming the job _has_ a return value in the first place,
of course).


## The `@job` decorator
If you're familiar with Celery, you might be used to its `@task` decorator.
Starting from RQ >= 0.3, there exists a similar decorator:

```python
from rq.decorators import job

@job('low', connection=my_redis_conn, timeout=5)
def add(x, y):
    return x + y

job = add.delay(3, 4)
time.sleep(1)
print(job.result)
```


## Bypassing workers

For testing purposes, you can enqueue jobs without delegating the actual
execution to a worker (available since version 0.3.1). To do this, pass the
`is_async=False` argument into the Queue constructor:

```python
>>> q = Queue('low', is_async=False, connection=my_redis_conn)
>>> job = q.enqueue(fib, 8)
>>> job.result
21
```

The above code runs without an active worker and executes `fib(8)`
synchronously within the same process. You may know this behaviour from Celery
as `ALWAYS_EAGER`. Note, however, that you still need a working connection to
a redis instance for storing states related to job execution and completion.


## Job dependencies

New in RQ 0.4.0 is the ability to chain the execution of multiple jobs.
To execute a job that depends on another job, use the `depends_on` argument:

```python
q = Queue('low', connection=my_redis_conn)
report_job = q.enqueue(generate_report)
q.enqueue(send_report, depends_on=report_job)
```

The ability to handle job dependencies allows you to split a big job into
several smaller ones. A job that is dependent on another is enqueued only when
its dependency finishes *successfully*.


## The worker

To learn about workers, see the [workers][w] documentation.

[w]: {{site.baseurl}}workers/


## Considerations for jobs

Technically, you can put any Python function call on a queue, but that does not
mean it's always wise to do so.  Some things to consider before putting a job
on a queue:

* Make sure that the function's `__module__` is importable by the worker.  In
  particular, this means that you cannot enqueue functions that are declared in
  the `__main__` module.
* Make sure that the worker and the work generator share _exactly_ the same
  source code.
* Make sure that the function call does not depend on its context.  In
  particular, global variables are evil (as always), but also _any_ state that
  the function depends on (for example a "current" user or "current" web
  request) is not there when the worker will process it.  If you want work done
  for the "current" user, you should resolve that user to a concrete instance
  and pass a reference to that user object to the job as an argument.


## Limitations

RQ workers will only run on systems that implement `fork()`.  Most notably,
this means it is not possible to run the workers on Windows without using the [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/about) and running in a bash shell.


[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
