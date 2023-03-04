---
title: "RQ: Exceptions & Retries"
layout: docs
---

Jobs can fail due to exceptions occurring. When your RQ workers run in the
background, how do you get notified of these exceptions?

## Default: FailedJobRegistry

The default safety net for RQ is the `FailedJobRegistry`. Every job that doesn't
execute successfully is stored here, along with its exception information (type,
value, traceback).

```python
from redis import Redis
from rq import Queue
from rq.job import Job
from rq.registry import FailedJobRegistry

redis = Redis()
queue = Queue(connection=redis)
registry = FailedJobRegistry(queue=queue)

# Show all failed job IDs and the exceptions they caused during runtime
for job_id in registry.get_job_ids():
    job = Job.fetch(job_id, connection=redis)
    print(job_id, job.exc_info)
```

## Retrying Failed Jobs

_New in version 1.5.0_

RQ lets you easily retry failed jobs. To configure retries, use RQ's
`Retry` object that accepts `max` and `interval` arguments. For example:

```python
from redis import Redis
from rq import Retry, Queue

from somewhere import my_func

queue = Queue(connection=redis)
# Retry up to 3 times, failed job will be requeued immediately
queue.enqueue(my_func, retry=Retry(max=3))

# Retry up to 3 times, with 60 seconds interval in between executions
queue.enqueue(my_func, retry=Retry(max=3, interval=60))

# Retry up to 3 times, with longer interval in between retries
queue.enqueue(my_func, retry=Retry(max=3, interval=[10, 30, 60]))
```

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="/img/warning.png" />
    <strong>Note:</strong>
    <p>
        If you use `interval` argument with `Retry`, don't forget to run your workers using
        the `--with-scheduler` argument.
    </p>
</div>


## Custom Exception Handlers

RQ supports registering custom exception handlers. This makes it possible to
inject your own error handling logic to your workers.

This is how you register custom exception handler(s) to an RQ worker:

```python
from exception_handlers import foo_handler, bar_handler

w = Worker([q], exception_handlers=[foo_handler, bar_handler])
```

The handler itself is a function that takes the following parameters: `job`,
`exc_type`, `exc_value` and `traceback`:

```python
def my_handler(job, exc_type, exc_value, traceback):
    # do custom things here
    # for example, write the exception info to a DB

```

You might also see the three exception arguments encoded as:

```python
def my_handler(job, *exc_info):
    # do custom things here
```

```python
from exception_handlers import foo_handler

w = Worker([q], exception_handlers=[foo_handler],
           disable_default_exception_handler=True)
```


## Chaining Exception Handlers

The handler itself is responsible for deciding whether or not the exception
handling is done, or should fall through to the next handler on the stack.
The handler can indicate this by returning a boolean. `False` means stop
processing exceptions, `True` means continue and fall through to the next
exception handler on the stack.

It's important to know for implementers that, by default, when the handler
doesn't have an explicit return value (thus `None`), this will be interpreted
as `True` (i.e.  continue with the next handler).

To prevent the next exception handler in the handler chain from executing,
use a custom exception handler that doesn't fall through, for example:

```python
def black_hole(job, *exc_info):
    return False
```

## Work Horse Killed Handler
_New in version 1.13.0._

In addition to job exception handler(s), RQ supports registering a handler for unexpected workhorse termination.
This handler is called when a workhorse is unexpectedly terminated, for example due to OOM.

This is how you set a workhorse termination handler to an RQ worker:

```python
from my_handlers import my_work_horse_killed_handler

w = Worker([q], work_horse_killed_handler=my_work_horse_killed_handler)
```

The handler itself is a function that takes the following parameters: `job`,
`retpid`, `ret_val` and `rusage`:

```python
from resource import struct_rusage
from rq.job import Job
def my_work_horse_killed_handler(job: Job, retpid: int, ret_val: int, rusage: struct_rusage):
    # do your thing here, for example set job.retries_left to 0 

```

## Built-in Exceptions
RQ Exceptions you can get in your job failure callbacks

# AbandonedJobError
This error means an unfinished job was collected by another worker's maintenance task.  
This usually happens when a worker is busy with a job and is terminated before it finished that job.  
Another worker collects this job and moves it to the FailedJobRegistry.