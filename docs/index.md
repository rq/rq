---
title: "RQ: Simple job queues for Python"
layout: default
---

RQ (_Redis Queue_) is a simple Python library for queueing jobs and processing
them in the background with workers.  It is backed by Redis and it is designed
to have a low barrier to entry.  It can be integrated in your web stack easily.

RQ requires Redis >= 3.0.0.

## Getting Started

First, run a Redis server.  You can use an existing one.  To put jobs on
queues, you don't have to do anything special, just define your typically
lengthy or blocking function:

```python
import requests

def count_words_at_url(url):
    resp = requests.get(url)
    return len(resp.text.split())
```

Then, create a RQ queue:

```python
from redis import Redis
from rq import Queue

q = Queue(connection=Redis())
```

And enqueue the function call:

```python
from my_module import count_words_at_url
result = q.enqueue(count_words_at_url, 'http://nvie.com')
```

### Scheduling Jobs
Scheduling jobs are similarly easy:

```python
# Schedule job to run at 9:15, October 10th
job = queue.enqueue_at(datetime(2019, 10, 8, 9, 15), say_hello)

# Schedule job to be run in 10 seconds
job = queue.enqueue_in(timedelta(seconds=10), say_hello)
```

## Repeating Jobs

To repeat jobs multiple times:

```python
from rq.repeat import Repeat

# Repeat job 3 times after successful completion, with 60 second intervals
job = queue.enqueue(say_hello, repeat=Repeat(times=3, interval=60))

# Use different intervals between repetitions
job = queue.enqueue(say_hello, repeat=Repeat(times=3, interval=[10, 30, 60]))
```

Note that jobs will only repeat after successful executions. To retry failed jobs, use `Retry`.

### Retrying Failed Jobs
You can also ask RQ to retry failed jobs:

```python
from rq import Retry

# Retry up to 3 times, failed job will be requeued immediately
queue.enqueue(say_hello, retry=Retry(max=3))

# Retry up to 3 times, with configurable intervals between retries
queue.enqueue(say_hello, retry=Retry(max=3, interval=[10, 30, 60]))
```

### The Worker

To start executing enqueued function calls in the background, start a worker
from your project's directory:

```console
$ rq worker --with-scheduler
*** Listening for work on default
Got count_words_at_url('http://nvie.com') from default
Job result = 818
*** Listening for work on default
```

That's about it.


## Installation

Simply use the following command to install the latest released version:

    pip install rq


## High Level Overview

There are several important concepts in RQ:
1. `Queue`: contains a list of `Job` instances to be executed in a FIFO manner.
2. `Job`: contains the function to be executed by the worker.
3. `Worker`: responsible for getting `Job` instances from a `Queue` and executing them.
4. `Execution`: contains runtime data of a `Job`, created by a `Worker` when it executes a `Job`.
5. `Result`: stores the outcome of an `Execution`, whether it succeeded or failed.

## Project History

This project has been inspired by the good parts of [Celery][1], [Resque][2]
and [this snippet][3], and has been created as a lightweight alternative to
existing queueing frameworks, with a low barrier to entry.

[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
[1]: http://www.celeryproject.org/
[2]: https://github.com/defunkt/resque
[3]: https://github.com/fengsp/flask-snippets/blob/1f65833a4291c5b833b195a09c365aa815baea4e/utilities/rq.py
