---
title: "RQ: Scheduling Jobs"
layout: docs
---

_New in version 1.2.0._

If you need a battle tested version of RQ job scheduling, please take a look at
https://github.com/rq/rq-scheduler instead.

New in RQ 1.2.0 is `RQScheduler`, a built-in component that allows you to schedule jobs
for future execution.

This component is developed based on prior experience of developing the external
`rq-scheduler` library. The goal of taking this component in house is to allow
RQ to have job scheduling capabilities without:
1. Running a separate `rqscheduler` CLI command.
2. Worrying about a separate `Scheduler` class.

Running RQ workers with the scheduler component is simple:

```console
$ rq worker --with-scheduler
```

## Scheduling Jobs for Execution

There are two main APIs to schedule jobs for execution, `enqueue_at()` and `enqueue_in()`.

`queue.enqueue_at()` works almost like `queue.enqueue()`, except that it expects a datetime
for its first argument.

```python
from datetime import datetime
from rq import Queue
from redis import Redis
from somewhere import say_hello

queue = Queue(name='default', connection=Redis())

# Schedules job to be run at 9:15, October 10th in the local timezone
job = queue.enqueue_at(datetime(2019, 10, 8, 9, 15), say_hello)
```

Note that if you pass in a naive datetime object, RQ will automatically convert it
to the local timezone.

`queue.enqueue_in()` accepts a `timedelta` as its first argument.

```python
from datetime import timedelta
from rq import Queue
from redis import Redis
from somewhere import say_hello

queue = Queue(name='default', connection=Redis())

# Schedules job to be run in 10 seconds
job = queue.enqueue_in(timedelta(seconds=10), say_hello)
```

Jobs that are scheduled for execution are not placed in the queue, but they are
stored in `ScheduledJobRegistry`.

```python
from datetime import timedelta
from redis import Redis

from rq import Queue
from rq.registry import ScheduledJobRegistry

redis = Redis()

queue = Queue(name='default', connection=redis)
job = queue.enqueue_in(timedelta(seconds=10), say_nothing)
print(job in queue)  # Outputs False as job is not enqueued

registry = ScheduledJobRegistry(queue=queue)
print(job in registry)  # Outputs True as job is placed in ScheduledJobRegistry
```

## Running the Scheduler

If you use RQ's scheduling features, you need to run RQ workers with the
scheduler component enabled.

```console
$ rq worker --with-scheduler
```

You can also run a worker with scheduler enabled in a programmatic way.

```python
from rq import Worker, Queue
from redis import Redis

redis = Redis()
queue = Queue(connection=redis)

worker = Worker(queues=[queue], connection=redis)
worker.work(with_scheduler=True)
```

Only a single scheduler can run for a specific queue at any one time. If you run multiple
workers with scheduler enabled, only one scheduler will be actively working for a given queue.

Active schedulers are responsible for enqueueing scheduled jobs. Active schedulers will check for
scheduled jobs once every second.

Idle schedulers will periodically (every 15 minutes) check whether the queues they're
responsible for have active schedulers. If they don't, one of the idle schedulers will start
working. This way, if a worker with active scheduler dies, the scheduling work will be picked
up by other workers with the scheduling component enabled.


## Safe importing of the worker module

When running the worker programmatically with the scheduler, you must keep in mind that the
import must be protected with `if __name__ == '__main__'`. The scheduler runs on it's own process
(using `multiprocessing` from the stdlib), so the new spawned process must able to safely import the module without
causing any side effects (starting a new process on top of the main ones).

```python
...

# When running `with_scheduler=True` this is necessary
if __name__ == '__main__':
    worker = Worker(queues=[queue], connection=redis)
    worker.work(with_scheduler=True)

...
# When running without the scheduler this is fine
worker = Worker(queues=[queue], connection=redis)
worker.work()
```

More information on the Python official docs [here](https://docs.python.org/3.7/library/multiprocessing.html#the-spawn-and-forkserver-start-methods).

