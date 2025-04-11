---
title: "RQ: Scheduling Jobs"
layout: docs
---

Running RQ workers with the scheduler component is simple:

```console
$ rq worker --with-scheduler
```

## Scheduling Jobs

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

queue = Queue(name='default', connection=Redis())
job = queue.enqueue_in(timedelta(seconds=10), say_nothing)
print(job in queue)  # Outputs False as job is not enqueued

registry = ScheduledJobRegistry(queue=queue)
print(job in registry)  # Outputs True as job is placed in ScheduledJobRegistry
```

## Repeating Jobs

_New in version 2.2.0._

RQ allows you to easily repeat job executions using the `Repeat` class. This functionality lets you specify how many times a job should be repeated and at what intervals.

### Using the Repeat Class

The `Repeat` class takes two main arguments:
- `times`: the number of times to repeat the job (must be at least 1)
- `interval`: the time interval between job repetitions (in seconds). This can be either a single value or a list of intervals. Defaults to 0.

```python
from redis import Redis
from rq import Queue, Repeat

queue = Queue(connection=Redis())

# Repeat a job 3 times with the same interval of 30 seconds
job = queue.enqueue(my_function, repeat=Repeat(times=3, interval=30))

# Or use different intervals between repetitions
job = queue.enqueue(my_function, repeat=Repeat(times=3, interval=[5, 10, 15]))
```

### How Repeat Works

When a job with a `Repeat` configuration completes successfully, it will be scheduled to run again after the specified interval. The job maintains a counter of remaining repeats, which decreases after each repetition.

`Repeat` only affects jobs that complete successfully. Failed jobs will not be repeated. To handle failed jobs, use RQ's [retry functionality](/docs/scheduling/) instead.

### Repeat Interval

If you specify an interval of zero, the job will be immediately re-enqueued after completion:

```python
# This job will repeat 3 times with no delay between executions
job = queue.enqueue(my_function, repeat=Repeat(times=3, interval=0))
```

If you provide a list of intervals that is shorter than the number of repetition, the last interval in the list will be used for any remaining repetitions.

```python
# This job will repeat 5 times with these intervals:
# 1st repetition: after 5 seconds
# 2nd repetition: after 10 seconds
# 3rd repetition: after 15 seconds
# 4th repetition: after 15 seconds
# 5th repetition: after 15 seconds
job = queue.enqueue(my_function, repeat=Repeat(times=5, interval=[5, 10, 15]))
```

### Checking Job Repeat Status

You can check whether a job will repeat by examining these attributes:

```python
job = queue.enqueue(my_function, repeat=Repeat(times=3, interval=30))

# Check how many repeats are remaining
# job.repeats_left will decrement with each repetition
print(job.repeats_left)  # 3

# Check the intervals
print(job.repeat_intervals)  # [30]
```

## Running the Scheduler

If you use RQ's scheduling and repeating features, you need to run RQ workers with the
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


## Safe Importing of the Worker Module

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
