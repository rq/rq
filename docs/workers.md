---
title: "RQ: Simple job queues for Python"
layout: docs
---

A worker is a Python process that typically runs in the background and exists
solely as a work horse to perform lengthy or blocking tasks that you don't want
to perform inside web processes.


## Starting workers

To start crunching work, simply start a worker from the root of your project
directory:

{% highlight console %}
$ rq worker high normal low
*** Listening for work on high, normal, low
Got send_newsletter('me@nvie.com') from default
Job ended normally without result
*** Listening for work on high, normal, low
...
{% endhighlight %}

Workers will read jobs from the given queues (the order is important) in an
endless loop, waiting for new work to arrive when all jobs are done.

Each worker will process a single job at a time.  Within a worker, there is no
concurrent processing going on.  If you want to perform jobs concurrently,
simply start more workers.


### Burst mode

By default, workers will start working immediately and will block and wait for
new work when they run out of work.  Workers can also be started in _burst
mode_ to finish all currently available work and quit as soon as all given
queues are emptied.

{% highlight console %}
$ rq worker --burst high normal low
*** Listening for work on high, normal, low
Got send_newsletter('me@nvie.com') from default
Job ended normally without result
No more work, burst finished.
Registering death.
{% endhighlight %}

This can be useful for batch work that needs to be processed periodically, or
just to scale up your workers temporarily during peak periods.


## Inside the worker

### The worker life-cycle

The life-cycle of a worker consists of a few phases:

1. _Boot_. Loading the Python environment.
2. _Birth registration_. The worker registers itself to the system so it knows
   of this worker.
3. _Start listening_. A job is popped from any of the given Redis queues.
   If all queues are empty and the worker is running in burst mode, quit now.
   Else, wait until jobs arrive.
4. _Prepare job execution_. The worker tells the system that it will begin work
   by setting its status to `busy` and registers job in the `StartedJobRegistry`.
5. _Fork a child process._
   A child process (the "work horse") is forked off to do the actual work in
   a fail-safe context.
6. _Process work_. This performs the actual job work in the work horse.
7. _Cleanup job execution_. The worker sets its status to `idle` and sets both
   the job and its result to expire based on `result_ttl`. Job is also removed
   from `StartedJobRegistry` and added to to `FinishedJobRegistry` in the case
   of successful execution, or `FailedQueue` in the case of failure.
8. _Loop_.  Repeat from step 3.


## Performance notes

Basically the `rq worker` shell script is a simple fetch-fork-execute loop.
When a lot of your jobs do lengthy setups, or they all depend on the same set
of modules, you pay this overhead each time you run a job (since you're doing
the import _after_ the moment of forking).  This is clean, because RQ won't
ever leak memory this way, but also slow.

A pattern you can use to improve the throughput performance for these kind of
jobs can be to import the necessary modules _before_ the fork.  There is no way
of telling RQ workers to perform this set up for you, but you can do it
yourself before starting the work loop.

To do this, provide your own worker script (instead of using `rq worker`).
A simple implementation example:

{% highlight python %}
#!/usr/bin/env python
import sys
from rq import Connection, Worker

# Preload libraries
import library_that_you_want_preloaded

# Provide queue names to listen to as arguments to this script,
# similar to rq worker
with Connection():
    qs = sys.argv[1:] or ['default']

    w = Worker(qs)
    w.work()
{% endhighlight %}


### Worker names

Workers are registered to the system under their names, see [monitoring][m].
By default, the name of a worker is equal to the concatenation of the current
hostname and the current PID.  To override this default, specify the name when
starting the worker, using the `--name` option.

[m]: /docs/monitoring/


### Retrieving worker information

`Worker` instances store their runtime information in Redis. Here's how to
retrieve them:

{% highlight python %}
from rq.worker import Worker
workers = Worker.all(redis_conn)
{% endhighlight %}

If you only want to retrieve a specific worker:

{% highlight python %}
from rq.worker import Worker
worker = Worker.find_by_key('rq:worker:name')

print(worker.birth_date) # date when worker was instantiated
{% endhighlight %}

### Worker statistics

_New in version 0.9.0._

If you want to check the utilization of your queues, `Worker` instances
store a few useful information:

{% highlight python %}
from rq.worker import Worker
worker = Worker.find_by_key('rq:worker:name')

worker.successful_job_count  # Number of jobs finished successfully
worker.failed_job_count. # Number of failed jobs processed by this worker
worker.total_working_time  # Number of time spent executing jobs
{% endhighlight %}


## Taking down workers

If, at any time, the worker receives `SIGINT` (via Ctrl+C) or `SIGTERM` (via
`kill`), the worker wait until the currently running task is finished, stop
the work loop and gracefully register its own death.

If, during this takedown phase, `SIGINT` or `SIGTERM` is received again, the
worker will forcefully terminate the child process (sending it `SIGKILL`), but
will still try to register its own death.


## Using a config file

_New in version 0.3.2._

If you'd like to configure `rq worker` via a configuration file instead of
through command line arguments, you can do this by creating a Python file like
`settings.py`:

{% highlight python %}
REDIS_URL = 'redis://localhost:6379/1'

# You can also specify the Redis DB to use
# REDIS_HOST = 'redis.example.com'
# REDIS_PORT = 6380
# REDIS_DB = 3
# REDIS_PASSWORD = 'very secret'

# Queues to listen on
QUEUES = ['high', 'normal', 'low']

# If you're using Sentry to collect your runtime exceptions, you can use this
# to configure RQ for it in a single step
# The 'sync+' prefix is required for raven: https://github.com/nvie/rq/issues/350#issuecomment-43592410
SENTRY_DSN = 'sync+http://public:secret@example.com/1'
{% endhighlight %}

The example above shows all the options that are currently supported.

_Note: The_ `QUEUES` _and_ `REDIS_PASSWORD` _settings are new since 0.3.3._

To specify which module to read settings from, use the `-c` option:

{% highlight console %}
$ rq worker -c settings
{% endhighlight %}


## Custom worker classes

_New in version 0.4.0._

There are times when you want to customize the worker's behavior. Some of the
more common requests so far are:

1. Managing database connectivity prior to running a job.
2. Using a job execution model that does not require `os.fork`.
3. The ability to use different concurrency models such as
   `multiprocessing` or `gevent`.

You can use the `-w` option to specify a different worker class to use:

{% highlight console %}
$ rq worker -w 'path.to.GeventWorker'
{% endhighlight %}


## Custom Job and Queue classes

_Will be available in next release._

You can tell the worker to use a custom class for jobs and queues using
`--job-class` and/or `--queue-class`.

{% highlight console %}
$ rq worker --job-class 'custom.JobClass' --queue-class 'custom.QueueClass'
{% endhighlight %}

Don't forget to use those same classes when enqueueing the jobs.

For example:

{% highlight python %}
from rq import Queue
from rq.job import Job

class CustomJob(Job):
    pass

class CustomQueue(Queue):
    job_class = CustomJob

queue = CustomQueue('default', connection=redis_conn)
queue.enqueue(some_func)
{% endhighlight %}


## Custom exception handlers

_New in version 0.5.5._

If you need to handle errors differently for different types of jobs, or simply want to customize
RQ's default error handling behavior, run `rq worker` using the `--exception-handler` option:

{% highlight console %}
$ rq worker --exception-handler 'path.to.my.ErrorHandler'

# Multiple exception handlers is also supported
$ rq worker --exception-handler 'path.to.my.ErrorHandler' --exception-handler 'another.ErrorHandler'
{% endhighlight %}
