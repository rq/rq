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
$ rqworker high normal low
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
$ rqworker --burst high normal low
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
{% comment %}  (Not implemented yet.)
4. _Broadcast work began_. The worker tells the system that it will begin work.
{% endcomment %}
5. _Fork a child process._
   A child process (the "work horse") is forked off to do the actual work in
   a fail-safe context.
6. _Process work_. This performs the actual job work in the work horse.
{% comment %}  (Not implemented yet.)
7. _Broadcast work ended_. The worker tells the system that it ended work.
{% endcomment %}
8. _Loop_.  Repeat from step 3.


### Worker names

Workers are registered to the system under their names, see [monitoring][m].
By default, the name of a worker is equal to the concatenation of the current
hostname and the current PID.  To override this default, specify the name when
starting the worker, using the `--name` option.

[m]: {{site.baseurl}}docs/monitoring/


## Taking down workers

If, at any time, the worker receives `SIGINT` (via Ctrl+C) or `SIGTERM` (via
`kill`), the worker wait until the currently running task is finished, stop
the work loop and gracefully register its own death.

If, during this takedown phase, `SIGINT` or `SIGTERM` is received again, the
worker will forcefully terminate the child process (sending it `SIGKILL`), but
will still try to register its own death.
