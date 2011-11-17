---
title: "RQ: Simple job queues for Python"
layout: default
---

RQ is a lightweight Python library for queueing work and processing them in
workers.  It is backed by Redis and it is extremely simple to use.

## Putting jobs on queues

To put jobs on queues, first declare a Python function to be called on
a background process:

{% highlight python %}
def slow_fib(n):
    if n <= 1:
        return 1
    else:
        return slow_fib(n-1) + slow_fib(n-2)
{% endhighlight %}

Notice anything?  There's nothing special about a job!  Any Python function can
be put on an RQ queue, as long as the function is in a module that is
importable from the worker process.

To calculate the 36th Fibonacci number in the background, simply do this:

{% highlight python %}
from rq import Queue
from fib import slow_fib

# Delay calculation of the 36th Fibonacci number
q = Queue()
q.enqueue(slow_fib, 36)
{% endhighlight %}

If you want to put the work on a specific queue, simply specify its name:

{% highlight python %}
q = Queue('math')
q.enqueue(slow_fib, 36)
{% endhighlight %}

You can use any queue name, so you can quite flexibly distribute work to your
own desire.  Common patterns are to name your queues after priorities (e.g.
`high`, `medium`, `low`).


## The worker

**NOTE: You currently need to create the worker yourself, which is extremely
easy, but RQ will include a custom script soon that can be used to start
arbitrary workers without writing any code.**

Creating a worker daemon is also extremely easy.  Create a file `worker.py`
with the following content:

{% highlight python %}
from rq import Queue, Worker

q = Queue()
Worker(q).work_forever()
{% endhighlight %}

After that, start a worker instance:

    python worker.py

This will wait for work on the default queue and start processing it as soon as
messages arrive.

You can even watch several queues at the same time and start processing from
them:

{% highlight python %}
from rq import Queue, Worker

queues = map(Queue, ['high', 'normal', 'low'])
Worker(queues).work()
{% endhighlight %}

Which will keep popping jobs from the given queues, giving precedence to the
`high` queue, then `normal`, etc.  It will return when there are no more jobs
left (contrast this to the previous example using `Worker.work_forever()`,
which will never return since it keeps waiting for new work to arrive).


## Installation

Simply use the following command to install the latest released version:

    pip install rq

If you want the cutting edge version (that may well be broken), use this:

    pip install -e git+git@github.com:nvie/rq.git@master#egg=rq


## Project history

This project has been inspired by the good parts of [Celery][1], [Resque][2]
and [this snippet][3], and has been created as a lightweight alternative to the
heaviness of Celery or other AMQP-based queueing implementations.

[1]: http://www.celeryproject.org/
[2]: https://github.com/defunkt/resque
[3]: http://flask.pocoo.org/snippets/73/

Project values:

* Simplicity over completeness
* Fail-safety over performance
* Runtime insight over static configuration upfront

This means that, to use RQ, you don't have to set up any queues up front, and
you don't have to specify any channels, exchanges, or whatnot.  You can put
jobs onto any queue you want, at runtime.  As soon as you enqueue a job, it is
created on the fly.
