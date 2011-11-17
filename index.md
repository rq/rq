---
title: "RQ: Simple job queues for Python"
layout: default
---

RQ is a lightweight Python library for queueing jobs and processing them in
workers.  It is backed by Redis and it is extremely simple to use.

## Putting jobs on queues

To put jobs on queues, first declare a job:

{% highlight python %}
def multiply(x, y):
    return x + y
{% endhighlight %}

Noticed anything?  There's nothing special about a job!  Any Python function
call can be put on an RQ queue.

To multiply two numbers in the background, simply do this:

{% highlight python %}
from rq import Queue
from stupidity import multiply

# Delay calculation of the multiplication
q = Queue()
result = q.enqueue(multiply, 20, 40)
print result.return_value   # => None

# Now, wait a while, until the worker is finished
print result.return_value   # => 800
{% endhighlight %}

Of course, multiplying does not make any sense to do in a worker, at all.
A more useful example where you would use jobs instead is for sending mail in
the background.  Here, we use the Python [mailer][m] package:

{% highlight python %}
import mailer

def send_mail(message):
    from my_config import smtp_settings
    sender = mailer.Mailer(**smtp_settings)
    sender.send(message)
{% endhighlight %}

If you want to put the work on a specific queue, simply specify its name:

{% highlight python %}
q = Queue('math')
q.enqueue(slow_fib, 36)
{% endhighlight %}

You can use any queue name, so you can quite flexibly distribute work to your
own desire.  Common patterns are to name your queues after priorities (e.g.
`high`, `medium`, `low`).

[m]: http://pypi.python.org/pypi/mailer


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
