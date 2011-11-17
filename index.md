---
title: "RQ: Simple job queues for Python"
layout: default
---

RQ (_Redis Queue_) is a lightweight Python library for queueing jobs and
processing them in workers.  It is backed by Redis and it is extremely simple
to use.

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
from rq import use_redis, Queue
from stupid import multiply

# Tell RQ what Redis connection to use
rq.use_redis()  # no args uses Redis on localhost:6379

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
msg = mailer.Message()
msg.To = 'me@nvie.com'
msg.Subject = 'RQ is awesome!'
msg.Body = 'Can I please contribute to RQ?'

q = Queue('low')
q.enqueue(send_mail, msg)
{% endhighlight %}

Notice the `Queue('low')` in the example above?  You can use any queue name, so
you can quite flexibly distribute work to your own desire.  A common naming
pattern is to name your queues after priorities (e.g.  `high`, `medium`,
`low`).



### On the Design

With RQ, you don't have to set up any queues upfront, and you don't have to
specify any channels, exchanges, routing rules, or whatnot.  You can just put
jobs onto any queue you want.  As soon as you enqueue a job to a queue that
does not exist yet, it is created on the fly.

RQ does _not_ use a broker to do the message routing for you.  You may consider
this an awesome advantage or a handicap, depending on the problem you're
solving.

Lastly, it does not speak a portable protocol, since it uses [pickle][p] to
serialize the jobs, so it's a Python-only system.


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


## Installation

Simply use the following command to install the latest released version:

    pip install rq

If you want the cutting edge version (that may well be broken), use this:

    pip install -e git+git@github.com:nvie/rq.git@master#egg=rq


## Project history

This project has been inspired by the good parts of [Celery][1], [Resque][2]
and [this snippet][3], and has been created as a lightweight alternative to the
heaviness of Celery or other AMQP-based queueing implementations.

Project values:

* Simplicity over completeness
* Fail-safety over performance
* Runtime insight over static configuration upfront

[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
[1]: http://www.celeryproject.org/
[2]: https://github.com/defunkt/resque
[3]: http://flask.pocoo.org/snippets/73/
