---
title: "RQ: Documentation"
layout: docs
---

A _job_ is a Python object, representing a function that is invoked
asynchronously in a worker (background) process.  Any Python function can be
invoked asynchronously, by simply pushing a reference to the function and its
arguments onto a queue.  This is called _enqueueing_.


## Enqueueing jobs

To put jobs on queues, first declare a function:

{% highlight python %}
import requests

def count_words_at_url(url):
    resp = requests.get(url)
    return len(resp.text.split())
{% endhighlight %}

Noticed anything?  There's nothing special about this function!  Any Python
function call can be put on an RQ queue.

To put this potentially expensive word count for a given URL in the background,
simply do this:

{% highlight python %}
from rq import Connection, Queue
from somewhere import count_words_at_url

# Tell RQ what Redis connection to use
redis_conn = Redis()
q = Queue(connection=redis_conn)  # no args implies the default queue

# Delay calculation of the multiplication
job = q.enqueue(count_words_at_url, 'http://nvie.com')
print job.result   # => None

# Now, wait a while, until the worker is finished
time.sleep(2)
print job.result   # => 889
{% endhighlight %}

If you want to put the work on a specific queue, simply specify its name:

{% highlight python %}
q = Queue('low', connection=redis_conn)
q.enqueue(count_words_at_url, 'http://nvie.com')
{% endhighlight %}

Notice the `Queue('low')` in the example above?  You can use any queue name, so
you can quite flexibly distribute work to your own desire.  A common naming
pattern is to name your queues after priorities (e.g.  `high`, `medium`,
`low`).

For cases where you want to pass in options to `.enqueue()` itself (rather than
to the job function), use `.enqueue_call()`, which is the more explicit
counterpart.  A typical use case for this is to pass in a `timeout` argument:

{% highlight python %}
q = Queue('low', connection=redis_conn)
q.enqueue_call(func=count_words_at_url,
               args=('http://nvie.com'),
               timeout=30)
{% endhighlight %}

For cases where the web process doesn't have access to the source code running
in the worker (i.e. code base X invokes a delayed function from code base Y),
you can pass the function as a string reference, too.

{% highlight python %}
q = Queue('low', connection=redis_conn)
q.enqueue('my_package.my_module.my_func', 3, 4)
{% endhighlight %}


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

{% highlight python %}
@job('low', conn=my_redis_conn, timeout=5)
def add(x, y):
    return x + y

job = add.delay(3, 4)
time.sleep(1)
print job.result
{% endhighlight %}


## The worker

To learn about workers, see the [workers][w] documentation.

[w]: {{site.baseurl}}docs/workers/


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
this means it is not possible to run the workers on Windows.


[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
