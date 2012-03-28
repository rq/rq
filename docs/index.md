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
from rq import use_connection, Queue
from somewhere import count_words_at_url

# Tell RQ what Redis connection to use
use_connection()  # no args uses Redis on localhost:6379

# Delay calculation of the multiplication
q = Queue()
result = q.enqueue(count_words_at_url, 'http://nvie.com')
print result.return_value   # => None

# Now, wait a while, until the worker is finished
time.sleep(2)
print result.return_value   # => 889
{% endhighlight %}

If you want to put the work on a specific queue, simply specify its name:

{% highlight python %}
q = Queue('low')
q.enqueue(count_words_at_url, 'http://nvie.com')
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

RQ does _not_ use an advanced broker to do the message routing for you.  You
may consider this an awesome advantage or a handicap, depending on the problem
you're solving.

Lastly, it does not speak a portable protocol, since it depends on [pickle][p]
to serialize the jobs, so it's a Python-only system.


## The delayed result

When jobs get enqueued, the `queue.enqueue()` method returns a `Job` instance.
This is nothing more than a proxy object that can be used to check the outcome
of the actual job.

For this purpose, it has a convenience `return_value` accessor property, that
will return `None` when the job is not yet finished, or a non-`None` value when
the job has finished (assuming the job _has_ a return value in the first place,
of course).


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

Currently, it is impossible to enqueue instance methods.

Furthermore, RQ workers will only run on systems that implement `fork()`.  Most
notably, this means it is not possible to run the workers on Windows.


[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
