---
title: "RQ: Simple job queues for Python"
layout: default
---

RQ (_Redis Queue_) is a lightweight Python library for queueing jobs and
processing them in workers.  It is backed by Redis and it is extremely simple
to use.


## Getting started

To put jobs on queues, you don't have to do anything special, just define
a function:

{% highlight python %}
def multiply(x, y):
    return x + y
{% endhighlight %}

Then, create a RQ queue:

{% highlight python %}
from rq import Queue
q = Queue()
{% endhighlight %}

And enqueue the function call:

{% highlight python %}
result = q.enqueue(multiply, 20, 40)
{% endhighlight %}

For a more complete example, refer to the [docs][d].  But this is the essence.

[d]: {{site.baseurl}}docs/


## The worker

To start executing the enqueued function calls in the background, start
a worker from your project's directory:

{% highlight console %}
$ rqworker
worker: Waiting on default
worker: Forked 76397 at 1321825763
horse: Processing work since 1321825763
horse: Processing multiply from default since 1321825763.68
horse: result = 800
{% endhighlight %}

That's about it.


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
