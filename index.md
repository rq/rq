---
title: "RQ: Simple job queues for Python"
layout: default
---

RQ (_Redis Queue_) is a lightweight Python library for queueing jobs and
processing them in workers.  It is backed by Redis and it is extremely simple
to use.


## Getting started

First, run a Redis server, of course:

{% highlight console %}
$ redis-server
{% endhighlight %}

To put jobs on queues, you don't have to do anything special, just define
your typically lengthy or blocking function:

{% highlight python %}
import urllib2

def count_words_at_url(url):
    f = urllib2.urlopen(url)
    count = 0
    while True:
        line = f.readline()
        if not line:
            break
        count += len(line.split())
    return count
{% endhighlight %}

Then, create a RQ queue:

{% highlight python %}
import rq import *
use_redis()
q = Queue()
{% endhighlight %}

And enqueue the function call:

{% highlight python %}
from my_module import count_words_at_url
result = q.enqueue(count_words_at_url,
                   'http://nvie.com')
{% endhighlight %}

For a more complete example, refer to the [docs][d].  But this is the essence.

[d]: {{site.baseurl}}docs/


## The worker

To start executing the enqueued function calls in the background, start
a worker from your project's directory:

{% highlight console %}
$ rqworker
worker: Watching queues: default
horse: Processing multiply from default since 1321825763.68
horse: Job result = 800
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
