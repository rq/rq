---
title: "RQ: Simple job queues for Python"
layout: default
---

RQ (_Redis Queue_) is a simple Python library for queueing jobs and processing
them in the background with workers.  It is backed by Redis and it is designed
to have a low barrier to entry.  It can be integrated in your web stack easily.

RQ requires Redis >= 2.6.0.

## Getting started

First, run a Redis server.  You can use an existing one.  To put jobs on
queues, you don't have to do anything special, just define your typically
lengthy or blocking function:

{% highlight python %}
import requests

def count_words_at_url(url):
    resp = requests.get(url)
    return len(resp.text.split())
{% endhighlight %}

Then, create a RQ queue:

{% highlight python %}
from redis import Redis
from rq import Queue

q = Queue(connection=Redis())
{% endhighlight %}

And enqueue the function call:

{% highlight python %}
from my_module import count_words_at_url
result = q.enqueue(
             count_words_at_url, 'http://nvie.com')
{% endhighlight %}

For a more complete example, refer to the [docs][d].  But this is the essence.

[d]: {{site.baseurl}}docs/


### The worker

To start executing enqueued function calls in the background, start a worker
from your project's directory:

{% highlight console %}
$ rqworker
*** Listening for work on default
Got count_words_at_url('http://nvie.com') from default
Job result = 818
*** Listening for work on default
{% endhighlight %}

That's about it.


## Installation

Simply use the following command to install the latest released version:

    pip install rq

If you want the cutting edge version (that may well be broken), use this:

    pip install -e git+git@github.com:nvie/rq.git@master#egg=rq


## Project history

This project has been inspired by the good parts of [Celery][1], [Resque][2]
and [this snippet][3], and has been created as a lightweight alternative to
existing queueing frameworks, with a low barrier to entry.

[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
[1]: http://www.celeryproject.org/
[2]: https://github.com/defunkt/resque
[3]: http://flask.pocoo.org/snippets/73/
