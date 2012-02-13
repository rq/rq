RQ (_Redis Queue_) is a lightweight<sup>*</sup> Python library for queueing
jobs and processing them in the background with workers.  It is backed by Redis
and it is extremely simple to use.

<p style="font-size: 80%; text-align: right; font-style: italic">
<sup>*</sup> It is under 20 kB in size and just over 500 lines of code.</p>


## Getting started

First, run a Redis server, of course:

    $ redis-server

To put jobs on queues, you don't have to do anything special, just define
your typically lengthy or blocking function:

    import requests

    def count_words_at_url(url):
        resp = requests.get(url)
        return len(resp.text.split())

Then, create a RQ queue:

    import rq import *
    use_redis()
    q = Queue()

And enqueue the function call:

    from my_module import count_words_at_url
    result = q.enqueue(count_words_at_url, 'http://nvie.com')

For a more complete example, refer to the [docs][d].  But this is the essence.

[d]: {{site.baseurl}}docs/


### The worker

To start executing enqueued function calls in the background, start a worker
from your project's directory:

    $ rqworker
    *** Listening for work on default
    Got count_words_at_url('http://nvie.com') from default
    Job result = 818
    *** Listening for work on default

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

[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
[1]: http://www.celeryproject.org/
[2]: https://github.com/defunkt/resque
[3]: http://flask.pocoo.org/snippets/73/
