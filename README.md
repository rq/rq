RQ (_Redis Queue_) is a simple Python library for queueing jobs and processing
them in the background with workers.  It is backed by Redis and it is designed
to have a low barrier to entry.  It should be integrated in your web stack
easily.

RQ requires Redis >= 2.7.0.

[![Build status](https://travis-ci.org/nvie/rq.svg?branch=master)](https://secure.travis-ci.org/nvie/rq)
[![Downloads](https://img.shields.io/pypi/dm/rq.svg)](https://pypi.python.org/pypi/rq)
[![Can I Use Python 3?](https://caniusepython3.com/project/rq.svg)](https://caniusepython3.com/project/rq)
[![Coverage Status](https://img.shields.io/coveralls/nvie/rq.svg)](https://coveralls.io/r/nvie/rq)

Full documentation can be found [here][d].


## Getting started

First, run a Redis server, of course:

```console
$ redis-server
```

To put jobs on queues, you don't have to do anything special, just define
your typically lengthy or blocking function:

```python
import requests

def count_words_at_url(url):
    """Just an example function that's called async."""
    resp = requests.get(url)
    return len(resp.text.split())
```

You do use the excellent [requests][r] package, don't you?

Then, create an RQ queue:

```python
from redis import Redis
from rq import Queue

q = Queue(connection=Redis())
```

And enqueue the function call:

```python
from my_module import count_words_at_url
result = q.enqueue(count_words_at_url, 'http://nvie.com')
```

For a more complete example, refer to the [docs][d].  But this is the essence.


### The worker

To start executing enqueued function calls in the background, start a worker
from your project's directory:

```console
$ rq worker
*** Listening for work on default
Got count_words_at_url('http://nvie.com') from default
Job result = 818
*** Listening for work on default
```

That's about it.

---------------------

By the way, if you want to use gevent, first please install gevent:

```console
$ pip install gevent
```

then use the worker in `worker.GeventWorker` in your custom code like:

```python
# remember to define your constants
WORKER_BROKER="redis://127.0.0.1:6379/0"

def gevent_worker(queues):
    worker = GeventWorker(
        queues=queues,
        connection=StrictRedis.from_url(WORKER_BROKER)
    )
    worker.work()

# and start your worker like:
if __name__ == "__main__":
    gevent_worker()
```


## Installation

Simply use the following command to install the latest released version:

    pip install rq

If you want the cutting edge version (that may well be broken), use this:

    pip install -e git+https://github.com/nvie/rq.git@master#egg=rq


## Project history

This project has been inspired by the good parts of [Celery][1], [Resque][2]
and [this snippet][3], and has been created as a lightweight alternative to the
heaviness of Celery or other AMQP-based queueing implementations.

[r]: http://python-requests.org
[d]: http://python-rq.org/
[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
[1]: http://www.celeryproject.org/
[2]: https://github.com/resque/resque
[3]: http://flask.pocoo.org/snippets/73/
