Redflow is a simple Python library for queueing jobs with dependencies and
processing them in the background with workers.  It is a fork of the very
easy-to-use redis-backed [RQ][d].

[![Build status](https://api.travis-ci.org/MicahChambers/redflow.svg?branch=master)](https://travis-ci.org/MicahChambers/redflow)
[![Coverage Status](https://coveralls.io/repos/github/MicahChambers/redflow/badge.svg?branch=master)](https://coveralls.io/github/MicahChambers/redflow?branch=master)

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


## Installation

Simply use the following command to install the latest released version:

    pip install rq

If you want the cutting edge version (that may well be broken), use this:

    pip install -e git+git@github.com:nvie/rq.git@master#egg=rq


## Development

    virtualenv .venv
    source .venv/bin/activate
    pip install -r requiments.txt
    python -m rq.cli.cli info

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
