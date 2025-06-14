RQ (_Redis Queue_) is a simple Python library for queueing jobs and processing
them in the background with workers. It is backed by Redis or Valkey and is designed
to have a low barrier to entry while scaling incredibly well for large applications.
It can be integrated into your web stack easily, making it suitable for projects
of any size—from simple applications to high-volume enterprise systems.

RQ requires Redis >= 5 or Valkey >= 7.2.

[![Build status](https://github.com/rq/rq/workflows/Test/badge.svg)](https://github.com/rq/rq/actions?query=workflow%3A%22Test%22)
[![PyPI](https://img.shields.io/pypi/pyversions/rq.svg)](https://pypi.python.org/pypi/rq)
[![Coverage](https://codecov.io/gh/rq/rq/branch/master/graph/badge.svg)](https://codecov.io/gh/rq/rq)
[![Code style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)


Full documentation can be found [here][d].


## Support RQ

If you find RQ useful, please consider supporting this project via [Tidelift](https://tidelift.com/subscription/pkg/pypi-rq?utm_source=pypi-rq&utm_medium=referral&utm_campaign=readme).


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

Then, create an RQ queue:

```python
from redis import Redis
from rq import Queue

queue = Queue(connection=Redis())
```

And enqueue the function call:

```python
from my_module import count_words_at_url
job = queue.enqueue(count_words_at_url, 'http://nvie.com')
```

## Scheduling Jobs

Scheduling jobs is also easy:

```python
# Schedule job to run at 9:15, October 10th
job = queue.enqueue_at(datetime(2019, 10, 10, 9, 15), say_hello)

# Schedule job to run in 10 seconds
job = queue.enqueue_in(timedelta(seconds=10), say_hello)
```

## Repeating Jobs

To execute a `Job` multiple times, use the `Repeat` class:

```python
from rq import Queue, Repeat

# Repeat job 3 times after successful execution, with 30 second intervals
queue.enqueue(my_function, repeat=Repeat(times=3, interval=30))

# Repeat job 3 times with different intervals between runs
queue.enqueue(my_function, repeat=Repeat(times=3, interval=[5, 10, 15]))
```

## Retrying Failed Jobs

Retrying failed jobs is also supported:

```python
from rq import Retry

# Retry up to 3 times, failed job will be requeued immediately
queue.enqueue(say_hello, retry=Retry(max=3))

# Retry up to 3 times, with configurable intervals between retries
queue.enqueue(say_hello, retry=Retry(max=3, interval=[10, 30, 60]))
```

For a more complete example, refer to the [docs][d].  But this is the essence.

## Cron Style Job Scheduling

To schedule jobs to be enqueued at specific intervals, RQ >= 2.4 now provides a cron-like feature (support for cron syntax coming soon).

First, create a configuration file (e.g., `cron_config.py`) that defines the jobs you want to run periodically.

```python
from rq import cron
from myapp import cleanup_database, send_daily_report

# Run database cleanup every 5 minutes
cron.register(
    cleanup_database,
    queue_name='default',
    interval=300  # 5 minutes in seconds
)

# Send daily reports every 24 hours
cron.register(
    send_daily_report,
    queue_name='repeating_tasks',
    args=('daily',),
    kwargs={'format': 'pdf'},
    interval=86400  # 24 hours in seconds
)
```

And then start the `rq cron` command to enqueue these jobs at specified intervals:

```sh
$ rq cron cron_config.py
```

More details on functionality can be found in the [docs](https://python-rq.org/docs/cron/).

### The Worker

To start executing enqueued function calls in the background, start a worker
from your project's directory:

```console
$ rq worker --with-scheduler
*** Listening for work on default
Got count_words_at_url('http://nvie.com') from default
Job result = 818
*** Listening for work on default
```

To run multiple workers in production, use process managers like `systemd`. RQ also ships with a beta version of `worker-pool` that lets you run multiple worker processes with a single command.

```console
$ rq worker-pool -n 4
```

More options are documented on [python-rq.org](https://python-rq.org/docs/workers/).


## Installation

Simply use the following command to install the latest released version:

    pip install rq

If you want the cutting edge version (that may well be broken), use this:

    pip install git+https://github.com/rq/rq.git@master#egg=rq


## Docs

To build and run the docs, install [jekyll](https://jekyllrb.com/docs/) and run:

```shell
cd docs
jekyll serve
```

## Related Projects

If you use RQ, Check out these below repos which might be useful in your rq based project.

- [django-rq](https://github.com/rq/django-rq)
- [rq-dashboard](https://github.com/Parallels/rq-dashboard)
- [rqmonitor](https://github.com/pranavgupta1234/rqmonitor)
- [Flask-RQ2](https://github.com/rq/Flask-RQ2)
- [rq-scheduler](https://github.com/rq/rq-scheduler)
- [rq-dashboard-fastAPI](https://github.com/Hannes221/rq-dashboard-fast)



## Project history

This project has been inspired by the good parts of [Celery][1], [Resque][2]
and [this snippet][3], and has been created as a lightweight alternative to the
heaviness of Celery or other AMQP-based queueing implementations.

RQ is maintained by [Stamps](https://stamps.id), an Indonesian based company that provides enterprise grade CRM and order management systems.


[d]: http://python-rq.org/
[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
[1]: http://docs.celeryq.dev/
[2]: https://github.com/resque/resque
[3]: https://github.com/fengsp/flask-snippets/blob/1f65833a4291c5b833b195a09c365aa815baea4e/utilities/rq.py
