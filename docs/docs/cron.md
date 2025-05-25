---
title: "RQ: Cron Scheduler"
layout: docs
---

RQ Cron is a simple interval-based job scheduler for RQ. It allows you to define functions that should be executed at regular intervals and automatically enqueues them to the appropriate queues at the scheduled times.

_New in version 2.4.0._

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="/img/warning.png" />
    <strong>Note:</strong>
    <p>`Cron` is still in beta, use at your own risk!</p>
</div>

## Quick Start

Create a cron configuration (`cron_config.py`):

```python
from rq import cron
from myapp import cleanup_database

# Run `cleanup_database` every 60 seconds on the 'maintenance' queue
cron.register(
    cleanup_database,
    queue_name='maintenance',
    interval=60
)
```

Start the scheduler:

```sh
rq cron cron_config.py
```

## Cron Configuration File

You can configure multiple jobs and schedules in a single file using the `register` function.
This is especially convenient when your web or application needs to run several background tasks.

Here's an example:

```python
# my_cron_config.py
from rq.cron import register
from myapp.tasks import cleanup_database, send_reports

# Register a job to run every 60 seconds
register(cleanup_database, queue_name='maintenance', interval=60)

# Register a job with arguments to run every 3600 seconds (1 hour)
register(
    send_reports,
    queue_name='reports',
    args=('daily',),
    kwargs={'format': 'pdf'},
    interval=3600
)
```

The cron scheduler will load this file and queue all the jobs with the specified intervals.

### Note on Cron Syntax

While this feature is named "cron", it currently only supports interval-based scheduling (running jobs every X seconds). Support for cron syntax (e.g., `0 0 * * *`) is planned for a future release.

### Register Parameters

When registering a job, you can specify the following parameters:

* `func`: The function to execute (required)
* `queue_name`: Name of the queue to enqueue the job to (required)
* `args`: Tuple of positional arguments to pass to the function (default: `()`)
* `kwargs`: Dictionary of keyword arguments to pass to the function (default: `{}`)
* `interval`: Time in seconds between job executions (default: `None`)
* `timeout`: Maximum runtime of the job before it's marked as failed (default: `None`)
* `result_ttl`: How long (in seconds) successful job results are kept (default: `500`)
* `ttl`: Maximum queued time for the job before it's discarded (default: `None`)
* `failure_ttl`: How long (in seconds) failed jobs are kept (default: `None`)
* `meta`: Additional metadata to store with the job (default: `None`)


## Starting the Cron Scheduler

To start the RQ cron scheduler, use the `rq cron` command followed by the path to your cron configuration file:

```console
$ rq cron my_cron_config.py
```

You can also specify a dotted module path:

```console
$ rq cron myapp.cron_config
```

### Command Line Options

The cron scheduler supports the following command line options:

* `--logging-level` or `-l`: set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is INFO.
* `--path` or `-P`: specify additional import paths to look for modules. You can specify this option multiple times (e.g., `--path app --path lib`).
* `--url` or `-u`: URL describing Redis connection details (e.g., `redis://username:password@host:port/db`).
* `--config` or `-c`: module containing RQ settings.
* `--serializer` or `-S`: path to serializer (defaults to `rq.serializers.DefaultSerializer`).

The `--path` option is particularly important when your cron configuration or jobs are in a package that's not in the default Python path.

## Using the Cron API Programmatically

You can also create and manage cron jobs programmatically:

```python
from redis import Redis
from rq.cron import Cron
from myapp.tasks import cleanup_database

# Create a cron scheduler
cron = Cron(connection=Redis())

# Register a job to run every 60 seconds
cron_job = cron.register(
    cleanup_database,
    queue_name='maintenance',
    interval=60
)

# Start the scheduler (this will block until interrupted)
cron.start()
```

### Loading Configuration Files

If you need to load your cron configuration from a file programmatically:

```python
from redis import Redis
from rq.cron import Cron

cron = Cron(connection=Redis())

# Load cron jobs from a configuration file
cron.load_config_from_file('cron_config.py')

# Load cron jobs from a module path
cron.load_config_from_file('myapp.cron_config')

# Start the scheduler
cron.start()
```
