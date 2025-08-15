---
title: "RQ: Cron Scheduler"
layout: docs
---

RQ's `CronScheduler` allows you to enqueue functions/jobs at regular intervals.

_New in version 2.4.0._

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="/img/warning.png" />
    <strong>Note:</strong>
    <p>`CronScheduler` is still in beta, use at your own risk!</p>
</div>

## Overview

`CronScheduler` provides a lightweight way to enqueue recurring jobs without the complexity of traditional cron systems. It's perfect for:

- Health Checks and Monitoring
- Data Pipeline and ETL Tasks
- Running Maintenance Tasks

Advantages over traditional cron:

1. **Sub-Minute Precision**: enqueue jobs every few seconds (e.g. every 5 seconds), traditional cron is limited to one minute intervals
2. **RQ Integration**: plugs into your existing RQ infrastructure
3. **Fault Tolerance**: jobs benefit from RQ's retry mechanisms and failure handling (soon)
4. **Scalability**: route functions to run in different queues, allowing for better resource management and scaling
5. **Dynamic Configuration**: easily configure functions and intervals without modifying system cron
6. **Job Control**: jobs can have timeouts, TTLs and custom failure/success handling

## Quick Start

### 1. Create a Cron Configuration

Create a cron configuration file (`cron_config.py`):

```python
from rq import cron
from myapp import cleanup_temp_files, send_newsletter

# Clean up temporary files every 30 minutes (interval-based)
cron.register(
    cleanup_temp_files,
    queue_name='maintenance',
    interval=1800  # 30 minutes in seconds
)

# Send newsletter every Tuesday at 10:30 AM (cron string)
cron.register(
    send_newsletter,
    queue_name='email',
    args=('weekly_digest',),
    kwargs={'template': 'newsletter.html'},
    cron_string='30 10 * * 2'
)
```

### 2. Start the Scheduler

```sh
rq cron cron_config.py
```

That's it! Your jobs will now be automatically enqueued at the specified intervals.

## Understanding RQ Cron

### How It Works

Key concepts:
- **Interval based**: jobs run every X seconds (e.g. every 5 seconds).
- **Cron based**: jobs run based on standard cron syntax (e.g. 0 9 * * * for daily at 9 AM)
- **Separation of concerns**: `CronScheduler` only enqueues jobs; RQ workers handle execution

`CronScheduler` is not a job executor - it's a scheduler to periodically enqueue functions. When you run `rq cron`:

1. **Registration**: functions are registered along with their schedules (interval or cron string) and target queues
2. **First run**: interval based functions are immediately enqueued when `CronScheduler` starts
3. **Worker execution**: RQ workers pick up and execute the jobs from their queues (workers need to be run separately)
4. **Sleep**: `CronScheduler` sleeps until the next job is due

## Scheduling Options

`CronScheduler` supports two scheduling methods.

### Interval Based

Use the interval parameter to specify execution frequency in seconds.

```python
cron.register(my_function, queue_name='default', interval=300)  # Every 5 minutes
cron.register(hourly_task, queue_name='hourly', interval=3600)  # Every hour
```

### Cron Based

__New in version 2.5__

Use the cron_string parameter with standard [cron syntax](https://en.wikipedia.org/wiki/Cron).

```python
# Every day at 2:30 AM
cron.register(daily_backup, queue_name='maintenance', cron_string='30 2 * * *')

# Every 15 minutes
cron.register(frequent_task, queue_name='default', cron_string='*/15 * * * *')

# Weekly on Sundays at 6:00 PM
cron.register(weekly_report, queue_name='reports', cron_string='0 18 * * 0')
```


## Configuration Files

### Basic Configuration

Configuration files use the global `register` function to define scheduled jobs:

```python
# my_cron_config.py
from rq.cron import register
from myapp.tasks import cleanup_database, generate_reports, backup_files

# Simple job - runs every 60 seconds
register(cleanup_database, queue_name='maintenance', interval=60)

# Job with arguments
register(
    generate_reports,
    queue_name='reports',
    args=('daily',),
    kwargs={'format': 'pdf', 'email': True},
    interval=3600
)

# Job with custom timeout and TTL settings
register(
    backup_files,
    queue_name='backup',
    cron_string='0 2 * * *',  # Daily at 2 AM
    timeout=1800,    # 30 minutes max execution time
    result_ttl=3600, # Keep results for 1 hour
    failure_ttl=86400 # Keep failed jobs for 1 day
)
```

Since configuration files are standard Python modules, you can include conditional logic. For example:

```python
import os
from rq.cron import register
from myapp.tasks import cleanup

if os.getenv("ENABLE_CLEANUP") == "true":
    register(cleanup, queue_name="maintenance", interval=600)
```

## Command Line Usage

```console
$ rq cron my_cron_config.py
```

You can specify a dotted module path instead of a file path:

```console
$ rq cron myapp.cron_config
```

The `rq cron` CLI command accepts the following options:

- `--url`, `-u`: Redis connection URL (env: RQ_REDIS_URL)
- `--config`, `-c`: Python module with RQ settings (env: RQ_CONFIG)
- `--path`, `-P`: Additional Python import paths (can be specified multiple times)
- `--logging-level`, `-l`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL; default: INFO)
- `--worker-class`, `-w`: Dotted path to RQ Worker class
- `--job-class`, `-j`: Dotted path to RQ Job class
- `--queue-class`: Dotted path to RQ Queue class
- `--connection-class`: Dotted path to Redis client class
- `--serializer`, `-S`: Dotted path to serializer class

Positional argument:

- `config_path`: File path or module path to your cron configuration

Example:

```console
$ rq cron myapp.cron_config --url redis://localhost:6379/1 --path src
```

## Programmatic API

### Basic Usage

```python
from redis import Redis
from rq.cron import CronScheduler
from myapp.tasks import cleanup_database, send_reports

# Create a cron scheduler
redis_conn = Redis(host='localhost', port=6379, db=0)
cron = CronScheduler(connection=redis_conn, logging_level='INFO')

# Register jobs
cron.register(
    cleanup_database,
    queue_name='maintenance',
    interval=3600
)

cron.register(
    send_reports,
    queue_name='reports',
    args=('daily',),
    cron_string='0 9 * * *'  # Daily at 9 AM
)

# Start the scheduler (this will block until interrupted)
try:
    cron.start()
except KeyboardInterrupt:
    print("Shutting down cron scheduler...")
```
