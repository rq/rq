---
title: "RQ: Cron Scheduler"
layout: docs
---

RQ's `CronScheduler` is a simple scheduler that allows you to enqueue functions at regular intervals.

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
from myapp import cleanup_database, send_notifications, send_daily_report

# Run database cleanup every 5 minutes
cron.register(
    cleanup_database,
    queue_name='repeating_tasks',
    interval=300  # 5 minutes in seconds
)

# Send notifications every hour
cron.register(
    send_notifications,
    queue_name='repeating_tasks',
    interval=5  # Every 5 seconds
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

### 2. Start the Scheduler

```sh
rq cron cron_config.py
```

That's it! Your jobs will now be automatically enqueued at the specified intervals.

## Understanding RQ Cron

### How It Works

Key concepts:
- **Interval-based**: jobs run every X seconds (e.g. every 5 seconds). Cron string based job enqueueing is planned for future releases.
- **Separation of concerns**: `CronScheduler` only enqueues jobs; RQ workers handle execution

`CronScheduler` is not a job executor - it's a scheduler to enqueue functions periodically. Here's how the system works:

1. **Registration**: functions are registered along with their intervals and target queues
2. **First run**: registered functions are immediately enqueued when `CronScheduler` starts
3. **Worker execution**: RQ workers pick up and execute the jobs from their queues
4. **Sleep**: `CronScheduler` sleeps until the next job is due

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
    interval=86400,  # Once per day
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
    interval=86400
)

# Start the scheduler (this will block until interrupted)
try:
    cron.start()
except KeyboardInterrupt:
    print("Shutting down cron scheduler...")
```
