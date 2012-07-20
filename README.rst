============
RQ Scheduler
============

`RQ Scheduler <https://github.com/ui/rq-scheduler>`_ is a small package that
adds job scheduling capabilities to `RQ <https://github.com/nvie/rq>`_,
a `Redis <http://redis.io/>`_ based Python queuing library.

============
Requirements
============

* `RQ`_

============
Installation
============

You can install `RQ Scheduler`_ via pip::

    pip install rq-scheduler

Or you can download the latest stable package from `PyPI <http://pypi.python.org/pypi/rq-scheduler>`_.

=====
Usage
=====

Schedule a job involves doing two different things:
# Putting a job in the scheduler
# Running a scheduler that will move scheduled jobs into queues when the time comes

----------------
Scheduling a Job
----------------

There are two ways you can schedule a job. The first is using RQ Scheduler's ``enqueue_at``::

    from rq import use_connection
    from rq_scheduler import Scheduler
    from datetime import datetime

    use_connection() # Use RQ's default Redis connection
    scheduler = Scheduler() # Get a scheduler for the "default" queue

    # Puts a job into the scheduler. The API is similar to RQ except that it
    # takes a datetime object as first argument. So for example to schedule a
    # job to run on Jan 1st 2020 we do:
    scheduler.enqueue_at(datetime(2020, 1, 1), func)

    # Here's another example scheduling a job to run at a specific date and time,
    # complete with args and kwargs
    scheduler.enqueue_at(datetime(2020, 1, 1, 3, 4), func, foo, bar=baz)


The second way is using ``enqueue_in``. Instead of taking a ``datetime`` object,
this method expects a ``timedelta`` and schedules the job to run at
X seconds/minutes/hours/days/weeks later. For example, if we want to monitor how
popular a tweet is a few times during the course of the day, we could do something like::

    from datetime import timedelta

    # Schedule a job to run 10 minutes, 1 hour and 1 day later
    scheduler.enqueue_in(timedelta(minutes=10), count_retweets, tweet_id)
    scheduler.enqueue_in(timedelta(hours=1), count_retweets, tweet_id)
    scheduler.enqueue_in(timedelta(days=1), count_retweets, tweet_id)


You can also explicitly pass in ``connection`` to use a different Redis server::

    from redis import Redis
    from rq_scheduler import Scheduler
    from datetime import datetime

    scheduler = Scheduler('default', connection=Redis('192.168.1.3', port=123))
    scheduler.enqueue_at(datetime(2020, 01, 01, 1, 1), func)

------------------------
Periodic & Repeated Jobs
------------------------

As of version 0.3, `RQ Scheduler`_ also supports creating periodic and repeated jobs.
You can do this via the ``enqueue`` method. This is the syntax::

    scheduler.enqueue(
        scheduled_time=datetime.now(), # Time for first execution
        func=func,                     # Function to be queued
        args=[arg1, arg2],             # Arguments passed into function when executed
        kwargs = {'foo': 'bar'},       # Keyword arguments passed into function when executed
        interval=60,                   # Time before the function is called again, in seconds
        repeat=10                      # Repeat this number of times (None means repeat forever)
    )

Note that the syntax for passing arguments and keyword arguments into the queued
function is slightly different than RQ's. This is deliberatefor readability
reasons and to prevent argument name clashes.

So for example if I want to check the number of retweets every minute::

    scheduler.enqueue(
        datetime.now(),
        count_retweets,
        args=[tweet_id],
        interval=60
    )

If we just want to count my retweets every minute for 10 minutes, we can use the
``repeat`` argument::

    scheduler.enqueue(
        datetime.now(),
        count_retweets,
        args=[tweet_id],
        interval=60,
        repeat=10        # Only do this 10 times
    )

If you prefer ``RQ``'s original syntax, you can achieve the same thing using
``enqueue_periodic``::

    scheduler.enqueue_periodic(
        datetime.now(), # Time for first execution
        interval,       # Time to wait before job is queued again, in seconds
        repeat,         # Number of times function is to be repeated (None means repeat forever)
        func,           # Function to be queued
        *args,          # Arguments passed into function when executed
        **kwargs        # Keyword arguments passed into function when executed
    )

    # Using our retweet counting example:
    scheduler.enqueue(
        datetime.now(),
        60,
        None,
        count_retweets,
        tweet_id,
    )

---------------
Canceling a job
---------------

To cancel a job, simply do:

    scheduler.cancel(job)

---------------------
Running the scheduler
---------------------

`RQ Scheduler`_ comes with a script ``rqscheduler`` that runs a scheduler
process that polls Redis once every minute and move scheduled jobs to the
relevant queues when they need to be executed::

    # This runs a scheduler process using the default Redis connection
    rqscheduler

If you want to use a different Redis server you could also do::

    rqscheduler --host localhost --port 6379 --db 0

The script accepts these arguments:

* ``-H`` or ``--host``: Redis server to connect to
* ``-p`` or ``--port``: port to connect to
* ``-d`` or ``--db``: Redis db to use
* ``-P`` or ``--password``: password to connect to Redis

=========
Changelog
=========

Version 0.3:

* Added the capability to create periodic (cron) and repeated job using ``scheduler.enqueue``
