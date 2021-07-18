Queues
======

A *job* is a Python object, representing a function that is invoked
asynchronously in a worker (background) process. Any Python function can
be invoked asynchronously, by simply pushing a reference to the function
and its arguments onto a queue. This is called *enqueueing*.

Enqueueing Jobs
---------------

To put jobs on queues, first declare a function:

.. code:: python

   import requests

   def count_words_at_url(url):
       resp = requests.get(url)
       return len(resp.text.split())

Noticed anything? There’s nothing special about this function! Any
Python function call can be put on an RQ queue.

To put this potentially expensive word count for a given URL in the
background, simply do this:

.. code:: python

   from rq import Queue
   from redis import Redis
   from somewhere import count_words_at_url
   import time

   # Tell RQ what Redis connection to use
   redis_conn = Redis()
   q = Queue(connection=redis_conn)  # no args implies the default queue

   # Delay execution of count_words_at_url('http://nvie.com')
   job = q.enqueue(count_words_at_url, 'http://nvie.com')
   print(job.result)   # => None

   # Now, wait a while, until the worker is finished
   time.sleep(2)
   print(job.result)   # => 889

If you want to put the work on a specific queue, simply specify its
name:

.. code:: python

   q = Queue('low', connection=redis_conn)
   q.enqueue(count_words_at_url, 'http://nvie.com')

Notice the ``Queue('low')`` in the example above? You can use any queue
name, so you can quite flexibly distribute work to your own desire. A
common naming pattern is to name your queues after priorities
(e.g. ``high``, ``medium``, ``low``).

In addition, you can add a few options to modify the behaviour of the
queued job. By default, these are popped out of the kwargs that will be
passed to the job function.

-  ``job_timeout`` specifies the maximum runtime of the job before it’s
   interrupted and marked as ``failed``. Its default unit is second and
   it can be an integer or a string representing an integer(e.g. ``2``,
   ``'2'``). Furthermore, it can be a string with specify unit including
   hour, minute, second(e.g. ``'1h'``, ``'3m'``, ``'5s'``).
-  ``result_ttl`` specifies how long (in seconds) successful jobs and
   their results are kept. Expired jobs will be automatically deleted.
   Defaults to 500 seconds.
-  ``ttl`` specifies the maximum queued time (in seconds) of the job
   before it’s discarded. This argument defaults to ``None`` (infinite
   TTL).
-  ``failure_ttl`` specifies how long failed jobs are kept (defaults to
   1 year)
-  ``depends_on`` specifies another job (or list of jobs) that must
   complete before this job will be queued.
-  ``job_id`` allows you to manually specify this job’s ``job_id``
-  ``at_front`` will place the job at the *front* of the queue, instead
   of the back
-  ``description`` to add additional description to enqueued jobs.
-  ``on_success`` allows you to run a function after a job completes
   successfully
-  ``on_failure`` allows you to run a function after a job fails
-  ``args`` and ``kwargs``: use these to explicitly pass arguments and
   keyword to the underlying job function. This is useful if your
   function happens to have conflicting argument names with RQ, for
   example ``description`` or ``ttl``.

In the last case, if you want to pass ``description`` and ``ttl``
keyword arguments to your job and not to RQ’s enqueue function, this is
what you do:

.. code:: python

   q = Queue('low', connection=redis_conn)
   q.enqueue(count_words_at_url,
             ttl=30,  # This ttl will be used by RQ
             args=('http://nvie.com',),
             kwargs={
                 'description': 'Function description', # This is passed on to count_words_at_url
                 'ttl': 15  # This is passed on to count_words_at_url function
             })

For cases where the web process doesn’t have access to the source code
running in the worker (i.e. code base X invokes a delayed function from
code base Y), you can pass the function as a string reference, too.

.. code:: python

   q = Queue('low', connection=redis_conn)
   q.enqueue('my_package.my_module.my_func', 3, 4)

Bulk Job Enqueueing
~~~~~~~~~~~~~~~~~~~

| *New in version 1.9.0.*
| You can also enqueue multiple jobs in bulk with
  ``queue.enqueue_many()`` and ``Queue.prepare_data()``:

.. code:: python

   jobs = q.enqueue_many(
     [
       Queue.prepare_data(count_words_at_url, 'http://nvie.com', job_id='my_job_id'),
       Queue.prepare_data(count_words_at_url, 'http://nvie.com', job_id='my_other_job_id'),
     ]
   )

which will enqueue all the jobs in a single redis ``pipeline`` which you
can optionally pass in yourself:

.. code:: python

   with q.connection.pipeline() as pipe:
     jobs = q.enqueue_many(
       [
         Queue.prepare_data(count_words_at_url, 'http://nvie.com', job_id='my_job_id'),
         Queue.prepare_data(count_words_at_url, 'http://nvie.com', job_id='my_other_job_id'),
       ]
       pipeline=pipe
     )
     pipe.execute()

``Queue.prepare_data`` accepts all arguments that ``Queue.parse_args``
does **EXCEPT** for ``depends_on``, which is not supported at this time,
so dependencies will be up to you to setup.

Job dependencies
----------------

RQ allows you to chain the execution of multiple jobs. To execute a job
that depends on another job, use the ``depends_on`` argument:

.. code:: python

   q = Queue('low', connection=my_redis_conn)
   report_job = q.enqueue(generate_report)
   q.enqueue(send_report, depends_on=report_job)

Specifying multiple dependencies are also supported:

.. code:: python

   queue = Queue('low', connection=redis)
   foo_job = queue.enqueue(foo)
   bar_job = queue.enqueue(bar)
   baz_job = queue.enqueue(baz, depends_on=[foo_job, bar_job])

The ability to handle job dependencies allows you to split a big job
into several smaller ones. A job that is dependent on another is
enqueued only when its dependency finishes *successfully*.

Job Callbacks
-------------

*New in version 1.9.0.*

If you want to execute a function whenever a job completes or fails, RQ
provides ``on_success`` and ``on_failure`` callbacks.

.. code:: python

   queue.enqueue(say_hello, on_success=report_success, on_failure=report_failure)

Success Callback
~~~~~~~~~~~~~~~~

Success callbacks must be a function that accepts ``job``,
``connection`` and ``result`` arguments. Your function should also
accept ``*args`` and ``**kwargs`` so your application doesn’t break when
additional parameters are added.

.. code:: python

   def report_success(job, connection, result, *args, **kwargs):
       pass

Success callbacks are executed after job execution is complete, before
dependents are enqueued. If an exception happens when your callback is
executed, job status will be set to ``FAILED`` and dependents won’t be
enqueued.

Callbacks are limited to 60 seconds of execution time. If you want to
execute a long running job, consider using RQ’s job dependency feature
instead.

Failure Callbacks
~~~~~~~~~~~~~~~~~

Failure callbacks are functions that accept ``job``, ``connection``,
``type``, ``value`` and ``traceback`` arguments. ``type``, ``value`` and
``traceback`` values returned by
`sys.exc_info() <https://docs.python.org/3/library/sys.html#sys.exc_info>`__,
which is the exception raised when executing your job.

.. code:: python

   def report_failure(job, connection, type, value, traceback):
       pass

Failure callbacks are limited to 60 seconds of execution time.

Working with Queues
-------------------

Besides enqueuing jobs, Queues have a few useful methods:

.. code:: python

   from rq import Queue
   from redis import Redis

   redis_conn = Redis()
   q = Queue(connection=redis_conn)

   # Getting the number of jobs in the queue
   # Note: Only queued jobs are counted, not including deferred ones
   print(len(q))

   # Retrieving jobs
   queued_job_ids = q.job_ids # Gets a list of job IDs from the queue
   queued_jobs = q.jobs # Gets a list of enqueued job instances
   job = q.fetch_job('my_id') # Returns job having ID "my_id"

   # Emptying a queue, this will delete all jobs in this queue
   q.empty()

   # Deleting a queue
   q.delete(delete_jobs=True) # Passing in `True` will remove all jobs in the queue
   # queue is now unusable. It can be recreated by enqueueing jobs to it.

On the Design
~~~~~~~~~~~~~

With RQ, you don’t have to set up any queues upfront, and you don’t have
to specify any channels, exchanges, routing rules, or whatnot. You can
just put jobs onto any queue you want. As soon as you enqueue a job to a
queue that does not exist yet, it is created on the fly.

RQ does *not* use an advanced broker to do the message routing for you.
You may consider this an awesome advantage or a handicap, depending on
the problem you’re solving.

Lastly, it does not speak a portable protocol, since it depends on
`pickle <https://docs.python.org/library/pickle.html>`__ to serialize the jobs, so it’s a Python-only system.

The delayed result
------------------

When jobs get enqueued, the ``queue.enqueue()`` method returns a ``Job``
instance. This is nothing more than a proxy object that can be used to
check the outcome of the actual job.

For this purpose, it has a convenience ``result`` accessor property,
that will return ``None`` when the job is not yet finished, or a
non-``None`` value when the job has finished (assuming the job *has* a
return value in the first place, of course).

The ``@job`` decorator
----------------------

If you’re familiar with Celery, you might be used to its ``@task``
decorator. Starting from RQ >= 0.3, there exists a similar decorator:

.. code:: python

   from rq.decorators import job

   @job('low', connection=my_redis_conn, timeout=5)
   def add(x, y):
       return x + y

   job = add.delay(3, 4)
   time.sleep(1)
   print(job.result)

Bypassing workers
-----------------

For testing purposes, you can enqueue jobs without delegating the actual
execution to a worker (available since version 0.3.1). To do this, pass
the ``is_async=False`` argument into the Queue constructor:

.. code:: python

   >>> q = Queue('low', is_async=False, connection=my_redis_conn)
   >>> job = q.enqueue(fib, 8)
   >>> job.result
   21

The above code runs without an active worker and executes ``fib(8)``
synchronously within the same process. You may know this behaviour from
Celery as ``ALWAYS_EAGER``. Note, however, that you still need a working
connection to a redis instance for storing states related to job
execution and completion.

The worker
----------

To learn about workers, see the
:ref:`workers <workers>` documentation.

Considerations for jobs
-----------------------

Technically, you can put any Python function call on a queue, but that
does not mean it’s always wise to do so. Some things to consider before
putting a job on a queue:

-  Make sure that the function’s ``__module__`` is importable by the
   worker. In particular, this means that you cannot enqueue functions
   that are declared in the ``__main__`` module.
-  Make sure that the worker and the work generator share *exactly* the
   same source code.
-  Make sure that the function call does not depend on its context. In
   particular, global variables are evil (as always), but also *any*
   state that the function depends on (for example a “current” user or
   “current” web request) is not there when the worker will process it.
   If you want work done for the “current” user, you should resolve that
   user to a concrete instance and pass a reference to that user object
   to the job as an argument.

Limitations
-----------

RQ workers will only run on systems that implement ``fork()``. Most
notably, this means it is not possible to run the workers on Windows
without using the `Windows Subsystem for
Linux <https://docs.microsoft.com/en-us/windows/wsl/about>`__ and
running in a bash shell.
