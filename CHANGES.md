### 0.3.5
(not yet released)

- `ended_at` is now recorded for normally finished jobs, too.  (Previously only
  for failed jobs.)


### 0.3.4
(January 23rd, 2013)

- Restore compatibility with Python 2.6.


### 0.3.3
(January 18th, 2013)

- Fix bug where work was lost due to silently ignored unpickle errors.

- Jobs can now access the current `Job` instance from within.  Relevant
  documentation [here](http://python-rq.org/docs/jobs/).

- Custom properties can be set by modifying the `job.meta` dict.  Relevant
  documentation [here](http://python-rq.org/docs/jobs/).

- Custom properties can be set by modifying the `job.meta` dict.  Relevant
  documentation [here](http://python-rq.org/docs/jobs/).

- `rqworker` now has an optional `--password` flag.

- Remove `logbook` dependency (in favor of `logging`)


### 0.3.2
(September 3rd, 2012)

- Fixes broken `rqinfo` command.

- Improve compatibility with Python < 2.7.



### 0.3.1
(August 30th, 2012)

- `.enqueue()` now takes a `result_ttl` keyword argument that can be used to
  change the expiration time of results.

- Queue constructor now takes an optional `async=False` argument to bypass the
  worker (for testing purposes).

- Jobs now carry status information.  To get job status information, like
  whether a job is queued, finished, or failed, use the property `status`, or
  one of the new boolean accessor properties `is_queued`, `is_finished` or
  `is_failed`.

- Jobs return values are always stored explicitly, even if they have to
  explicit return value or return `None` (with given TTL of course).  This
  makes it possible to distinguish between a job that explicitly returned
  `None` and a job that isn't finished yet (see `status` property).

- Custom exception handlers can now be configured in addition to, or to fully
  replace, moving failed jobs to the failed queue.  Relevant documentation
  [here](http://python-rq.org/docs/exceptions/) and
  [here](http://python-rq.org/patterns/sentry/).

- `rqworker` now supports passing in configuration files instead of the
  many command line options: `rqworker -c settings` will source
  `settings.py`.

- `rqworker` now supports one-flag setup to enable Sentry as its exception
  handler: `rqworker --sentry-dsn="http://public:secret@example.com/1"`
  Alternatively, you can use a settings file and configure `SENTRY_DSN
  = 'http://public:secret@example.com/1'` instead.


### 0.3.0
(August 5th, 2012)

- Reliability improvements

    - Warm shutdown now exits immediately when Ctrl+C is pressed and worker is idle
    - Worker does not leak worker registrations anymore when stopped gracefully

- `.enqueue()` does not consume the `timeout` kwarg anymore.  Instead, to pass
  RQ a timeout value while enqueueing a function, use the explicit invocation
  instead:

      ```python
      q.enqueue(do_something, args=(1, 2), kwargs={'a': 1}, timeout=30)
      ```

- Add a `@job` decorator, which can be used to do Celery-style delayed
  invocations:

      ```python
      from redis import Redis
      from rq.decorators import job

      # Connect to Redis
      redis = Redis()

      @job('high', timeout=10, connection=redis)
      def some_work(x, y):
          return x + y
      ```

  Then, in another module, you can call `some_work`:

      ```python
      from foo.bar import some_work

      some_work.delay(2, 3)
      ```


### 0.2.2
(August 1st, 2012)

- Fix bug where return values that couldn't be pickled crashed the worker


### 0.2.1
(July 20th, 2012)

- Fix important bug where result data wasn't restored from Redis correctly
  (affected non-string results only).


### 0.2.0
(July 18th, 2012)

- `q.enqueue()` accepts instance methods now, too.  Objects will be pickle'd
  along with the instance method, so beware.
- `q.enqueue()` accepts string specification of functions now, too.  Example:
  `q.enqueue("my.math.lib.fibonacci", 5)`.  Useful if the worker and the
  submitter of work don't share code bases.
- Job can be assigned custom attrs and they will be pickle'd along with the
  rest of the job's attrs.  Can be used when writing RQ extensions.
- Workers can now accept explicit connections, like Queues.
- Various bug fixes.


### 0.1.2
(May 15, 2012)

- Fix broken PyPI deployment.


### 0.1.1
(May 14, 2012)

- Thread-safety by using context locals
- Register scripts as console_scripts, for better portability
- Various bugfixes.


### 0.1.0:
(March 28, 2012)

- Initially released version.
