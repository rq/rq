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
