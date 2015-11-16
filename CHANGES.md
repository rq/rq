### 0.10.0
- `@job` decorator now accepts `description`, `meta`, `at_front` and `depends_on` kwargs. Thanks @jlucas91 and @nlyubchich!
- Added the capability to fetch workers by queue using `Worker.all(queue=queue)` and `Worker.count(queue=queue)`.
- Improved RQ's default logging configuration. Thanks @samuelcolvin!
- `job.data` and `job.exc_info` are now stored in compressed format in Redis.

### 0.9.2
- Fixed an issue where `worker.refresh()` may fail when `birth_date` is not set. Thanks @vanife!

### 0.9.1
- Fixed an issue where `worker.refresh()` may fail when upgrading from previous versions of RQ.

### 0.9.0
- `Worker` statistics! `Worker` now keeps track of `last_heartbeat`, `successful_job_count`, `failed_job_count` and `total_working_time`. Thanks @selwin!
- `Worker` now sends heartbeat during suspension check. Thanks @theodesp!
- Added `queue.delete()` method to delete `Queue` objects entirely from Redis. Thanks @theodesp!
- More robust exception string decoding. Thanks @stylight!
- Added `--logging-level` option to command line scripts. Thanks @jiajunhuang!
- Added millisecond precision to job timestamps. Thanks @samuelcolvin!
- Python 2.6 is no longer supported. Thanks @samuelcolvin!

### 0.8.2
- Fixed an issue where `job.save()` may fail with unpickleable return value.

### 0.8.1

- Replace `job.id` with `Job` instance in local `_job_stack `. Thanks @katichev!
- `job.save()` no longer implicitly calls `job.cleanup()`. Thanks @katichev!
- Properly catch `StopRequested` `worker.heartbeat()`. Thanks @fate0!
- You can now pass in timeout in days. Thanks @yaniv-g!
- The core logic of sending job to `FailedQueue` has been moved to `rq.handlers.move_to_failed_queue`. Thanks @yaniv-g!
- RQ cli commands now accept `--path` parameter. Thanks @kirill and @sjtbham!
- Make `job.dependency` slightly more efficient. Thanks @liangsijian!
- `FailedQueue` now returns jobs with the correct class. Thanks @amjith!

### 0.8.0

- Refactored APIs to allow custom `Connection`, `Job`, `Worker` and `Queue` classes via CLI. Thanks @jezdez!
- `job.delete()` now properly cleans itself from job registries. Thanks @selwin!
- `Worker` should no longer overwrite `job.meta`. Thanks @WeatherGod!
- `job.save_meta()` can now be used to persist custom job data. Thanks @katichev!
- Added Redis Sentinel support. Thanks @strawposter!
- Make `Worker.find_by_key()` more efficient. Thanks @selwin!
- You can now specify job `timeout` using strings such as `queue.enqueue(foo, timeout='1m')`. Thanks @luojiebin!
- Better unicode handling. Thanks @myme5261314 and @jaywink!
- Sentry should default to HTTP transport. Thanks @Atala!
- Improve `HerokuWorker` termination logic. Thanks @samuelcolvin!


### 0.7.1

- Fixes a bug that prevents fetching jobs from `FailedQueue` (#765). Thanks @jsurloppe!
- Fixes race condition when enqueueing jobs with dependency (#742). Thanks @th3hamm0r!
- Skip a test that requires Linux signals on MacOS (#763). Thanks @jezdez!
- `enqueue_job` should use Redis pipeline when available (#761). Thanks mtdewulf!

### 0.7.0

- Better support for Heroku workers (#584, #715)
- Support for connecting using a custom connection class (#741)
- Fix: connection stack in default worker (#479, #641)
- Fix: `fetch_job` now checks that a job requested actually comes from the
  intended queue (#728, #733)
- Fix: Properly raise exception if a job dependency does not exist (#747)
- Fix: Job status not updated when horse dies unexpectedly (#710)
- Fix: `request_force_stop_sigrtmin` failing for Python 3 (#727)
- Fix `Job.cancel()` method on failed queue (#707)
- Python 3.5 compatibility improvements (#729)
- Improved signal name lookup (#722)


### 0.6.0

- Jobs that depend on job with result_ttl == 0 are now properly enqueued.
- `cancel_job` now works properly. Thanks @jlopex!
- Jobs that execute successfully now no longer tries to remove itself from queue. Thanks @amyangfei!
- Worker now properly logs Falsy return values. Thanks @liorsbg!
- `Worker.work()` now accepts `logging_level` argument. Thanks @jlopex!
- Logging related fixes by @redbaron4 and @butla!
- `@job` decorator now accepts `ttl` argument. Thanks @javimb!
- `Worker.__init__` now accepts `queue_class` keyword argument. Thanks @antoineleclair!
- `Worker` now saves warm shutdown time. You can access this property from `worker.shutdown_requested_date`. Thanks @olingerc!
- Synchronous queues now properly sets completed job status as finished. Thanks @ecarreras!
- `Worker` now correctly deletes `current_job_id` after failed job execution. Thanks @olingerc!
- `Job.create()` and `queue.enqueue_call()` now accepts `meta` argument. Thanks @tornstrom!
- Added `job.started_at` property. Thanks @samuelcolvin!
- Cleaned up the implementation of `job.cancel()` and `job.delete()`. Thanks @glaslos!
- `Worker.execute_job()` now exports `RQ_WORKER_ID` and `RQ_JOB_ID` to OS environment variables. Thanks @mgk!
- `rqinfo` now accepts `--config` option. Thanks @kfrendrich!
- `Worker` class now has `request_force_stop()` and `request_stop()` methods that can be overridden by custom worker classes. Thanks @samuelcolvin!
- Other minor fixes by @VicarEscaped, @kampfschlaefer, @ccurvey, @zfz, @antoineleclair,
  @orangain, @nicksnell, @SkyLothar, @ahxxm and @horida.


### 0.5.6

- Job results are now logged on `DEBUG` level. Thanks @tbaugis!
- Modified `patch_connection` so Redis connection can be easily mocked
- Customer exception handlers are now called if Redis connection is lost. Thanks @jlopex!
- Jobs can now depend on jobs in a different queue. Thanks @jlopex!

### 0.5.5

(August 25th, 2015)

- Add support for `--exception-handler` command line flag
- Fix compatibility with click>=5.0
- Fix maximum recursion depth problem for very large queues that contain jobs
  that all fail


### 0.5.4

(July 8th, 2015)

- Fix compatibility with raven>=5.4.0


### 0.5.3

(June 3rd, 2015)

- Better API for instantiating Workers. Thanks @RyanMTB!
- Better support for unicode kwargs. Thanks @nealtodd and @brownstein!
- Workers now automatically cleans up job registries every hour
- Jobs in `FailedQueue` now have their statuses set properly
- `enqueue_call()` no longer ignores `ttl`. Thanks @mbodock!
- Improved logging. Thanks @trevorprater!


### 0.5.2

(April 14th, 2015)

- Support SSL connection to Redis (requires redis-py>=2.10)
- Fix to prevent deep call stacks with large queues


### 0.5.1

(March 9th, 2015)

- Resolve performance issue when queues contain many jobs
- Restore the ability to specify connection params in config
- Record `birth_date` and `death_date` on Worker
- Add support for SSL URLs in Redis (and `REDIS_SSL` config option)
- Fix encoding issues with non-ASCII characters in function arguments
- Fix Redis transaction management issue with job dependencies


### 0.5.0
(Jan 30th, 2015)

- RQ workers can now be paused and resumed using `rq suspend` and
  `rq resume` commands. Thanks Jonathan Tushman!
- Jobs that are being performed are now stored in `StartedJobRegistry`
  for monitoring purposes. This also prevents currently active jobs from
  being orphaned/lost in the case of hard shutdowns.
- You can now monitor finished jobs by checking `FinishedJobRegistry`.
  Thanks Nic Cope for helping!
- Jobs with unmet dependencies are now created with `deferred` as their
  status. You can monitor deferred jobs by checking `DeferredJobRegistry`.
- It is now possible to enqueue a job at the beginning of queue using
  `queue.enqueue(func, at_front=True)`. Thanks Travis Johnson!
- Command line scripts have all been refactored to use `click`. Thanks Lyon Zhang!
- Added a new `SimpleWorker` that does not fork when executing jobs.
  Useful for testing purposes. Thanks Cal Leeming!
- Added `--queue-class` and `--job-class` arguments to `rqworker` script.
  Thanks David Bonner!
- Many other minor bug fixes and enhancements.


### 0.4.6
(May 21st, 2014)

- Raise a warning when RQ workers are used with Sentry DSNs using
  asynchronous transports.  Thanks Wei, Selwin & Toms!


### 0.4.5
(May 8th, 2014)

- Fix where rqworker broke on Python 2.6. Thanks, Marko!


### 0.4.4
(May 7th, 2014)

- Properly declare redis dependency.
- Fix a NameError regression that was introduced in 0.4.3.


### 0.4.3
(May 6th, 2014)

- Make job and queue classes overridable. Thanks, Marko!
- Don't require connection for @job decorator at definition time. Thanks, Sasha!
- Syntactic code cleanup.


### 0.4.2
(April 28th, 2014)

- Add missing depends_on kwarg to @job decorator.  Thanks, Sasha!


### 0.4.1
(April 22nd, 2014)

- Fix bug where RQ 0.4 workers could not unpickle/process jobs from RQ < 0.4.


### 0.4.0
(April 22nd, 2014)

- Emptying the failed queue from the command line is now as simple as running
  `rqinfo -X` or `rqinfo --empty-failed-queue`.

- Job data is unpickled lazily. Thanks, Malthe!

- Removed dependency on the `times` library. Thanks, Malthe!

- Job dependencies!  Thanks, Selwin.

- Custom worker classes, via the `--worker-class=path.to.MyClass` command line
  argument.  Thanks, Selwin.

- `Queue.all()` and `rqinfo` now report empty queues, too.  Thanks, Rob!

- Fixed a performance issue in `Queue.all()` when issued in large Redis DBs.
  Thanks, Rob!

- Birth and death dates are now stored as proper datetimes, not timestamps.

- Ability to provide a custom job description (instead of using the default
  function invocation hint).  Thanks, İbrahim.

- Fix: temporary key for the compact queue is now randomly generated, which
  should avoid name clashes for concurrent compact actions.

- Fix: `Queue.empty()` now correctly deletes job hashes from Redis.


### 0.3.13
(December 17th, 2013)

- Bug fix where the worker crashes on jobs that have their timeout explicitly
  removed.  Thanks for reporting, @algrs.


### 0.3.12
(December 16th, 2013)

- Bug fix where a worker could time out before the job was done, removing it
  from any monitor overviews (#288).


### 0.3.11
(August 23th, 2013)

- Some more fixes in command line scripts for Python 3


### 0.3.10
(August 20th, 2013)

- Bug fix in setup.py


### 0.3.9
(August 20th, 2013)

- Python 3 compatibility (Thanks, Alex!)

- Minor bug fix where Sentry would break when func cannot be imported


### 0.3.8
(June 17th, 2013)

- `rqworker` and `rqinfo` have a  `--url` argument to connect to a Redis url.

- `rqworker` and `rqinfo` have a `--socket` option to connect to a Redis server
  through a Unix socket.

- `rqworker` reads `SENTRY_DSN` from the environment, unless specifically
  provided on the command line.

- `Queue` has a new API that supports paging `get_jobs(3, 7)`, which will
  return at most 7 jobs, starting from the 3rd.


### 0.3.7
(February 26th, 2013)

- Fixed bug where workers would not execute builtin functions properly.


### 0.3.6
(February 18th, 2013)

- Worker registrations now expire.  This should prevent `rqinfo` from reporting
  about ghosted workers.  (Thanks, @yaniv-aknin!)

- `rqworker` will automatically clean up ghosted worker registrations from
  pre-0.3.6 runs.

- `rqworker` grew a `-q` flag, to be more silent (only warnings/errors are shown)


### 0.3.5
(February 6th, 2013)

- `ended_at` is now recorded for normally finished jobs, too.  (Previously only
  for failed jobs.)

- Adds support for both `Redis` and `StrictRedis` connection types

- Makes `StrictRedis` the default connection type if none is explicitly provided


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
      from redis import StrictRedis
      from rq.decorators import job

      # Connect to Redis
      redis = StrictRedis()

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
