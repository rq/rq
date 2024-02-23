### RQ 2.0 (unreleased)
* Dropped support for Python 3.6
* Dropped support for Redis server < 4
* Support for multiple job executions. A job can now properly manage multiple executions running simultaneously, allowing future support for long running scheduled jobs.
* [DEPRECATED] `RoundRobinWorker` and `RandomWorker` are deprecated. Use  `--dequeue-strategy <round-robin/random>` instead.

#### Breaking Changes
* `Job.__init__` requires both `id` and `connection` to be passed in.
* `Job.exists()` requires `connection` argument to be passed in.
* `Queue.all()` requires `connection` argument.
* `@job` decorator now requires `connection` argument.

### RQ 1.15.1 (2023-06-20)
* Fixed a bug that may cause a crash when cleaning intermediate queue. Thanks @selwin!
* Fixed a bug that may cause canceled jobs to still run dependent jobs. Thanks @fredsod!

### RQ 1.15 (2023-05-24)
* Added `Callback(on_stopped='my_callback)`. Thanks @eswolinsky3241!
* `Callback` now accepts dotted path to function as input. Thanks @rishabh-ranjan!
* `queue.enqueue_many()` now supports job dependencies. Thanks @eswolinsky3241!
* `rq worker` CLI script now configures logging based on `DICT_CONFIG` key present in config file. Thanks @juur!
* Whenever possible, `Worker` now uses `lmove()` to implement [reliable queue pattern](https://redis.io/commands/lmove/). Thanks @selwin!
* Require `redis>=4.0.0`
* `Scheduler` should only release locks that it successfully acquires. Thanks @xzander!
* Fixes crashes that may happen by changes to `as_text()` function in v1.14. Thanks @tchapi!
* Various linting, CI and code quality improvements. Thanks @robhudson!

### RQ 1.14.1 (2023-05-05)
* Fixes a crash that happens if Redis connection uses SSL. Thanks @tchapi!
* Fixes a crash if `job.meta()` is loaded using the wrong serializer. Thanks @gabriels1234!

### RQ 1.14.0 (2023-05-01)
* Added `WorkerPool` (beta) that manages multiple workers in a single CLI. Thanks @selwin!
* Added a new `Callback` class that allows more flexibility in declaring job callbacks. Thanks @ronlut!
* Fixed a regression where jobs with unserializable return value crashes RQ. Thanks @tchapi!
* Added `--dequeue-strategy` option to RQ's CLI. Thanks @ccrvlh!
* Added `--max-idle-time` option to RQ's worker CLI. Thanks @ronlut!
* Added `--maintenance-interval` option to RQ's worker CLI. Thanks @ronlut!
* Fixed RQ usage in Windows as well as various other refactorings. Thanks @ccrvlh!
* Show more info on `rq info` CLI command. Thanks @iggeehu!
* `queue.enqueue_jobs()` now properly account for job dependencies. Thanks @sim6!
* `TimerDeathPenalty` now properly handles negative/infinite timeout. Thanks @marqueurs404!

### RQ 1.13.0 (2023-02-19)
* Added `work_horse_killed_handler` argument to `Worker`. Thanks @ronlut!
* Fixed an issue where results aren't properly persisted on synchronous jobs. Thanks @selwin!
* Fixed a bug where job results are not properly persisted when `result_ttl` is `-1`. Thanks @sim6!
* Various documentation and logging fixes. Thanks @lowercase00!
* Improve Redis connection reliability. Thanks @lowercase00!
* Scheduler reliability improvements. Thanks @OlegZv and @lowercase00!
* Fixed a bug where `dequeue_timeout` ignores `worker_ttl`. Thanks @ronlut!
* Use `job.return_value()` instead of `job.result` when processing callbacks. Thanks @selwin!
* Various internal refactorings to make `Worker` code more easily extendable. Thanks @lowercase00!
* RQ's source code is now black formatted. Thanks @aparcar!

### RQ 1.12.0 (2023-01-15)
* RQ now stores multiple job execution results. This feature is only available on Redis >= 5.0 Redis Streams. Please refer to [the docs](https://python-rq.org/docs/results/) for more info. Thanks @selwin!
* Improve performance when enqueueing many jobs at once. Thanks @rggjan!
* Redis server version is now cached in connection object. Thanks @odarbelaeze!
* Properly handle `at_front` argument when jobs are scheduled. Thanks @gabriels1234!
* Add type hints to RQ's code base. Thanks @lowercase00!
* Fixed a bug where exceptions are logged twice. Thanks @selwin!
* Don't delete `job.worker_name` after job is finished. Thanks @eswolinsky3241!

### RQ 1.11.1 (2022-09-25)
* `queue.enqueue_many()` now supports `on_success` and on `on_failure` arguments. Thanks @y4n9squared!
* You can now pass `enqueue_at_front` to `Dependency()` objects to put dependent jobs at the front when they are enqueued. Thanks @jtfidje!
* Fixed a bug where workers may wrongly acquire scheduler locks. Thanks @milesjwinter!
* Jobs should not be enqueued if any one of it's dependencies is canceled. Thanks @selwin!
* Fixed a bug when handling jobs that have been stopped. Thanks @ronlut!
* Fixed a bug in handling Redis connections that don't allow `SETNAME` command. Thanks @yilmaz-burak!

### RQ 1.11 (2022-07-31)
* This will be the last RQ version that supports Python 3.5.
* Allow jobs to be enqueued even when their dependencies fail via `Dependency(allow_failure=True)`. Thanks @mattchan-tencent, @caffeinatedMike and @selwin!
* When stopped jobs are deleted, they should also be removed from FailedJobRegistry. Thanks @selwin!
* `job.requeue()` now supports `at_front()` argument. Thanks @buroa!
* Added ssl support for sentinel connections. Thanks @nevious!
* `SimpleWorker` now works better on Windows. Thanks @caffeinatedMike!
* Added `on_failure` and `on_success` arguments to @job decorator. Thanks @nepta1998!
* Fixed a bug in dependency handling. Thanks @th3hamm0r!
* Minor fixes and optimizations by @xavfernandez, @olaure, @kusaku.

### RQ 1.10.1 (2021-12-07)
* **BACKWARDS INCOMPATIBLE**: synchronous execution of jobs now correctly mimics async job execution. Exception is no longer raised when a job fails, job status will now be correctly set to `FAILED` and failure callbacks are now properly called when job is run synchronously. Thanks @ericman93!
* Fixes a bug that could cause job keys to be left over when `result_ttl=0`. Thanks @selwin!
* Allow `ssl_cert_reqs` argument to be passed to Redis. Thanks @mgcdanny!
* Better compatibility with Python 3.10. Thanks @rpkak!
* `job.cancel()` should also remove itself from registries. Thanks @joshcoden!
* Pubsub threads are now launched in `daemon` mode. Thanks @mik3y!

### RQ 1.10.0 (2021-09-09)
* You can now enqueue jobs from CLI. Docs [here](https://python-rq.org/docs/#cli-enqueueing). Thanks @rpkak!
* Added a new `CanceledJobRegistry` to keep track of canceled jobs. Thanks @selwin!
* Added custom serializer support to various places in RQ. Thanks @joshcoden!
* `cancel_job(job_id, enqueue_dependents=True)` allows you to cancel a job while enqueueing its dependents. Thanks @joshcoden!
* Added `job.get_meta()` to fetch fresh meta value directly from Redis. Thanks @aparcar!
* Fixes a race condition that could cause jobs to be incorrectly added to FailedJobRegistry. Thanks @selwin!
* Requeueing a job now clears `job.exc_info`. Thanks @selwin!
* Repo infrastructure improvements by @rpkak.
* Other minor fixes by @cesarferradas and @bbayles.

### RQ 1.9.0 (2021-06-30)
* Added success and failure callbacks. You can now do `queue.enqueue(foo, on_success=do_this, on_failure=do_that)`. Thanks @selwin!
* Added `queue.enqueue_many()` to enqueue many jobs in one go. Thanks @joshcoden!
* Various improvements to CLI commands. Thanks @rpkak!
* Minor logging improvements. Thanks @clavigne and @natbusa!

### RQ 1.8.1 (2021-05-17)
* Jobs that fail due to hard shutdowns are now retried. Thanks @selwin!
* `Scheduler` now works with custom serializers. Thanks @alella!
* Added support for click 8.0. Thanks @rpkak!
* Enqueueing static methods are now supported. Thanks @pwws!
* Job exceptions no longer get printed twice. Thanks @petrem!

### RQ 1.8.0 (2021-03-31)
* You can now declare multiple job dependencies. Thanks @skieffer and @thomasmatecki for laying the groundwork for multi dependency support in RQ.
* Added `RoundRobinWorker` and `RandomWorker` classes to control how jobs are dequeued from multiple queues. Thanks @bielcardona!
* Added `--serializer` option to `rq worker` CLI. Thanks @f0cker!
* Added support for running asyncio tasks. Thanks @MyrikLD!
* Added a new `STOPPED` job status so that you can differentiate between failed and manually stopped jobs. Thanks @dralley!
* Fixed a serialization bug when used with job dependency feature. Thanks @jtfidje!
* `clean_worker_registry()` now works in batches of 1,000 jobs to prevent modifying too many keys at once. Thanks @AxeOfMen and @TheSneak!
* Workers will now wait and try to reconnect in case of Redis connection errors. Thanks @Asrst!

### RQ 1.7.0 (2020-11-29)
* Added `job.worker_name` attribute that tells you which worker is executing a job. Thanks @selwin!
* Added `send_stop_job_command()` that tells a worker to stop executing a job. Thanks @selwin!
* Added `JSONSerializer` as an alternative to the default `pickle` based serializer. Thanks @JackBoreczky!
* Fixes `RQScheduler` running on Redis with `ssl=True`. Thanks @BobReid!

### RQ 1.6.1 (2020-11-08)
* Worker now properly releases scheduler lock when run in burst mode. Thanks @selwin!

### RQ 1.6.0 (2020-11-08)
* Workers now listen to external commands via pubsub. The first two features taking advantage of this infrastructure are `send_shutdown_command()` and `send_kill_horse_command()`. Thanks @selwin!
* Added `job.last_heartbeat` property that's periodically updated when job is running. Thanks @theambient!
* Now horses are killed by their parent group. This helps in cleanly killing all related processes if job uses multiprocessing. Thanks @theambient!
* Fixed scheduler usage with Redis connections that uses custom parser classes. Thanks @selwin!
* Scheduler now enqueue jobs in batches to prevent lock timeouts. Thanks @nikkonrom!
* Scheduler now follows RQ worker's logging configuration. Thanks @christopher-dG!

### RQ 1.5.2 (2020-09-10)
* Scheduler now uses the class of connection that's used. Thanks @pacahon!
* Fixes a bug that puts retried jobs in `FailedJobRegistry`. Thanks @selwin!
* Fixed a deprecated import. Thanks @elmaghallawy!

### RQ 1.5.1 (2020-08-21)
* Fixes for Redis server version parsing. Thanks @selwin!
* Retries can now be set through @job decorator. Thanks @nerok!
* Log messages below logging.ERROR is now sent to stdout. Thanks @selwin!
* Better logger name for RQScheduler. Thanks @atainter!
* Better handling of exceptions thrown by horses. Thanks @theambient!

### RQ 1.5.0 (2020-07-26)
* Failed jobs can now be retried. Thanks @selwin!
* Fixed scheduler on Python > 3.8.0. Thanks @selwin!
* RQ is now aware of which version of Redis server it's running on. Thanks @aparcar!
* RQ now uses `hset()` on redis-py >= 3.5.0. Thanks @aparcar!
* Fix incorrect worker timeout calculation in SimpleWorker.execute_job(). Thanks @davidmurray!
* Make horse handling logic more robust. Thanks @wevsty!

### RQ 1.4.3 (2020-06-28)
* Added `job.get_position()` and `queue.get_job_position()`. Thanks @aparcar!
* Longer TTLs for worker keys to prevent them from expiring inside the worker lifecycle. Thanks @selwin!
* Long job args/kwargs are now truncated during logging. Thanks @JhonnyBn!
* `job.requeue()` now returns the modified job. Thanks @ericatkin!

### RQ 1.4.2 (2020-05-26)
* Reverted changes to `hmset` command which causes workers on Redis server < 4 to crash. Thanks @selwin!
* Merged in more groundwork to enable jobs with multiple dependencies. Thanks @thomasmatecki!

### RQ 1.4.1 (2020-05-16)
* Default serializer now uses `pickle.HIGHEST_PROTOCOL` for backward compatibility reasons. Thanks @bbayles!
* Avoid deprecation warnings on redis-py >= 3.5.0. Thanks @bbayles!

### RQ 1.4.0 (2020-05-13)
* Custom serializer is now supported. Thanks @solababs!
* `delay()` now accepts `job_id` argument. Thanks @grayshirt!
* Fixed a bug that may cause early termination of scheduled or requeued jobs. Thanks @rmartin48!
* When a job is scheduled, always add queue name to a set containing active RQ queue names. Thanks @mdawar!
* Added `--sentry-ca-certs` and `--sentry-debug` parameters to `rq worker` CLI. Thanks @kichawa!
* Jobs cleaned up by `StartedJobRegistry` are given an exception info. Thanks @selwin!
* Python 2.7 is no longer supported. Thanks @selwin!

### RQ 1.3.0 (2020-03-09)
* Support for infinite job timeout. Thanks @theY4Kman!
* Added `__main__` file so you can now do `python -m rq.cli`. Thanks @bbayles!
* Fixes an issue that may cause zombie processes. Thanks @wevsty!
* `job_id` is now passed to logger during failed jobs. Thanks @smaccona!
* `queue.enqueue_at()` and `queue.enqueue_in()` now supports explicit `args` and `kwargs` function invocation. Thanks @selwin!

### RQ 1.2.2 (2020-01-31)
* `Job.fetch()` now properly handles unpickleable return values. Thanks @selwin!

### RQ 1.2.1 (2020-01-31)
* `enqueue_at()` and `enqueue_in()` now sets job status to `scheduled`. Thanks @coolhacker170597!
* Failed jobs data are now automatically expired by Redis. Thanks @selwin!
* Fixes `RQScheduler` logging configuration. Thanks @FlorianPerucki!

### RQ 1.2.0 (2020-01-04)
* This release also contains an alpha version of RQ's builtin job scheduling mechanism. Thanks @selwin!
* Various internal API changes in preparation to support multiple job dependencies. Thanks @thomasmatecki!
* `--verbose` or `--quiet` CLI arguments should override `--logging-level`. Thanks @zyt312074545!
* Fixes a bug in `rq info` where it doesn't show workers for empty queues. Thanks @zyt312074545!
* Fixed `queue.enqueue_dependents()` on custom `Queue` classes. Thanks @van-ess0!
* `RQ` and Python versions are now stored in job metadata. Thanks @eoranged!
* Added `failure_ttl` argument to job decorator. Thanks @pax0r!

### RQ 1.1.0 (2019-07-20)

- Added `max_jobs` to `Worker.work` and `--max-jobs` to `rq worker` CLI. Thanks @perobertson!
- Passing `--disable-job-desc-logging` to `rq worker` now does what it's supposed to do. Thanks @janierdavila!
- `StartedJobRegistry` now properly handles jobs with infinite timeout. Thanks @macintoshpie!
- `rq info` CLI command now cleans up registries when it first runs. Thanks @selwin!
- Replaced the use of `procname` with `setproctitle`. Thanks @j178!


### 1.0 (2019-04-06)
Backward incompatible changes:

- `job.status` has been removed. Use `job.get_status()` and `job.set_status()` instead. Thanks @selwin!

- `FailedQueue` has been replaced with `FailedJobRegistry`:
  * `get_failed_queue()` function has been removed. Please use `FailedJobRegistry(queue=queue)` instead.
  * `move_to_failed_queue()` has been removed.
  * RQ now provides a mechanism to automatically cleanup failed jobs. By default, failed jobs are kept for 1 year.
  * Thanks @selwin!

- RQ's custom job exception handling mechanism has also changed slightly:
  * RQ's default exception handling mechanism (moving jobs to `FailedJobRegistry`) can be disabled by doing `Worker(disable_default_exception_handler=True)`.
  * Custom exception handlers are no longer executed in reverse order.
  * Thanks @selwin!

- `Worker` names are now randomized. Thanks @selwin!

- `timeout` argument on `queue.enqueue()` has been deprecated in favor of `job_timeout`. Thanks @selwin!

- Sentry integration has been reworked:
  * RQ now uses the new [sentry-sdk](https://pypi.org/project/sentry-sdk/) in place of the deprecated [Raven](https://pypi.org/project/raven/) library
  * RQ will look for the more explicit `RQ_SENTRY_DSN` environment variable instead of `SENTRY_DSN` before instantiating Sentry integration
  * Thanks @selwin!

- Fixed `Worker.total_working_time` accounting bug. Thanks @selwin!


### 0.13.0 (2018-12-11)
- Compatibility with Redis 3.0. Thanks @dash-rai!
- Added `job_timeout` argument to `queue.enqueue()`. This argument will eventually replace `timeout` argument. Thanks @selwin!
- Added `job_id` argument to `BaseDeathPenalty` class. Thanks @loopbio!
- Fixed a bug which causes long running jobs to timeout under `SimpleWorker`. Thanks @selwin!
- You can now override worker's name from config file. Thanks @houqp!
- Horses will now return exit code 1 if they don't terminate properly (e.g when Redis connection is lost). Thanks @selwin!
- Added `date_format` and `log_format` arguments to `Worker` and `rq worker` CLI. Thanks @shikharsg!


### 0.12.0 (2018-07-14)
- Added support for Python 3.7. Since `async` is a keyword in Python 3.7,
`Queue(async=False)` has been changed to `Queue(is_async=False)`. The `async`
keyword argument will still work, but raises a `DeprecationWarning`. Thanks @dchevell!


### 0.11.0 (2018-06-01)
- `Worker` now periodically sends heartbeats and checks whether child process is still alive while performing long running jobs. Thanks @Kriechi!
- `Job.create` now accepts `timeout` in string format (e.g `1h`). Thanks @theodesp!
- `worker.main_work_horse()` should exit with return code `0` even if job execution fails. Thanks @selwin!
- `job.delete(delete_dependents=True)` will delete job along with its dependents. Thanks @olingerc!
- Other minor fixes and documentation updates.


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


### 0.5.5 (2015-08-25)

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
