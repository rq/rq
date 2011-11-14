# WARNING: DON'T USE THIS IN PRODUCTION (yet)

# rq — Simple job queues for Python

**rq** is an attempt at a lightweight Python job queue, using Redis as the
queue provider.


# Putting jobs on queues

Some terminology before we get started:

* *Queues* are queues, in the computer science way.  Technically, they are
  Redis lists where work is `rpush`'ed on and `lpop`'ed from.
* *Jobs* are a definitions of work that can be carried out by a different
  processes.  Technically, they are just plain old Python function calls, with
  arguments and return values and the like.
* *Workers* are processes that pop off work from queues and start
  executing them.  They report back return values or exceptions.

To put work on queues, tag a Python function call as a job, like so:

    @job('default')
    def slow_fib(n):
        if n <= 1:
            return 1
        else:
            return slow_fib(n-1) + slow_fib(n-2)

You can still call the function synchronously:

    from fib import slow_fib
    slow_fib(4)

You can find an example implementation in the `examples/` directory.  To run
it, open three terminal windows and run the following commands in them:

1. `python example/run_worker.py`
1. `python example/run_worker.py`
1. `python example/run_example.py`

This starts two workers and starts crunching the fibonacci calculations in the
background, while the script shows the crunched data updates every second.


### Installation

Simply use the following command to install the latest released version:

    pip install rq

If you want the cutting edge version (that may well be broken), use this:

    pip install -e git+git@github.com:nvie/rq.git@master#egg=rq

