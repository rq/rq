# WARNING: DON'T USE THIS IN PRODUCTION (yet)

# rq — Simple job queues for Python

**rq** is a lightweight Python job queue, based on Redis.


# Putting jobs on queues

To put jobs on queues, first declare a Python function call as a job, like so:

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
it, open two terminal windows and run the following commands in them:

1. `python example/run_worker.py`
1. `python example/run_example.py`

This starts two workers and starts crunching the fibonacci calculations in the
background, while the script shows the crunched data updates every second.


### Installation

Simply use the following command to install the latest released version:

    pip install rq

If you want the cutting edge version (that may well be broken), use this:

    pip install -e git+git@github.com:nvie/rq.git@master#egg=rq

