Getting started
===============

Installation
------------

Simply use the following command to install the latest released version:

.. code:: console

   $ pip install rq

If you want the cutting edge version (that may well be broken), use
this:

.. code:: console

   $ pip install git+https://github.com/nvie/rq.git@master#egg=rq

Sample Application
------------------

First, run a Redis server. You can use an existing one. To put jobs on
queues, you don’t have to do anything special, just define your
typically lengthy or blocking function:

.. code:: python

   import requests

   def count_words_at_url(url):
       resp = requests.get(url)
       return len(resp.text.split())

Then, create a RQ queue:

.. code:: python

   from redis import Redis
   from rq import Queue

   q = Queue(connection=Redis())

And enqueue the function call:

.. code:: python

   from my_module import count_words_at_url
   result = q.enqueue(count_words_at_url, 'http://nvie.com')

Scheduling jobs are similarly easy:

.. code:: python

   # Schedule job to run at 9:15, October 10th
   job = queue.enqueue_at(datetime(2019, 10, 8, 9, 15), say_hello)

   # Schedule job to be run in 10 seconds
   job = queue.enqueue_in(timedelta(seconds=10), say_hello)

You can also ask RQ to retry failed jobs:

.. code:: python

   from rq import Retry

   # Retry up to 3 times, failed job will be requeued immediately
   queue.enqueue(say_hello, retry=Retry(max=3))

   # Retry up to 3 times, with configurable intervals between retries
   queue.enqueue(say_hello, retry=Retry(max=3, interval=[10, 30, 60]))

The worker
~~~~~~~~~~

To start executing enqueued function calls in the background, start a
worker from your project’s directory:

.. code:: console

   $ rq worker --with-scheduler
   *** Listening for work on default
   Got count_words_at_url('http://nvie.com') from default
   Job result = 818
   *** Listening for work on default

That’s about it.
