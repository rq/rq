Welcome to RQ's documentation!
==============================

RQ (*Redis Queue*) is a simple Python library for queueing jobs and
processing them in the background with workers. It is backed by Redis
and it is designed to have a low barrier to entry. It can be integrated
in your web stack easily.

RQ requires Redis >= 3.0.0.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   gettingstarted
   docs/index
   api
   patterns/index
   contrib/index
   chat

Project history
---------------

This project has been inspired by the good parts of
`Celery <https://docs.celeryproject.org/>`__,
`Resque <http://resque.github.io/>`__ and `this
snippet <https://github.com/fengsp/flask-snippets/blob/d1bd8f578253fa952f773429f0168aa471f03cc8/utilities/rq.py>`__,
and has been created as a lightweight alternative to existing queueing
frameworks, with a low barrier to entry.
