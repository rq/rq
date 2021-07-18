API Reference
=============

.. contents::
   :local:

Classes
-------

Queue
~~~~~

.. autoclass:: rq.Queue
   :members:
   :show-inheritance:

Worker
~~~~~~

.. autoclass:: rq.Worker
   :members:
   :show-inheritance:

SimpleWorker
~~~~~~~~~~~~

.. autoclass:: rq.SimpleWorker
   :show-inheritance:

Job
~~~

.. autoclass:: rq.job.Job
   :members:
   :show-inheritance:

Retry
~~~~~

.. autoclass:: rq.Retry
   :members:
   :show-inheritance:

Connection
~~~~~~~~~~

.. autoclass:: rq.Connection
   :members:
   :show-inheritance:

Functions
---------

Job Functions
~~~~~~~~~~~~~

.. autofunction:: rq.get_current_job

.. autofunction:: rq.requeue_job

.. autofunction:: rq.cancel_job

Connection Functions
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: rq.get_current_connection

.. autofunction:: rq.get_current_job

.. autofunction:: rq.push_connection

.. autofunction:: rq.pop_connection

.. autofunction:: rq.use_connection
