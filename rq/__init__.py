# -*- coding: utf-8 -*-
# flake8: noqa
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from os.path import exists

from dotenv import load_dotenv

dotenv_path = '/etc/environment'
if exists(dotenv_path):
    print("loading environment variables from /etc/environment file..")
    load_dotenv(dotenv_path)

from .connections import (Connection, get_current_connection, pop_connection,
                          push_connection, use_connection)
from .job import cancel_job, get_current_job, requeue_job
from .queue import get_failed_queue, Queue
from .version import VERSION
from .worker import SimpleWorker, Worker

__version__ = VERSION
