from os.path import join, dirname, exists

from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '/etc/environment')
if exists(dotenv_path):
    print("loading environment variables from /etc/environment file..")
    load_dotenv(dotenv_path)

# flake8: noqa
from .cli import main

# TODO: the following imports can be removed when we drop the `rqinfo` and
# `rqworkers` commands in favor of just shipping the `rq` command.
from .cli import info, worker
