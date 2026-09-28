import sys

import pytest

if sys.version_info < (3, 12):
    # due-work-harness needs Python 3.12 or later.
    collect_ignore_glob = ['*.py']
else:
    from due_work_harness import configure
    from due_work_harness.integrations.redis import redis_host
    from due_work_harness.integrations.rq import rq_callback_breaker

    from .connection import CONNECTION

    configure(redis_host(CONNECTION, {'rq'}, receiver_breaker=rq_callback_breaker))

    @pytest.fixture
    def empty_redis():
        CONNECTION.flushdb()
        yield
        CONNECTION.flushdb()
