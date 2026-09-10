import asyncio

import pytest
from redis import Redis

from rq.job import Job, get_current_job
from tests import fixtures


def close_test_loop(loop):
    # Clean up the unfixed implementation too, so regressions do not leak resources.
    if not loop.is_closed():
        tasks = asyncio.all_tasks(loop)
        for task in tasks:
            task.cancel()
        if tasks:
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()


@pytest.mark.parametrize('error', [None, ValueError('job failed'), asyncio.CancelledError()])
def test_coroutine_job_closes_event_loop(error):
    state = {}
    # Job.perform executes locally; this client is never connected to a server.
    with Redis() as connection:
        job = Job.create(fixtures.record_job_loop, args=(state, error), connection=connection)
        try:
            if error is None:
                assert job.perform() == 42
            else:
                with pytest.raises(type(error)) as raised:
                    job.perform()
                if isinstance(error, Exception):
                    assert raised.value is error
            assert state['job'] is job
            assert get_current_job() is None
            assert state['loop'].is_closed()
        finally:
            close_test_loop(state['loop'])


def test_coroutine_job_finalizes_async_resources():
    state = {}
    with Redis() as connection:
        job = Job.create(fixtures.leave_async_resources, args=(state,), connection=connection)
        try:
            assert job.perform() == 42
            assert state.get('task_closed') is True
            assert state.get('generator_closed') is True
            assert state['task'].done()
        finally:
            close_test_loop(state['loop'])
