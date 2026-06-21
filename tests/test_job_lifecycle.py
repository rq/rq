from unittest import mock

from rq.job_lifecycle import call_exception_handlers


def test_none_return_continues_chain():
    """A handler returning None does not disable the rest of the chain."""
    job = object()
    first = mock.MagicMock(return_value=None)
    second = mock.MagicMock(return_value=None)

    call_exception_handlers([first, second], job, 'exc')

    first.assert_called_once_with(job, 'exc')
    second.assert_called_once_with(job, 'exc')


def test_truthy_return_continues_chain():
    """A handler returning a truthy value continues to the next handler."""
    job = object()
    first = mock.MagicMock(return_value=True)
    second = mock.MagicMock(return_value=True)

    call_exception_handlers([first, second], job, 'exc')

    first.assert_called_once_with(job, 'exc')
    second.assert_called_once_with(job, 'exc')


def test_falsy_return_stops_chain():
    """A handler returning an explicit falsy value stops the remaining handlers."""
    job = object()
    first = mock.MagicMock(return_value=False)
    second = mock.MagicMock()

    call_exception_handlers([first, second], job, 'exc')

    first.assert_called_once_with(job, 'exc')
    second.assert_not_called()
