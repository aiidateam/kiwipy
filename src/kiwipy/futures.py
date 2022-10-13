# -*- coding: utf-8 -*-
import concurrent.futures
import contextlib
import logging

__all__ = 'Future', 'chain', 'copy_future', 'CancelledError', 'capture_exceptions', 'wait', 'as_completed'

_LOGGER = logging.getLogger(__name__)

CancelledError = concurrent.futures.CancelledError
wait = concurrent.futures.wait  # pylint: disable=invalid-name
as_completed = concurrent.futures.as_completed  # pylint: disable=invalid-name
Future = concurrent.futures.Future


def copy_future(source, target):
    """ Copy the status of future a to b unless b is already done in
    which case return

    :param source: The source future
    :type source: :class:`kiwipy.Future`
    :param target: The target future
    :type target: :class:`kiwipy.Future`
    """
    if target.done():
        return

    if source.cancelled():
        target.cancel()
    else:
        with capture_exceptions(target):
            target.set_result(source.result())


def chain(source, target):
    """Chain two futures together so that when one completes, so does the other.

    The result (success or failure) of ``a`` will be copied to ``b``, unless
    ``b`` has already been completed or cancelled by the time ``a`` finishes.
    """
    source.add_done_callback(lambda first: copy_future(first, target))


@contextlib.contextmanager
def capture_exceptions(future, ignore=()):
    """
    Capture any exceptions in the context and set them as the result of the given future

    :param future: The future to the exception on
    :type future: :class:`kiwipy.Future`
    :param ignore: An optional list of exception types to ignore, these will be raised and not set on the future
    """
    try:
        yield
    except Exception as exception:  # pylint: disable=broad-except
        if isinstance(exception, ignore):
            raise

        future.set_exception(exception)
