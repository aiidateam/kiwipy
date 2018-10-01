from __future__ import absolute_import
import concurrent.futures
import contextlib
import logging

__all__ = ['Future', 'chain', 'copy_future', 'CancelledError', 'capture_exceptions']

_LOGGER = logging.getLogger(__name__)

CancelledError = concurrent.futures.CancelledError
Future = concurrent.futures.Future


def copy_future(source, target):
    """ Copy the status of future a to b unless b is already done in
    which case return

    :param source: The source future
    :type source: :class:`Future`
    :param target: The target future
    :type target: :class:`Future`
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
def capture_exceptions(future):
    try:
        yield
    except Exception as exception:  # pylint: disable=broad-except
        future.set_exception(exception)
