from __future__ import absolute_import
import contextlib
import logging
import platform
import sys
import traceback

__all__ = ['Future', 'chain', 'copy_future', 'InvalidStateError', 'CancelledError', 'capture_exceptions']

_LOGGER = logging.getLogger(__name__)

# Can the garbage collector handle cycles that include __del__ methods?
# This is true in cpython beginning with version 3.4 (PEP 442).
_GC_CYCLE_FINALIZERS = (platform.python_implementation() == 'CPython' and sys.version_info >= (3, 4))


class InvalidStateError(BaseException):
    """The operation is not allowed in this state."""
    pass


class CancelledError(Exception):
    """The operation as cancelled ."""
    pass


# States for Future.
_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'


class Future(object):
    """
    A future implementation taken from asyncio but modified to suit our purposes.

    """

    # Class variables serving as defaults for instance variables.
    _state = _PENDING
    _result = None
    _exception = None

    # This field is used for a dual purpose:
    # - Its presence is a marker to declare that a class implements
    #   the Future protocol (i.e. is intended to be duck-type compatible).
    #   The value must also be not-None, to enable a subclass to declare
    #   that it is not compatible by setting this to None.
    # - It is set by __iter__() below so that Task._step() can tell
    #   the difference between
    #   `await Future()` or`yield from Future()` (correct) vs.
    #   `yield Future()` (incorrect).
    _asyncio_future_blocking = False

    __log_traceback = False

    def __init__(self, communicator):
        """Initialize the future.
        The optional event_loop argument allows explicitly setting the event
        loop object used by the future. If it's not provided, the future uses
        the default event loop.

        :param communicator: The communicator this future was created by
        :type communicator: :class:`kiwipy.Communicator`
        """
        self._communicator = communicator
        self._callbacks = []

    def __del__(self):
        if not self.__log_traceback:
            # set_exception() was not called, or result() or exception()
            # has consumed the exception
            return

    @property
    def _log_traceback(self):
        return self.__log_traceback

    @_log_traceback.setter
    def _log_traceback(self, val):
        if bool(val):
            raise ValueError('_log_traceback can only be set to False')
        self.__log_traceback = False

    def cancel(self):
        """Cancel the future and schedule callbacks.
        If the future is already done or cancelled, return False.  Otherwise,
        change the future's state to cancelled, schedule the callbacks and
        return True.
        """
        self.__log_traceback = False
        if self._state != _PENDING:
            return False
        self._state = _CANCELLED
        self.__schedule_callbacks()
        return True

    def __schedule_callbacks(self):
        """Internal: Ask the event loop to call all callbacks.
        The callbacks are scheduled to be called as soon as possible. Also
        clears the callback list.
        """
        callbacks = self._callbacks[:]
        if not callbacks:
            return

        self._callbacks[:] = []
        for callback in callbacks:
            try:
                callback(self)
            except Exception:  # pylint: disable=broad-except
                _LOGGER.warning("Exception calling future callback %s:\n%s", callback, traceback.format_exc())

    def cancelled(self):
        """Return True if the future was cancelled."""
        return self._state == _CANCELLED

    # Don't implement running(); see http://bugs.python.org/issue18699

    def done(self):
        """Return True if the future is done.
        Done means either that a result / exception are available, or that the
        future was cancelled.
        """
        return self._state != _PENDING

    def result(self, timeout=None):
        """Return the result this future represents.
        If the future has been cancelled, raises CancelledError.  If the
        future's result isn't yet available, raises InvalidStateError.  If
        the future is done and has an exception set, this exception is raised.
        """
        if self._state == _CANCELLED:
            raise CancelledError
        if self._state != _FINISHED:
            self._communicator.wait_for(self, timeout)

        self.__log_traceback = False
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self, timeout=None):
        """Return the exception that was set on this future.
        The exception (or None if no exception was set) is returned only if
        the future is done.  If the future has been cancelled, raises
        CancelledError.  If the future isn't done yet, raises
        InvalidStateError.
        """
        if self._state == _CANCELLED:
            raise CancelledError
        if self._state != _FINISHED:
            self._communicator.wait_for(self, timeout)

        self.__log_traceback = False
        return self._exception

    def add_done_callback(self, callback):
        """Add a callback to be run when the future becomes done.
        The callback is called with a single argument - the future object. If
        the future is already done when this is called, the callback is
        scheduled with call_soon.
        """
        if self._state != _PENDING:
            try:
                callback(self)
            except Exception:  # pylint: disable=broad-except
                _LOGGER.warning("Exception calling future callback %s:\n%s", callback, traceback.format_exc())
        else:
            self._callbacks.append(callback)

    def remove_done_callback(self, callback):
        """Remove all instances of a callback from the "call when done" list.
        Returns the number of callbacks removed.
        """
        filtered_callbacks = [f for f in self._callbacks if f != callback]
        removed_count = len(self._callbacks) - len(filtered_callbacks)
        if removed_count:
            self._callbacks[:] = filtered_callbacks
        return removed_count

    # So-called internal methods (note: no set_running_or_notify_cancel()).

    def set_result(self, result):
        """Mark the future done and set its result.
        If the future is already done when this method is called, raises
        InvalidStateError.
        """
        if self._state != _PENDING:
            raise InvalidStateError('{}: {!r}'.format(self._state, self))
        self._result = result
        self._state = _FINISHED
        self.__schedule_callbacks()

    def set_exception(self, exception):
        """Mark the future done and set an exception.
        If the future is already done when this method is called, raises
        InvalidStateError.
        """
        if self._state != _PENDING:
            raise InvalidStateError('{}: {!r}'.format(self._state, self))
        if isinstance(exception, type):
            exception = exception()
        if isinstance(exception, StopIteration):
            raise TypeError("StopIteration interacts badly with generators " "and cannot be raised into a Future")
        self._exception = exception
        self._state = _FINISHED
        self.__schedule_callbacks()
        self.__log_traceback = True

    def __await__(self):
        if not self.done():
            self._asyncio_future_blocking = True
            yield self  # This tells Task to wait for completion.
        if not self.done():
            raise RuntimeError("await wasn't used with future")
        return self.result()  # May raise too.

    __iter__ = __await__  # make compatible with 'yield from'.


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
