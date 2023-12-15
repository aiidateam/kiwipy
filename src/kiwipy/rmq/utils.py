# -*- coding: utf-8 -*-
import asyncio
import collections.abc
import functools
import inspect
import logging
import os
import socket
import traceback

import aio_pika

from kiwipy import exceptions

__all__ = ()

# The key used in messages to give information about the host that send a message
HOST_KEY = 'host'
HOSTNAME_KEY = 'hostname'
PID_KEY = 'pid'
RESULT_KEY = 'result'
EXCEPTION_KEY = 'exception'
CANCELLED_KEY = 'cancelled'
PENDING_KEY = 'pending'


def auto_reopen_channel(wrapped):
    """Call the wrapped method and automatically recover from closed channels and connections.

    This decorator should be used on methods that attempt to use the open channel. It calls the method catching
    :class:`aio_pika.ChannelInvalidStateError`, which is thrown when the channel was closed. RabbitMQ will close
    channels if a call is made over it that errors, in order to protect other channels that may be using the same
    connection. The decorator will attempt to reopen the channel. If the connection has also closed, it will wait
    for it to be restored throught the robust connection mechanism.
    """
    logger = logging.getLogger(auto_reopen_channel.__name__)

    async def wrapper(self, *args, **kwargs):
        from aio_pika.exceptions import ChannelInvalidStateError

        while True:
            try:
                return await wrapped(self, *args, **kwargs)
            except ChannelInvalidStateError as exception:
                # This is thrown when the ``Channel`` was closed, so attempt to reopen it.
                logger.exception('Channel was closed: <%s>. Attempting to reopen it.', exception)
                try:
                    await self._channel.reopen()  # pylint: disable=protected-access
                except RuntimeError as exc:
                    logger.exception(
                        'Caught `RuntimeError`: %s . Maybe connection closed, waiting for it to be restored', exc
                    )
                    await asyncio.sleep(2)

    return wrapper


def get_host_info():
    return {'hostname': socket.gethostname(), 'pid': os.getpid()}


def add_host_info(msg):
    if HOST_KEY in msg:
        raise ValueError('Host information key already exists in message')

    msg[HOST_KEY] = get_host_info()


def result_response(result):
    return {RESULT_KEY: result}


def exception_response(exception: Exception, trace=None) -> dict:
    """
    Create an exception response dictionary
    :param exception: The exception to encode
    :param trace: Optional traceback
    :return: An exception response dictionary
    """
    msg = str(exception)
    if trace is not None:
        msg += f"\n{''.join(traceback.format_tb(trace)[0])}"
    return {EXCEPTION_KEY: msg}


def cancelled_response(msg=None):
    return {CANCELLED_KEY: msg}


def pending_response(msg=None):
    return {PENDING_KEY: msg}


def response_result(response):
    future = asyncio.Future()
    response_to_future(response, future)
    return future.result()


def response_to_future(response, future=None):
    """
    Take a response message and set the appropriate value on the given future
    :param response:
    :param future:
    :return:
    """
    if not isinstance(response, collections.abc.Mapping):
        raise TypeError('Response must be a mapping')

    if future is None:
        future = asyncio.Future()

    if CANCELLED_KEY in response:
        future.cancel()
    elif EXCEPTION_KEY in response:
        future.set_exception(exceptions.RemoteException(response[EXCEPTION_KEY]))
    elif RESULT_KEY in response:
        future.set_result(response[RESULT_KEY])
    elif PENDING_KEY in response:
        future.set_result(asyncio.Future())
    else:
        raise ValueError(f"Unknown response type '{response}'")

    return future


def future_to_response(future):
    """
    Convert a future to a response dictionary
    :param future: The future
    :type future: :class:`kiwipy.Future`
    :return: The response dictionary
    :rtype: dict
    """
    if future.cancelled():
        return cancelled_response()
    try:
        return result_response(future.result())
    except Exception as exception:  # pylint: disable=broad-except
        return exception_response(exception)


def ensure_coroutine(coro_or_fn):
    if asyncio.iscoroutinefunction(coro_or_fn):
        return coro_or_fn
    if callable(coro_or_fn):
        if inspect.isclass(coro_or_fn):
            coro_or_fn = coro_or_fn.__call__

        @functools.wraps(coro_or_fn)
        async def wrap(*args, **kwargs):
            return coro_or_fn(*args, **kwargs)

        return wrap

    raise TypeError('coro_or_fn must be a callable')
