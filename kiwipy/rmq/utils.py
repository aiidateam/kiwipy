import collections
import kiwipy
import os
import socket
from tornado import concurrent, gen

# The key used in messages to give information about the host that send a message
HOST_KEY = 'host'
HOSTNAME_KEY = 'hostname'
PID_KEY = 'pid'
RESULT_KEY = 'result'
EXCEPTION_KEY = 'exception'
CANCELLED_KEY = 'cancelled'
PENDING_KEY = 'pending'


def get_host_info():
    return {
        'hostname': socket.gethostname(),
        'pid': os.getpid()
    }


def add_host_info(msg):
    if HOST_KEY in msg:
        raise ValueError("Host information key already exists in message")

    msg[HOST_KEY] = get_host_info()


def result_response(result):
    return {RESULT_KEY: result}


def exception_response(exception, trace=None):
    """
    Create an exception response dictionary
    :param exception: The exception to encode
    :type exception: :class:`Exception`
    :param trace: Optional traceback
    :return: A response dictionary
    :rtype: dict
    """
    msg = str(exception)
    if trace is not None:
        msg += "\n{}".format(trace)
    return {EXCEPTION_KEY: msg}


def cancelled_response(msg=None):
    return {CANCELLED_KEY: msg}


def pending_response(msg=None):
    return {PENDING_KEY: msg}


def response_result(response):
    future = concurrent.Future()
    response_to_future(response, future)
    return future.result()


def response_to_future(response, future=None):
    """
    Take a response message and set the appropriate value on the given future
    :param response:
    :param future:
    :return:
    """
    if not isinstance(response, collections.Mapping):
        raise TypeError("Response must be a mapping")

    if future is None:
        future = concurrent.Future()

    if CANCELLED_KEY in response:
        future.cancel()
    elif EXCEPTION_KEY in response:
        future.set_exception(kiwipy.RemoteException(response[EXCEPTION_KEY]))
    elif RESULT_KEY in response:
        future.set_result(response[RESULT_KEY])
    elif PENDING_KEY in response:
        future.set_result(concurrent.Future())
    else:
        raise ValueError("Unknown response type '{}'".format(response))

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
    except Exception as e:
        return exception_response(e)


def tornado_to_kiwi_future(tornado_future):
    """
    :type tornado_future: :class:`tornado.concurrent.Future`
    :rtype: :class:`kiwipy.Future`
    """
    kiwi_future = kiwipy.Future()

    def done(done_future):
        # Copy over the future
        try:
            result = done_future.result()
            if concurrent.is_future(result):
                # Change the future type to a kiwi one
                result = tornado_to_kiwi_future(result)
            kiwi_future.set_result(result)
        except kiwipy.CancelledError:
            kiwi_future.cancel()
        except Exception as e:
            kiwi_future.set_exception(e)

    tornado_future.add_done_callback(done)
    return kiwi_future


def kiwi_to_tornado_future(kiwi_future):
    tornado_future = concurrent.Future()

    def done(done_future):
        if done_future.cancelled():
            tornado_future.cancel()
        try:
            tornado_future.set_result(done_future.result())
        except Exception as e:
            tornado_future.set_exception(e)

    kiwi_future.add_done_callback(done)

    return tornado_future


def ensure_coroutine(coro_or_fn):
    if gen.is_coroutine_function(coro_or_fn):
        return coro_or_fn
    else:
        return gen.coroutine(coro_or_fn)
