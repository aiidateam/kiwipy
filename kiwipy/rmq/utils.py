import functools
import inspect
import kiwipy
import os
import socket

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


def exception_response(exception):
    return {EXCEPTION_KEY: str(exception)}


def cancelled_response(msg=None):
    return {CANCELLED_KEY: msg}


def pending_response(msg=None):
    return {PENDING_KEY: msg}


def response_result(response):
    future = kiwipy.Future()
    response_to_future(response, future)
    return future.result()


def response_to_future(response, future=None):
    if future is None:
        future = kiwipy.Future()

    if CANCELLED_KEY in response:
        future.cancel()
    elif EXCEPTION_KEY in response:
        future.set_exception(kiwipy.RemoteException(response[EXCEPTION_KEY]))
    elif RESULT_KEY in response:
        future.set_result(response[RESULT_KEY])
    elif not PENDING_KEY in response:
        raise ValueError("Unknown response type '{}'".format(response))

    return future


def _get_initialisers(rmq_instance):
    def is_initialiser(fn):
        try:
            return inspect.ismethod(fn) and fn.__is_initailiser
        except AttributeError:
            return False

    return inspect.getmembers(rmq_instance, predicate=is_initialiser)


def initialiser():
    def wrapper(wrapped):
        @functools.wraps(wrapped)
        def init_fn(self, *a, **kw):
            if not self._in_init_method:
                self._in_init_method = True
                try:
                    wrapped(self, *a, **kw)
                except Exception:
                    import sys
                    self._initialising.set_exc_info(sys.exc_info())
                else:
                    self._initialised(wrapped)
                finally:
                    self._in_init_method = False
            else:
                wrapped(self, *a, **kw)

        init_fn.__is_initailiser = True
        return init_fn

    return wrapper


class InitialisationMixin(object):
    def __init__(self, *args, **kwargs):
        super(InitialisationMixin, self).__init__(*args, **kwargs)
        # Make a set of the intiialiser method names
        self._initialisers = set(method[0] for method in _get_initialisers(self))
        self._initialising = kiwipy.Future()
        self._in_init_method = False

    def initialised_future(self):
        return self._initialising

    def reinitialising(self):
        self._initialising = kiwipy.Future()

    def _initialised(self, fn):
        self._initialisers.remove(fn.__name__)
        if not self._initialisers:
            self._initialising.set_result(True)
