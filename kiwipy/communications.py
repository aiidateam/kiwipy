from __future__ import absolute_import
import abc
import concurrent.futures
import sys
import time

import six

from . import futures

__all__ = [
    'Communicator', 'CommunicatorHelper', 'RemoteException', 'DeliveryFailed', 'TaskRejected', 'UnroutableError',
    'TimeoutError'
]


class RemoteException(Exception):
    """ An exception occurred at the remote end of the call """
    pass


class DeliveryFailed(Exception):
    """ Failed to deliver a message """
    pass


class UnroutableError(DeliveryFailed):
    """ The messages was unroutable """
    pass


class TaskRejected(Exception):
    """ A task was rejected at the remote end """
    pass


TimeoutError = concurrent.futures.TimeoutError  # pylint: disable=redefined-builtin


@six.add_metaclass(abc.ABCMeta)
class Communicator(object):
    """
    The interface for a communicator used to both send and receive various
    types of message.
    """

    @abc.abstractmethod
    def add_rpc_subscriber(self, subscriber, identifier):
        pass

    @abc.abstractmethod
    def remove_rpc_subscriber(self, identifier):
        """
        Remove an RPC subscriber given the identifier.  Raises a `ValueError` if there
        is no such subscriber.

        :param identifier: The RPC subscriber identifier
        """
        pass

    @abc.abstractmethod
    def add_task_subscriber(self, subscriber):
        pass

    @abc.abstractmethod
    def remove_task_subscriber(self, subscriber):
        pass

    @abc.abstractmethod
    def add_broadcast_subscriber(self, subscriber):
        pass

    @abc.abstractmethod
    def remove_broadcast_subscriber(self, subscriber):
        pass

    @abc.abstractmethod
    def task_send(self, msg):
        """
        Send a task messages, this will be queued and picked up by a
        worker at some point in the future.  The method returns a future
        representing the outcome of the task.

        :param msg: The task message
        :return: A future corresponding to the outcome of the task
        :rtype: :class:`kiwi.Future`
        """

    @abc.abstractmethod
    def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient.  This method
        returns a future representing the outcome of the call.

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        :rtype: :class:`kiwi.Future`
        """
        pass

    @abc.abstractmethod
    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        pass

    @abc.abstractmethod
    def wait_for(self, future, timeout=None):
        """
        Wait for a future to complete

        :param future: the future to be waited on
        :param timeout: the timeout
        :type timeout: float
        """
        pass

    def wait_for_many(self, *futs, **kwargs):
        """
        Wait for multiple futures to complete, takes a single additional keyword argument,
        timeout which can be a float.

        :param futs: the futures
        """
        timeout = kwargs.pop('timeout', None)
        if kwargs:
            raise ValueError('Unexpected keyword arguments supplied: {}'.format(kwargs))

        start_time = time.time()
        for future in futs:
            if timeout is not None:
                timeout = timeout - (time.time() - start_time) if timeout is not None else None
                if timeout <= 0.:
                    raise TimeoutError()
            self.wait_for(future, timeout)


class CommunicatorHelper(Communicator):
    # Have to disable this linter because this class remains abstract and it is
    # just used by calsses that will themselves be concrete
    # pylint: disable=abstract-method

    def __init__(self):
        self._task_subscribers = []
        self._broadcast_subscribers = []
        self._rpc_subscribers = {}

    def add_rpc_subscriber(self, subscriber, identifier):
        self._rpc_subscribers[identifier] = subscriber

    def remove_rpc_subscriber(self, identifier):
        try:
            self._rpc_subscribers.pop(identifier)
        except KeyError:
            raise ValueError("Unknown subscriber '{}'".format(identifier))

    def add_task_subscriber(self, subscriber):
        """
        Register a task subscriber

        :param subscriber: The task callback function
        """
        self._task_subscribers.append(subscriber)

    def remove_task_subscriber(self, subscriber):
        """
        Remove a task subscriber

        :param subscriber: The task callback function
        """
        try:
            self._task_subscribers.remove(subscriber)
        except ValueError:
            raise ValueError("Unknown subscriber: '{}'".format(subscriber))

    def add_broadcast_subscriber(self, subscriber):
        self._broadcast_subscribers.append(subscriber)

    def remove_broadcast_subscriber(self, subscriber):
        self._broadcast_subscribers.remove(subscriber)

    def fire_task(self, msg):
        future = futures.Future()
        handled = False

        for subscriber in self._task_subscribers:
            try:
                result = subscriber(self, msg)
                future.set_result(result)
                handled = True
                break
            except TaskRejected:
                pass
            except Exception:  # pylint: disable=broad-except
                future.set_exception(RemoteException(sys.exc_info()))
                handled = True
                break

        if not handled:
            future.set_exception(TaskRejected("Rejected by all subscribers"))

        return future

    def fire_rpc(self, recipient_id, msg):
        try:
            subscriber = self._rpc_subscribers[recipient_id]
        except KeyError:
            raise UnroutableError("Unknown rpc recipient '{}'".format(recipient_id))
        else:
            future = futures.Future()
            with futures.capture_exceptions(future):
                future.set_result(subscriber(self, msg))

            return future

    def fire_broadcast(self, body, sender=None, subject=None, correlation_id=None):
        for subscriber in self._broadcast_subscribers:
            subscriber(self, body=body, sender=sender, subject=subject, correlation_id=correlation_id)
        future = futures.Future()
        future.set_result(True)
        return future
