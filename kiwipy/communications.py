from __future__ import absolute_import
import abc
import concurrent.futures
import sys

import shortuuid
import six

from . import futures

__all__ = [
    'Communicator', 'CommunicatorHelper', 'RemoteException', 'DeliveryFailed', 'TaskRejected', 'UnroutableError',
    'TimeoutError', 'DuplicateSubscriberIdentifier'
]


class RemoteException(Exception):
    """An exception occurred at the remote end of the call """


class DeliveryFailed(Exception):
    """Failed to deliver a message """


class UnroutableError(DeliveryFailed):
    """The messages was unroutable """


class TaskRejected(Exception):
    """ A task was rejected at the remote end """


class DuplicateSubscriberIdentifier(Exception):
    """Failed to add a subscriber because the identifier supplied is already in use"""


TimeoutError = concurrent.futures.TimeoutError  # pylint: disable=redefined-builtin


@six.add_metaclass(abc.ABCMeta)
class Communicator(object):
    """
    The interface for a communicator used to both send and receive various
    types of message.
    """

    @abc.abstractmethod
    def add_rpc_subscriber(self, subscriber, identifier=None):
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
    def add_broadcast_subscriber(self, subscriber, identifier=None):
        """
        Add a broadcast subscriber that will receive all broadcast messages

        :param subscriber: the subscriber function to be called
        :param identifier: an optional identifier for the subscriber
        :return: an identifier for the subscriber and can be subsequently used to remove it
        """
        pass

    @abc.abstractmethod
    def remove_broadcast_subscriber(self, identifier):
        """
        Remove a broadcast subscriber

        :param identifier: the identifier of the subscriber to remove
        """
        pass

    @abc.abstractmethod
    def task_send(self, task, no_reply=False):
        """
        Send a task messages, this will be queued and picked up by a
        worker at some point in the future.  The method returns a future
        representing the outcome of the task.

        :param task: The task message
        :param no_reply: Do not send a reply containing the result of the task
        :type no_reply: bool
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


class CommunicatorHelper(Communicator):
    # Have to disable this linter because this class remains abstract and it is
    # just used by calsses that will themselves be concrete
    # pylint: disable=abstract-method

    def __init__(self):
        self._task_subscribers = []
        self._broadcast_subscribers = {}
        self._rpc_subscribers = {}

    def add_rpc_subscriber(self, subscriber, identifier=None):
        identifier = identifier or shortuuid.uuid()
        if identifier in self._rpc_subscribers:
            raise DuplicateSubscriberIdentifier("RPC identifier '{}'".format(identifier))
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

    def add_broadcast_subscriber(self, subscriber, identifier=None):
        identifier = identifier or shortuuid.uuid()
        if identifier in self._broadcast_subscribers:
            raise DuplicateSubscriberIdentifier("Broadcast identifier '{}'".format(identifier))

        self._broadcast_subscribers[identifier] = subscriber
        return identifier

    def remove_broadcast_subscriber(self, identifier):
        try:
            del self._broadcast_subscribers[identifier]
        except KeyError:
            raise ValueError("Broadcast subscriber '{}' unknown".format(identifier))

    def fire_task(self, msg, no_reply=False):
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

        if no_reply:
            return None

        return future

    def fire_rpc(self, recipient_id, msg):
        try:
            subscriber = self._rpc_subscribers[recipient_id]
        except KeyError:
            raise UnroutableError("Unknown rpc recipient '{}'".format(recipient_id))
        else:
            future = futures.Future()
            try:
                future.set_result(subscriber(self, msg))
            except Exception:  # pylint: disable=broad-except
                future.set_exception(RemoteException(sys.exc_info()))

            return future

    def fire_broadcast(self, body, sender=None, subject=None, correlation_id=None):
        for subscriber in self._broadcast_subscribers.values():
            subscriber(self, body=body, sender=sender, subject=subject, correlation_id=correlation_id)
        return True
