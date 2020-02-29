import abc
import sys

import shortuuid

from . import exceptions
from . import futures

__all__ = 'Communicator', 'CommunicatorHelper'


class Communicator:
    """
    The interface for a communicator used to both send and receive various types of message.
    """

    @abc.abstractmethod
    def is_closed(self) -> bool:
        """Return `True` if the event loop was closed"""

    @abc.abstractmethod
    def close(self):
        """Close a communicator, free up all resources and do not allow any further operations"""

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

    @abc.abstractmethod
    def remove_broadcast_subscriber(self, identifier):
        """
        Remove a broadcast subscriber

        :param identifier: the identifier of the subscriber to remove
        """

    @abc.abstractmethod
    def task_send(self, task, no_reply=False) -> futures.Future:
        """
        Send a task messages, this will be queued and picked up by a
        worker at some point in the future.  The method returns a future
        representing the outcome of the task.

        :param task: The task message
        :param no_reply: Do not send a reply containing the result of the task
        :type no_reply: bool
        :return: A future corresponding to the outcome of the task
        """

    @abc.abstractmethod
    def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient.  This method
        returns a future representing the outcome of the call.

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        :rtype: :class:`kiwipy.Future`
        """

    @abc.abstractmethod
    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None) -> bool:
        pass


class CommunicatorHelper(Communicator):
    # Have to disable this linter because this class remains abstract and it is
    # just used by calsses that will themselves be concrete
    # pylint: disable=abstract-method

    def __init__(self):
        self._task_subscribers = []
        self._broadcast_subscribers = {}
        self._rpc_subscribers = {}
        self._closed = False

    def is_closed(self) -> bool:
        return self._closed

    def close(self):
        if self._closed:
            return
        self._closed = True
        del self._task_subscribers
        del self._broadcast_subscribers
        del self._rpc_subscribers

    def add_rpc_subscriber(self, subscriber, identifier=None):
        self._ensure_open()
        identifier = identifier or shortuuid.uuid()
        if identifier in self._rpc_subscribers:
            raise exceptions.DuplicateSubscriberIdentifier("RPC identifier '{}'".format(identifier))
        self._rpc_subscribers[identifier] = subscriber

    def remove_rpc_subscriber(self, identifier):
        self._ensure_open()
        try:
            self._rpc_subscribers.pop(identifier)
        except KeyError:
            raise ValueError("Unknown subscriber '{}'".format(identifier))

    def add_task_subscriber(self, subscriber):
        """
        Register a task subscriber

        :param subscriber: The task callback function
        """
        self._ensure_open()
        self._task_subscribers.append(subscriber)

    def remove_task_subscriber(self, subscriber):
        """
        Remove a task subscriber

        :param subscriber: The task callback function
        """
        self._ensure_open()
        try:
            self._task_subscribers.remove(subscriber)
        except ValueError:
            raise ValueError("Unknown subscriber: '{}'".format(subscriber))

    def add_broadcast_subscriber(self, subscriber, identifier=None):
        self._ensure_open()
        identifier = identifier or shortuuid.uuid()
        if identifier in self._broadcast_subscribers:
            raise exceptions.DuplicateSubscriberIdentifier("Broadcast identifier '{}'".format(identifier))

        self._broadcast_subscribers[identifier] = subscriber
        return identifier

    def remove_broadcast_subscriber(self, identifier):
        self._ensure_open()
        try:
            del self._broadcast_subscribers[identifier]
        except KeyError:
            raise ValueError("Broadcast subscriber '{}' unknown".format(identifier))

    def fire_task(self, msg, no_reply=False):
        self._ensure_open()
        future = futures.Future()
        handled = False

        for subscriber in self._task_subscribers:
            try:
                result = subscriber(self, msg)
                future.set_result(result)
                handled = True
                break
            except exceptions.TaskRejected:
                pass
            except Exception:  # pylint: disable=broad-except
                future.set_exception(exceptions.RemoteException(sys.exc_info()))
                handled = True
                break

        if not handled:
            future.set_exception(exceptions.TaskRejected("Rejected by all subscribers"))

        if no_reply:
            return None

        return future

    def fire_rpc(self, recipient_id, msg):
        self._ensure_open()
        try:
            subscriber = self._rpc_subscribers[recipient_id]
        except KeyError:
            raise exceptions.UnroutableError("Unknown rpc recipient '{}'".format(recipient_id))
        else:
            future = futures.Future()
            try:
                future.set_result(subscriber(self, msg))
            except Exception:  # pylint: disable=broad-except
                future.set_exception(exceptions.RemoteException(sys.exc_info()))

            return future

    def fire_broadcast(self, body, sender=None, subject=None, correlation_id=None):
        self._ensure_open()
        for subscriber in self._broadcast_subscribers.values():
            subscriber(self, body=body, sender=sender, subject=subject, correlation_id=correlation_id)
        return True

    def _ensure_open(self):
        if self.is_closed():
            raise exceptions.CommunicatorClosed
