# -*- coding: utf-8 -*-
import abc
import sys
from typing import Any, Callable

import shortuuid

from . import exceptions, futures
# For backwards compatibility import exceptions too
from .exceptions import *  # pylint: disable=wildcard-import, redefined-builtin, unused-wildcard-import

__all__ = 'Communicator', 'CommunicatorHelper'

# RPC subscriber params: communicator, msg
RpcSubscriber = Callable[['Communicator', Any], Any]
# Task subscriber params: communicator, task
TaskSubscriber = Callable[['Communicator', Any], Any]
# Broadcast subscribers params: communicator, body, sender, subject, correlation id
BroadcastSubscriber = Callable[['Communicator', Any, Any, Any, Any], Any]


class Communicator:
    """
    The interface for a communicator used to both send and receive various types of message.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abc.abstractmethod
    def is_closed(self) -> bool:
        """Return `True` if the communicator was closed"""

    @abc.abstractmethod
    def close(self):
        """Close a communicator, free up all resources and do not allow any further operations"""

    @abc.abstractmethod
    def add_rpc_subscriber(self, subscriber: RpcSubscriber, identifier=None) -> Any:
        """Add an RPC subscriber to the communicator with an optional identifier.  If an identifier
        is not provided the communicator will generate a unique one.  In all cases the identifier
        will be returned."""

    @abc.abstractmethod
    def remove_rpc_subscriber(self, identifier):
        """
        Remove an RPC subscriber given the identifier.  Raises a `ValueError` if there
        is no such subscriber.

        :param identifier: The RPC subscriber identifier
        """

    @abc.abstractmethod
    def add_task_subscriber(self, subscriber: TaskSubscriber, identifier=None) -> Any:
        """Add a task subscriber to the communicator's default queue.  Returns the identifier.

        :param subscriber: The task callback function
        :param identifier: the subscriber identifier
        """

    @abc.abstractmethod
    def remove_task_subscriber(self, identifier):
        """Remove a task subscriber from the communicator's default queue.

        :param identifier: the subscriber to remove
        :raises: ValueError if identifier does not correspond to a known subscriber
        """

    @abc.abstractmethod
    def add_broadcast_subscriber(self, subscriber: BroadcastSubscriber, identifier=None) -> Any:
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
        """Broadcast a message to all subscribers"""


class CommunicatorHelper(Communicator):
    # Have to disable this linter because this class remains abstract and it is
    # just used by classes that will themselves be concrete
    # pylint: disable=abstract-method

    def __init__(self):
        self._task_subscribers = {}
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

    def add_rpc_subscriber(self, subscriber, identifier=None) -> Any:
        self._ensure_open()
        identifier = identifier or shortuuid.uuid()
        if identifier in self._rpc_subscribers:
            raise exceptions.DuplicateSubscriberIdentifier(f"RPC identifier '{identifier}'")
        self._rpc_subscribers[identifier] = subscriber
        return identifier

    def remove_rpc_subscriber(self, identifier):
        self._ensure_open()
        try:
            self._rpc_subscribers.pop(identifier)
        except KeyError as exception:
            raise ValueError(f"Unknown subscriber '{identifier}'") from exception

    def add_task_subscriber(self, subscriber, identifier=None):
        """
        Register a task subscriber

        :param subscriber: The task callback function
        :param identifier: the subscriber identifier
        """
        self._ensure_open()
        identifier = identifier or shortuuid.uuid()
        if identifier in self._rpc_subscribers:
            raise exceptions.DuplicateSubscriberIdentifier(f"RPC identifier '{identifier}'")
        self._task_subscribers[identifier] = subscriber
        return identifier

    def remove_task_subscriber(self, identifier):
        """
        Remove a task subscriber

        :param identifier: the subscriber to remove
        :raises: ValueError if identifier does not correspond to a known subscriber
        """
        self._ensure_open()
        try:
            self._task_subscribers.pop(identifier)
        except KeyError as exception:
            raise ValueError(f"Unknown subscriber: '{identifier}'") from exception

    def add_broadcast_subscriber(self, subscriber: BroadcastSubscriber, identifier=None) -> Any:
        self._ensure_open()
        identifier = identifier or shortuuid.uuid()
        if identifier in self._broadcast_subscribers:
            raise exceptions.DuplicateSubscriberIdentifier(f"Broadcast identifier '{identifier}'")

        self._broadcast_subscribers[identifier] = subscriber
        return identifier

    def remove_broadcast_subscriber(self, identifier):
        self._ensure_open()
        try:
            del self._broadcast_subscribers[identifier]
        except KeyError as exception:
            raise ValueError(f"Broadcast subscriber '{identifier}' unknown") from exception

    def fire_task(self, msg, no_reply=False):
        self._ensure_open()
        future = futures.Future()

        for subscriber in self._task_subscribers.values():
            try:
                result = subscriber(self, msg)
                future.set_result(result)
                break
            except exceptions.TaskRejected:
                pass
            except Exception:  # pylint: disable=broad-except
                future.set_exception(exceptions.RemoteException(sys.exc_info()))
                break

        if no_reply:
            return None

        return future

    def fire_rpc(self, recipient_id, msg):
        self._ensure_open()
        try:
            subscriber = self._rpc_subscribers[recipient_id]
        except KeyError as exception:
            raise exceptions.UnroutableError(f"Unknown rpc recipient '{recipient_id}'") from exception
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
