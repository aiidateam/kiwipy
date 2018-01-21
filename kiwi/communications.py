import abc
from future.utils import with_metaclass
import sys

from . import futures

__all__ = ['Communicator', 'CommunicatorHelper',
           'RemoteException', 'DeliveryFailed', 'TaskRejected',
           ]


class RemoteException(Exception):
    pass


class DeliveryFailed(Exception):
    pass


class TaskRejected(Exception):
    pass


class Communicator(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def add_rpc_subscriber(self, receiver, identifier):
        pass

    @abc.abstractmethod
    def remove_rpc_subscriber(self, receiver):
        pass

    @abc.abstractmethod
    def add_task_subscriber(self, task_receiver):
        pass

    @abc.abstractmethod
    def remove_task_subscriber(self, task_receiver):
        pass

    @abc.abstractmethod
    def task_send(self, msg):
        """
        Send a task messages, this will be queued and picked up by a
        worker receiver at some point in the future.  When finished
        the task future will have a result.

        :param msg: The task message
        :return: A future corresponding to the outcome of the task
        :rtype: :class:`kiwi.Future`
        """

    def task_send_and_wait(self, msg):
        future = self.task_send(msg)
        self.await_response(future)
        return future.result()

    @abc.abstractmethod
    def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        :rtype: :class:`kiwi.Future`
        """
        pass

    def rpc_send_and_wait(self, recipient_id, msg):
        future = self.rpc_send(recipient_id, msg)
        self.await_response(future)
        return future.result()

    @abc.abstractmethod
    def broadcast_msg(self, msg, reply_to=None, correlation_id=None):
        pass

    @abc.abstractmethod
    def await_response(self, future):
        pass


class CommunicatorHelper(Communicator):
    def __init__(self):
        self._task_subscribers = []
        self._broadcast_subscribers = []
        self._rpc_subscribers = {}

    def add_rpc_subscriber(self, subscriber, identifier):
        self._rpc_subscribers[identifier] = subscriber

    def remove_rpc_subscriber(self, subscriber):
        for identifier, sub in self._rpc_subscribers.items():
            if sub is subscriber:
                self._rpc_subscribers.pop(identifier)
                return
        raise ValueError("Unknown subscriber '{}'".format(subscriber))

    def add_task_subscriber(self, subscriber):
        """
        Register a task subscriber

        :param subscriber: The task callback function
        """
        self._task_subscribers.append(subscriber)

    def remove_task_subscriber(self, subscriber):
        """
        Deregister a task subscriber

        :param subscriber: The task callback function
        """
        # TODO: Put exception guard and raise out own exception
        self._task_subscribers.remove(subscriber)

    def fire_task(self, msg):
        future = futures.Future()

        for subscriber in self._task_subscribers:
            try:
                result = subscriber(msg)
                future.set_result(result)
                break
            except TaskRejected:
                pass
            except Exception:
                future.set_exception(RemoteException(sys.exc_info()))
        if not future.done():
            future.set_exception(TaskRejected("Rejected by all subscribers"))

        return future

    def fire_rpc(self, recipient_id, msg):
        try:
            subscriber = self._rpc_subscribers[recipient_id]
        except KeyError:
            raise ValueError("Unknown recipient '{}'".format(recipient_id))
        else:
            future = futures.Future()
            try:
                result = subscriber(msg)
                if isinstance(result, futures.Future):
                    futures.chain(result, future)
                else:
                    future.set_result(result)
            except Exception:
                future.set_exception(RemoteException(sys.exc_info()))
            return future

    def fire_broadcast(self, msg):
        for subscriber in self._broadcast_subscribers:
            subscriber(msg)
