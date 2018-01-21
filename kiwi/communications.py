import abc
from future.utils import with_metaclass

from . import futures

__all__ = ['Communicator', 'RemoteException', 'Receiver', 'DeliveryFailed', 'TaskRejected']


class RemoteException(Exception):
    pass


class DeliveryFailed(Exception):
    pass


class TaskRejected(Exception):
    pass


class Communicator(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def register_receiver(self, receiver, identifier=None):
        pass

    @abc.abstractmethod
    def add_task_receiver(self, task_receiver):
        pass

    @abc.abstractmethod
    def remove_task_receiver(self, task_receiver):
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

    @abc.abstractmethod
    def broadcast_msg(self, msg, reply_to=None, correlation_id=None):
        pass


class Receiver(object):
    def on_rpc_receive(self, msg):
        """
        Receive a remote procedure call sent directly to this receiver.
        :param msg: The RPC message
        :return: The return value will be returned to the sender
        """
        pass

    def on_broadcast_receive(self, msg):
        """
        Receive a broadcast message.
        :param msg: The broadcast message
        :return:
        """
        pass


class TaskReceiver(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def on_task_received(self, msg):
        """
        Receive a task.
        :param msg: The task message
        :return: The task result or a Future to indicate that the task is in
        progress in which case the response will no be send until it is done
        """
        pass


class Action(futures.Future):
    def execute(self, publisher):
        pass
