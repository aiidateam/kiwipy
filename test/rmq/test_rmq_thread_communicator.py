from __future__ import absolute_import
import unittest
import shortuuid

from kiwipy import rmq

from ..utils import CommunicatorTester


class TestRmqThreadCommunicator(CommunicatorTester, unittest.TestCase):
    """Use the standard tests cases to check the RMQ thread communicator"""

    def create_communicator(self):
        message_exchange = "{}.message_exchange.{}".format(self.__class__.__name__, shortuuid.uuid())
        task_exchange = "{}.task_exchange.{}".format(self.__class__.__name__, shortuuid.uuid())
        task_queue = "{}.task_queue.{}".format(self.__class__.__name__, shortuuid.uuid())

        return rmq.RmqThreadCommunicator.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            testing_mode=True)

    def destroy_communicator(self, communicator):
        communicator.stop()
