from __future__ import absolute_import
import unittest
import shortuuid

import kiwipy
from kiwipy import rmq

from ..utils import CommunicatorTester

# pylint: disable=invalid-name


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

    def test_context_manager(self):
        MESSAGE = 'get this yo'

        rpc_future = kiwipy.Future()

        def rpc_get(_comm, msg):
            rpc_future.set_result(msg)

        self.communicator.add_rpc_subscriber(rpc_get, 'test_context_manager')
        # Check the context manager of the communicator works
        with self.communicator as comm:
            comm.rpc_send('test_context_manager', MESSAGE)

        message = rpc_future.result(self.WAIT_TIMEOUT)
        self.assertEqual(MESSAGE, message)
