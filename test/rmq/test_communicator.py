import unittest
import uuid

import kiwipy

from . import utils
from ..utils import CommunicatorTester

try:
    import pika
    from kiwipy import rmq
except ImportError:
    pika = None


@unittest.skipIf(not pika, "Requires pika library and RabbitMQ")
class TestCommunicator(CommunicatorTester, utils.TestCaseWithLoop):
    def create_communicator(self):
        self.connector = rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.exchange = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        self.task_queue = "{}.{}".format(self.__class__.__name__, uuid.uuid4())

        communicator = rmq.RmqCommunicator(
            self.connector,
            exchange_name=self.exchange,
            task_queue=self.task_queue,
            testing_mode=True
        )

        communicator.connect()
        return communicator

    def destroy_communicator(self, communicator):
        communicator.disconnect()


@unittest.skipIf(not pika, "Requires pika library and RabbitMQ")
class TestCommunicatorDroppyConnection(utils.TestCaseWithLoop):
    def setUp(self):
        super(TestCommunicatorDroppyConnection, self).setUp()

        self.connector = rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.exchange = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        self.task_queue = "{}.{}".format(self.__class__.__name__, uuid.uuid4())

        self._communicator = rmq.RmqCommunicator(
            self.connector,
            exchange_name=self.exchange,
            task_queue=self.task_queue,
            testing_mode=True
        )

        self._communicator.connect()

    def tearDown(self):
        self._communicator.disconnect()
        self.connector.disconnect()

        # Have to call super after because this closes the loop
        super(TestCommunicatorDroppyConnection, self).tearDown()

    def test_connect_disconnect(self):
        self.loop.run_sync(lambda: self.connector.connect())
