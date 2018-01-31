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
        self.exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        self.task_queue_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())

        communicator = rmq.RmqCommunicator(
            self.connector,
            exchange_name=self.exchange_name,
            task_queue=self.task_queue_name,
            testing_mode=True
        )

        communicator.init()
        return communicator

    def destroy_communicator(self, communicator):
        communicator.close()
        self.connector.close()
