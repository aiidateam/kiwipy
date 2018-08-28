import shortuuid
import topika
from tornado import gen
import unittest

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
        message_exchange = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())
        task_queue = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())

        return rmq.RmqThreadCommunicator.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_queue=task_queue,
            loop=self.loop,
            testing_mode=True)

    def destroy_communicator(self, communicator):
        communicator.stop()
