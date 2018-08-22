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
        exchange_name = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())
        task_queue_name = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())

        @gen.coroutine
        def init():
            connection = yield topika.connect_robust('amqp://guest:guest@localhost:5672/', loop=self.loop)
            communicator = rmq.RmqThreadCommunicator(
                connection,
                exchange_name=exchange_name,
                task_queue=task_queue_name,
                testing_mode=True
            )
            raise gen.Return(communicator)

        communicator = self.loop.run_sync(init)
        communicator.start()
        return communicator

    def destroy_communicator(self, communicator):
        communicator.stop()
