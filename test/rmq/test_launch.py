import unittest
import uuid

import kiwi
from . import utils

try:
    import pika
    from kiwi import rmq
except ImportError:
    pika = None


@unittest.skipIf(not pika, "Requires pika library and RabbitMQ")
class TestTaskControllerAndRunner(utils.TestCaseWithLoop):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        self.connector = rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        self.queue_name = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        self.subscriber = rmq.RmqTaskSubscriber(
            self.connector,
            exchange_name=self.exchange_name,
            task_queue_name=self.queue_name,
            testing_mode=True)
        self.publisher = rmq.RmqTaskPublisher(
            self.connector,
            exchange_name=self.exchange_name,
            task_queue_name=self.queue_name,
            testing_mode=True)

        self.connector.connect()
        # Run the loop until until both are ready
        rmq.run_until_complete(
            kiwi.gather(self.subscriber.initialised_future(),
                        self.publisher.initialised_future()))

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.connector.close()
        super(TestTaskControllerAndRunner, self).tearDown()

    # TODO: Test publisher/subscriber

