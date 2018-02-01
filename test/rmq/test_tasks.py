import unittest
import uuid

import kiwipy

from . import utils

try:
    import pika
    import pika.exceptions
    from kiwipy import rmq
except ImportError:
    pika = None


@unittest.skipIf(not pika, "Requires pika library and RabbitMQ")
class TestTaskPublisher(utils.TestCaseWithLoop):
    def setUp(self):
        super(TestTaskPublisher, self).setUp()
        self.connector = rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        self.task_queue = "{}.{}".format(self.__class__.__name__, uuid.uuid4())

        self.task_publisher = rmq.RmqTaskPublisher(
            self.connector,
            task_queue_name=self.task_queue,
            exchange_name=self.exchange_name,
            testing_mode=True)

        self.loop.run_sync(self.task_publisher.connect)

    def tearDown(self):
        self.loop.run_sync(self.task_publisher.disconnect)
        self.loop.run_sync(self.connector.disconnect)
        # Call this last, it closes the loop
        super(TestTaskPublisher, self).tearDown()

    def test_send_no_subscribers(self):
        """ Test what happens when there are no task queues bound to the exchange """
        TASK = 'The meaning?'
        with self.assertRaises(pika.exceptions.UnroutableError):
            self.task_publisher.task_send(TASK)
