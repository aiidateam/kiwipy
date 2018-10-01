from __future__ import absolute_import
import topika
from tornado import testing, gen
import unittest
import uuid

import kiwipy.rmq

try:
    import pika
    import pika.exceptions
    from kiwipy import rmq
except ImportError:
    pika = None


@unittest.skipIf(not pika, "Requires pika library and RabbitMQ")
class TestTaskPublisher(testing.AsyncTestCase):

    def setUp(self):
        super(TestTaskPublisher, self).setUp()
        self.loop = self.io_loop

        exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        task_queue_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())

        self.connection = None  # type: topika.Connection
        self.task_publisher = None  # type: kiwipy.rmq.RmqTaskPublisher

        @gen.coroutine
        def init():
            self.connection = yield topika.connect_robust('amqp://guest:guest@localhost:5672/', loop=self.loop)
            self.task_publisher = rmq.RmqTaskPublisher(
                self.connection, task_queue_name=task_queue_name, exchange_name=exchange_name, testing_mode=True)
            yield self.task_publisher.connect()

        self.loop.run_sync(init)

    def tearDown(self):

        @gen.coroutine
        def destroy():
            yield self.task_publisher.disconnect()
            yield self.connection.close()

        self.loop.run_sync(destroy)

        # Call this last, it closes the loop
        super(TestTaskPublisher, self).tearDown()

    @testing.gen_test
    def test_send_no_subscribers(self):
        """ Test what happens when there are no task queues bound to the exchange """
        TASK = 'The meaning?'
        with self.assertRaises(pika.exceptions.UnroutableError):
            result = yield self.task_publisher.task_send(TASK)
