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
class TestCommunicator(utils.TestCaseWithLoop):
    def setUp(self):
        super(TestCommunicator, self).setUp()

        self.connector = rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        self.task_queue_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())

        self.communicator = rmq.RmqCommunicator(
            self.connector,
            exchange_name=self.exchange_name,
            task_queue=self.task_queue_name,
            testing_mode=True
        )

        self.connector.connect()
        # Run the loop until until both are ready
        rmq.run_until_complete(self.communicator.initialised_future())

    def tearDown(self):
        self.communicator.close()
        # Close the connector before calling super because it will
        # close the loop
        self.connector.close()
        super(TestCommunicator, self).tearDown()

    def test_rpc_send(self):
        """ Testing making an RPC message and receiving a response """
        MSG = {'test': 5}
        RESPONSE = 'response'
        messages_received = kiwi.Future()

        class Receiver(kiwi.Receiver):
            def on_rpc_receive(self, msg):
                messages_received.set_result(msg)
                return RESPONSE

            def on_broadcast_receive(self, msg):
                pass

        receiver = Receiver()
        self.communicator.register_receiver(receiver, 'receiver')

        # Send and make sure we get the message
        future = self.communicator.rpc_send('receiver', MSG)
        result = rmq.run_until_complete(messages_received, self.loop)
        self.assertEqual(result, MSG)

        # Now make sure we get the response
        response = rmq.run_until_complete(future)
        self.assertEqual(response, RESPONSE)
