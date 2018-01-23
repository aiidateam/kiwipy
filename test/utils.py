from future.utils import with_metaclass
import abc
import kiwipy


class CommunicatorTester(with_metaclass(abc.ABCMeta)):
    def setUp(self):
        super(CommunicatorTester, self).setUp()
        self.communicator = self.create_communicator()

    def tearDown(self):
        self.destroy_communicator(self.communicator)

    @abc.abstractmethod
    def create_communicator(self):
        """
        :return: A constructed communicator
        :rtype: :class:`kiwi.Communicator`
        """
        pass

    def destroy_communicator(self, communicator):
        pass

    def wait_for_message(self, future):
        return future.result()

    def test_rcp_send_receive(self):
        MESSAGE = "sup yo'"
        RESPONSE = "nuthin bra"

        messages = []

        def on_receive(msg):
            messages.append(msg)
            return RESPONSE

        self.communicator.add_rpc_subscriber(on_receive, 'rpc')
        response = self.communicator.rpc_send_and_wait('rpc', MESSAGE)

        self.assertEqual(messages[0], MESSAGE)
        self.assertEqual(response, RESPONSE)

    def test_task_send(self):
        TASK = 'The meaning?'
        RESULT = 42

        tasks = []

        def on_task(task):
            tasks.append(task)
            return RESULT

        self.communicator.add_task_subscriber(on_task)
        result = self.communicator.task_send_and_wait(TASK)

        self.assertEqual(tasks[0], TASK)
        self.assertEqual(result, RESULT)

    def test_task_exception(self):
        TASK = 'The meaning?'

        tasks = []

        def on_task(task):
            tasks.append(task)
            raise RuntimeError("I cannea do it Captain!")

        self.communicator.add_task_subscriber(on_task)
        with self.assertRaises(kiwipy.RemoteException):
            self.communicator.task_send_and_wait(TASK)

        self.assertEqual(tasks[0], TASK)

    def test_broadcast_send(self):
        MSG = 'Shout it out loud!'

        message1 = kiwipy.Future()
        message2 = kiwipy.Future()

        def on_broadcast_1(msg):
            message1.set_result(msg)

        def on_broadcast_2(msg):
            message2.set_result(msg)

        self.communicator.add_broadcast_subscriber(on_broadcast_1)
        self.communicator.add_broadcast_subscriber(on_broadcast_2)

        sent = self.communicator.broadcast_send(MSG)
        # Wait fot the send and receive
        self.communicator.await_response(sent)
        self.communicator.await_response(kiwipy.gather(message1, message2))

        self.assertEqual(message1.result(), MSG)
        self.assertEqual(message2.result(), MSG)

