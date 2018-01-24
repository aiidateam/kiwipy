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
        SUBJECT = 'yo momma'
        BODY = 'so fat'
        SENDER_ID = 'me'
        FULL_MSG = {'body': BODY, 'subject': SUBJECT, 'sender': SENDER_ID, 'correlation_id': None}

        message1 = kiwipy.Future()
        message2 = kiwipy.Future()

        def on_broadcast_1(*args, **msg):
            message1.set_result(msg)

        def on_broadcast_2(*args, **msg):
            message2.set_result(msg)

        self.communicator.add_broadcast_subscriber(on_broadcast_1)
        self.communicator.add_broadcast_subscriber(on_broadcast_2)

        sent = self.communicator.broadcast_send(**FULL_MSG)
        # Wait fot the send and receive
        self.communicator.await_response(sent)
        self.communicator.await_response(kiwipy.gather(message1, message2))

        self.assertDictEqual(message1.result(), FULL_MSG)
        self.assertDictEqual(message2.result(), FULL_MSG)

    def test_broadcast_filter_subject(self):
        subjects = []

        def on_broadcast_1(body, sender=None, subject=None, correlation_id=None):
            subjects.append(subject)

        self.communicator.add_broadcast_subscriber(
            kiwipy.BroadcastFilter(on_broadcast_1, subject="purchase.*"))

        for subject in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
            sent = self.communicator.broadcast_send(None, subject=subject)
            self.communicator.await_response(sent)

        # self.communicator.await_response(message1)

        self.assertEqual(len(subjects), 2)
        self.assertListEqual(['purchase.car', 'purchase.piano'], subjects)

    def test_broadcast_filter_sender(self):
        senders = []

        def on_broadcast_1(body, sender=None, subject=None, correlation_id=None):
            senders.append(sender)

        self.communicator.add_broadcast_subscriber(
            kiwipy.BroadcastFilter(on_broadcast_1, sender="*.jones"))

        for sender in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
            sent = self.communicator.broadcast_send(None, sender=sender)
            self.communicator.await_response(sent)

        self.assertEqual(len(senders), 2)
        self.assertListEqual(['bob.jones', 'alice.jones'], senders)

    def test_broadcast_filter_sender_and_subject(self):
        senders_and_subects = []

        def on_broadcast_1(body, sender=None, subject=None, correlation_id=None):
            senders_and_subects.append((sender, subject))

        filtered = kiwipy.BroadcastFilter(on_broadcast_1)
        filtered.add_sender_filter("*.jones")
        filtered.add_subject_filter("purchase.*")
        self.communicator.add_broadcast_subscriber(filtered)

        for sender in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
            for subject in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
                sent = self.communicator.broadcast_send(None, sender=sender, subject=subject)
                self.communicator.await_response(sent)

        # self.communicator.await_response(message1)

        self.assertEqual(len(senders_and_subects), 4)
        self.assertListEqual(
            [
                ('bob.jones', 'purchase.car'),
                ('bob.jones', 'purchase.piano'),
                ('alice.jones', 'purchase.car'),
                ('alice.jones', 'purchase.piano'),
            ],
            senders_and_subects
        )
