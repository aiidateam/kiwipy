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

    def test_rpc_send_receive(self):
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

        self.communicator.broadcast_send(**FULL_MSG)
        # Wait fot the send and receive
        self.communicator.await(kiwipy.gather(message1, message2), timeout=1.0)

        self.assertDictEqual(message1.result(), FULL_MSG)
        self.assertDictEqual(message2.result(), FULL_MSG)

    def test_broadcast_filter_subject(self):
        subjects = []
        EXPECTED_SUBJECTS = ['purchase.car', 'purchase.piano']

        done = kiwipy.Future()

        def on_broadcast_1(body, sender=None, subject=None, correlation_id=None):
            subjects.append(subject)
            if len(subjects) == len(EXPECTED_SUBJECTS):
                done.set_result(True)

        self.communicator.add_broadcast_subscriber(
            kiwipy.BroadcastFilter(on_broadcast_1, subject="purchase.*"))

        for subject in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
            self.communicator.broadcast_send(None, subject=subject)

        self.communicator.await(done, timeout=1.0)

        self.assertEqual(len(subjects), 2)
        self.assertListEqual(EXPECTED_SUBJECTS, subjects)

    def test_broadcast_filter_sender(self):
        EXPECTED_SENDERS = ['bob.jones', 'alice.jones']
        senders = []

        done = kiwipy.Future()

        def on_broadcast_1(body, sender=None, subject=None, correlation_id=None):
            senders.append(sender)
            if len(senders) == len(EXPECTED_SENDERS):
                done.set_result(True)

        self.communicator.add_broadcast_subscriber(
            kiwipy.BroadcastFilter(on_broadcast_1, sender="*.jones"))

        for sender in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
            self.communicator.broadcast_send(None, sender=sender)

        self.communicator.await(done, timeout=1.0)

        self.assertEqual(2, len(senders))
        self.assertListEqual(EXPECTED_SENDERS, senders)

    def test_broadcast_filter_sender_and_subject(self):
        senders_and_subects = set()
        EXPECTED = {
            ('bob.jones', 'purchase.car'),
            ('bob.jones', 'purchase.piano'),
            ('alice.jones', 'purchase.car'),
            ('alice.jones', 'purchase.piano'),
        }

        done = kiwipy.Future()

        def on_broadcast_1(body, sender=None, subject=None, correlation_id=None):
            senders_and_subects.add((sender, subject))
            if len(senders_and_subects) == len(EXPECTED):
                done.set_result(True)

        filtered = kiwipy.BroadcastFilter(on_broadcast_1)
        filtered.add_sender_filter("*.jones")
        filtered.add_subject_filter("purchase.*")
        self.communicator.add_broadcast_subscriber(filtered)

        for sender in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
            for subject in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
                self.communicator.broadcast_send(None, sender=sender, subject=subject)

        self.communicator.await(done, timeout=1.0)

        self.assertEqual(4, len(senders_and_subects))
        self.assertSetEqual(EXPECTED, senders_and_subects)
