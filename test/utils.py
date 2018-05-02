from future.utils import with_metaclass
import abc
import kiwipy


class CommunicatorTester(with_metaclass(abc.ABCMeta)):
    WAIT_TIMEOUT = 0.5

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

    def test_future_task(self):
        TASK = 'The meaning?'
        RESULT = 42
        result_future = kiwipy.Future()

        tasks = []

        def on_task(task):
            tasks.append(task)
            return result_future

        self.communicator.add_task_subscriber(on_task)
        task_future = self.communicator.task_send(TASK)

        result_future.set_result(42)

        result = self.communicator.await(task_future)

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
        self.communicator.await(kiwipy.gather(message1, message2), timeout=self.WAIT_TIMEOUT)

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

        self.communicator.await(done, timeout=self.WAIT_TIMEOUT)

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

        self.communicator.await(done, timeout=self.WAIT_TIMEOUT)

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

        self.communicator.await(done, timeout=self.WAIT_TIMEOUT)

        self.assertEqual(4, len(senders_and_subects))
        self.assertSetEqual(EXPECTED, senders_and_subects)

    def test_add_remove_broadcast_subscriber(self):
        broadcast_received = kiwipy.Future()

        def broadcast_subscriber(body, sender=None, subject=None, correlation_id=None):
            broadcast_received.set_result(True)

        # Check we're getting messages
        self.communicator.add_broadcast_subscriber(broadcast_subscriber)
        self.communicator.broadcast_send(None)
        self.assertTrue(self.communicator.await(broadcast_received, timeout=self.WAIT_TIMEOUT))

        self.communicator.remove_broadcast_subscriber(broadcast_subscriber)
        # Check that we're unsubscribed
        broadcast_received = kiwipy.Future()
        with self.assertRaises(kiwipy.TimeoutError):
            self.communicator.await(broadcast_received, timeout=self.WAIT_TIMEOUT)

    def test_add_remove_rpc_subscriber(self):
        def rpc_subscriber(msg):
            return True

        # Check we're getting messages
        self.communicator.add_rpc_subscriber(rpc_subscriber, rpc_subscriber.__name__)
        result = self.communicator.rpc_send_and_wait(rpc_subscriber.__name__, None, timeout=self.WAIT_TIMEOUT)
        self.assertTrue(result)

        self.communicator.remove_rpc_subscriber(rpc_subscriber.__name__)
        # Check that we're unsubscribed
        with self.assertRaises((kiwipy.UnroutableError, kiwipy.TimeoutError)):
            self.communicator.rpc_send_and_wait(rpc_subscriber.__name__, None, timeout=self.WAIT_TIMEOUT)
