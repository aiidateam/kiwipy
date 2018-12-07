"""
A generic test for any communicator that conforms to the `kiwipy.Communicator` interface.

I've had to disable invalid-name for some portions of this file as I use the convention
that all caps 'variable' names mean that it's a constant.
"""

from __future__ import absolute_import
import abc
import six

import kiwipy


@six.add_metaclass(abc.ABCMeta)
class CommunicatorTester(object):
    # Disable invalid name because I use caps for constants which the linter doesn't like
    # also disable no-member for superclass calls because we use this as a mixin that gets used
    # with unittest.TestCase
    # pylint: disable=invalid-name, no-member

    WAIT_TIMEOUT = 5.

    communicator = None

    def setUp(self):
        super(CommunicatorTester, self).setUp()
        self.communicator = self.create_communicator()

    def tearDown(self):
        self.destroy_communicator(self.communicator)
        self.communicator = None

    @abc.abstractmethod
    def create_communicator(self):
        """
        :return: A constructed communicator
        :rtype: :class:`kiwi.Communicator`
        """
        pass

    def destroy_communicator(self, communicator):
        pass

    # region RPC

    def test_rpc_send_receive(self):
        MESSAGE = "sup yo'"  # pylint: disable=invalid-name
        RESPONSE = "nuthin bra"  # pylint: disable=invalid-name

        messages = []

        def on_receive(_comm, msg):
            messages.append(msg)
            return RESPONSE

        self.communicator.add_rpc_subscriber(on_receive, 'rpc')
        response = self.communicator.rpc_send('rpc', MESSAGE).result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(MESSAGE, messages[0])
        self.assertEqual(RESPONSE, response)

    def test_add_remove_rpc_subscriber(self):
        """ Test adding, sending to, removing and readding an RPC subscriber """

        def rpc_subscriber(_comm, _msg):
            return True

        # Check we're getting messages
        self.communicator.add_rpc_subscriber(rpc_subscriber, rpc_subscriber.__name__)
        result = self.communicator.rpc_send(rpc_subscriber.__name__, None).result(timeout=self.WAIT_TIMEOUT)
        self.assertTrue(result)

        self.communicator.remove_rpc_subscriber(rpc_subscriber.__name__)
        # Check that we're unsubscribed
        with self.assertRaises((kiwipy.UnroutableError, kiwipy.TimeoutError)):
            self.communicator.rpc_send(rpc_subscriber.__name__, None).result(timeout=self.WAIT_TIMEOUT)

        self.communicator.add_rpc_subscriber(rpc_subscriber, rpc_subscriber.__name__)
        result = self.communicator.rpc_send(rpc_subscriber.__name__, None).result(timeout=self.WAIT_TIMEOUT)
        self.assertTrue(result)

    def test_rpc_nested_futrues(self):
        """Test that an RPC call that returns a future, which itself resolves to a future works"""
        RESULT = "You've reached the bottom of the rabbit hole my friend"  # pylint: disable=invalid-name

        def rpc_subscriber(_comm, _msg):
            future1 = kiwipy.Future()
            future2 = kiwipy.Future()
            future1.set_result(future2)
            future2.set_result(RESULT)
            return future1

        # Check we're getting messages
        self.communicator.add_rpc_subscriber(rpc_subscriber, rpc_subscriber.__name__)
        future1 = self.communicator.rpc_send(rpc_subscriber.__name__, None).result(timeout=self.WAIT_TIMEOUT)
        self.assertIsInstance(future1, kiwipy.Future)

        future2 = future1.result(timeout=self.WAIT_TIMEOUT)
        self.assertIsInstance(future2, kiwipy.Future)

        result = future2.result(timeout=self.WAIT_TIMEOUT)
        self.assertEqual(RESULT, result)

    def test_rpc_exception(self):
        MESSAGE = 'The meaning?'  # pylint: disable=invalid-name
        EXCEPTION = RuntimeError("I cannea do it Captain!")  # pylint: disable=invalid-name

        messages = []

        def on_receive(_com, msg):
            messages.append(msg)
            raise EXCEPTION

        self.communicator.add_rpc_subscriber(on_receive, on_receive.__name__)
        with self.assertRaises(kiwipy.RemoteException):
            self.communicator.rpc_send(on_receive.__name__, MESSAGE).result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(messages[0], MESSAGE)

    def test_rpc_unroutable(self):
        """Test that if there is no subscriber then an RPC call is unroutable"""
        with self.assertRaises(kiwipy.UnroutableError):
            self.communicator.rpc_send('noone', 'nothing').result(timeout=1.)

    # endregion

    # region Task

    def test_task_send(self):
        TASK = 'The meaning?'  # pylint: disable=invalid-name
        RESULT = 42  # pylint: disable=invalid-name

        tasks = []

        def on_task(_comm, task):
            tasks.append(task)
            return RESULT

        self.communicator.add_task_subscriber(on_task)
        result = self.communicator.task_send(TASK).result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(tasks[0], TASK)
        self.assertEqual(RESULT, result)

    def test_future_task(self):
        """
        Test a task that returns a future meaning that will be resolve to a value later
        """
        TASK = 'The meaning?'
        RESULT = 42
        result_future = kiwipy.Future()

        tasks = []

        def on_task(_comm, task):
            tasks.append(task)
            return result_future

        self.communicator.add_task_subscriber(on_task)
        task_future = self.communicator.task_send(TASK).result(timeout=self.WAIT_TIMEOUT)

        result_future.set_result(42)

        result = task_future.result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(TASK, tasks[0])
        self.assertEqual(RESULT, result)

    def test_task_exception(self):
        TASK = 'The meaning?'  # pylint: disable=invalid-name
        EXCEPTION = RuntimeError("I cannea do it Captain!")  # pylint: disable=invalid-name

        tasks = []

        def on_task(_com, task):
            tasks.append(task)
            raise EXCEPTION

        self.communicator.add_task_subscriber(on_task)
        with self.assertRaises(kiwipy.RemoteException):
            self.communicator.task_send(TASK).result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(tasks[0], TASK)

    def test_task_no_reply(self):
        """Test that we don't get a reply if we don't ask for one, i.e. fire-and-forget"""
        TASK = 'The meaning?'  # pylint: disable=invalid-name
        RESULT = 42  # pylint: disable=invalid-name

        tasks = []

        task_future = kiwipy.Future()

        def on_task(_comm, task):
            tasks.append(task)
            task_future.set_result(RESULT)
            return RESULT

        self.communicator.add_task_subscriber(on_task)
        send_result = self.communicator.task_send(TASK, no_reply=True)
        self.assertIsNone(send_result)

        # Make sure the task is done
        result = task_future.result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(1, len(tasks))
        self.assertEqual(TASK, tasks[0])
        self.assertEqual(RESULT, result)

    # endregion

    # region Broadcast

    def test_broadcast_send(self):
        SUBJECT = 'yo momma'  # pylint: disable=invalid-name
        BODY = 'so fat'  # pylint: disable=invalid-name
        SENDER_ID = 'me'  # pylint: disable=invalid-name
        FULL_MSG = {'body': BODY, 'subject': SUBJECT, 'sender': SENDER_ID, 'correlation_id': None}  # pylint: disable=invalid-name

        message1 = kiwipy.Future()
        message2 = kiwipy.Future()

        def on_broadcast_1(_comm, body, sender, subject, correlation_id):
            message1.set_result({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

        def on_broadcast_2(_comm, body, sender, subject, correlation_id):
            message2.set_result({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

        self.communicator.add_broadcast_subscriber(on_broadcast_1)
        self.communicator.add_broadcast_subscriber(on_broadcast_2)

        self.communicator.broadcast_send(**FULL_MSG)
        # Wait fot the send and receive
        kiwipy.wait((message1, message2), timeout=self.WAIT_TIMEOUT)

        self.assertDictEqual(message1.result(), FULL_MSG)
        self.assertDictEqual(message2.result(), FULL_MSG)

    def test_broadcast_filter_subject(self):
        subjects = []
        EXPECTED_SUBJECTS = ['purchase.car', 'purchase.piano']  # pylint: disable=invalid-name

        done = kiwipy.Future()

        def on_broadcast_1(_comm, _body, _sender=None, subject=None, _correlation_id=None):
            subjects.append(subject)
            if len(subjects) == len(EXPECTED_SUBJECTS):
                done.set_result(True)

        self.communicator.add_broadcast_subscriber(kiwipy.BroadcastFilter(on_broadcast_1, subject="purchase.*"))

        for subj in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
            self.communicator.broadcast_send(None, subject=subj)

        done.result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(len(subjects), 2)
        self.assertListEqual(EXPECTED_SUBJECTS, subjects)

    def test_broadcast_filter_sender(self):
        EXPECTED_SENDERS = ['bob.jones', 'alice.jones']  # pylint: disable=invalid-name
        senders = []

        done = kiwipy.Future()

        def on_broadcast_1(_comm, _body, sender=None, _subject=None, _correlation_id=None):
            senders.append(sender)
            if len(senders) == len(EXPECTED_SENDERS):
                done.set_result(True)

        self.communicator.add_broadcast_subscriber(kiwipy.BroadcastFilter(on_broadcast_1, sender="*.jones"))

        for sendr in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
            self.communicator.broadcast_send(None, sender=sendr)

        done.result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(2, len(senders))
        self.assertListEqual(EXPECTED_SENDERS, senders)

    def test_broadcast_filter_sender_and_subject(self):
        # pylint: disable=invalid-name
        senders_and_subects = set()
        EXPECTED = {
            ('bob.jones', 'purchase.car'),
            ('bob.jones', 'purchase.piano'),
            ('alice.jones', 'purchase.car'),
            ('alice.jones', 'purchase.piano'),
        }

        done = kiwipy.Future()

        def on_broadcast_1(_communicator, _body, sender=None, subject=None, _correlation_id=None):
            senders_and_subects.add((sender, subject))
            if len(senders_and_subects) == len(EXPECTED):
                done.set_result(True)

        filtered = kiwipy.BroadcastFilter(on_broadcast_1)
        filtered.add_sender_filter("*.jones")
        filtered.add_subject_filter("purchase.*")
        self.communicator.add_broadcast_subscriber(filtered)

        for sendr in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
            for subj in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
                self.communicator.broadcast_send(None, sender=sendr, subject=subj)

        done.result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(4, len(senders_and_subects))
        self.assertSetEqual(EXPECTED, senders_and_subects)

    def test_add_remove_broadcast_subscriber(self):
        """Test adding, removing and readding a broadcast subscriber"""
        broadcast_received = kiwipy.Future()

        def broadcast_subscriber(_comm, body, sender=None, subject=None, correlation_id=None):
            # pylint: disable=unused-argument
            broadcast_received.set_result(True)

        # Check we're getting messages
        self.communicator.add_broadcast_subscriber(broadcast_subscriber, broadcast_subscriber.__name__)
        self.communicator.broadcast_send(None)
        self.assertTrue(broadcast_received.result(timeout=self.WAIT_TIMEOUT))

        self.communicator.remove_broadcast_subscriber(broadcast_subscriber.__name__)
        # Check that we're unsubscribed
        broadcast_received = kiwipy.Future()
        with self.assertRaises(kiwipy.TimeoutError):
            broadcast_received.result(timeout=self.WAIT_TIMEOUT)

        broadcast_received = kiwipy.Future()
        self.communicator.add_broadcast_subscriber(broadcast_subscriber, broadcast_subscriber.__name__)
        self.communicator.broadcast_send(None)
        self.assertTrue(broadcast_received.result(timeout=self.WAIT_TIMEOUT))

    def test_broadcast_noone_listening(self):
        """ Check that it's not an error to send a broadcast if no one is listening"""
        result = self.communicator.broadcast_send('into the ether')
        self.assertIs(True, result)

    # endregion
