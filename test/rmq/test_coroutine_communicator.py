from __future__ import absolute_import
import shortuuid
import topika
from tornado import gen, testing, concurrent

import kiwipy
from kiwipy import rmq


class TestCoroutineCommunicator(testing.AsyncTestCase):
    # Disable invalid name because  I use all caps to designate a constant which pylint
    # doesn't like
    # pylint: disable=invalid-name

    WAIT_TIMEOUT = testing.get_async_test_timeout()  # Wait timeout in seconds for async operations

    def setUp(self):
        super(TestCoroutineCommunicator, self).setUp()

        self.loop = self.io_loop
        message_exchange = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())
        task_exchange = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())
        task_queue = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())
        self.communicator = None  # type: kiwipy.rmq.RmqCommunicator

        @gen.coroutine
        def init():
            connection = yield topika.connect_robust('amqp://guest:guest@localhost:5672/', loop=self.loop)
            self.communicator = rmq.RmqCommunicator(
                connection,
                message_exchange=message_exchange,
                task_exchange=task_exchange,
                task_queue=task_queue,
                testing_mode=True)
            yield self.communicator.connect()

        self.loop.run_sync(init)

    def tearDown(self):
        self.loop.run_sync(self.communicator.disconnect)
        super(TestCoroutineCommunicator, self).tearDown()

    # region RPC

    @testing.gen_test
    def test_rpc_send_receive(self):
        MESSAGE = "sup yo'"
        RESPONSE = "nuthin bra"

        messages = []

        def on_receive(_comm, msg):
            messages.append(msg)
            return RESPONSE

        yield self.communicator.add_rpc_subscriber(on_receive, 'rpc')
        response_future = yield self.communicator.rpc_send('rpc', MESSAGE)
        response = yield response_future

        self.assertEqual(messages[0], MESSAGE)
        self.assertEqual(response, RESPONSE)

    @testing.gen_test
    def test_add_remove_rpc_subscriber(self):
        """ Test adding, sending to, and then removing an RPC subscriber """

        def rpc_subscriber(_comm, _msg):
            return True

        # Check we're getting messages
        yield self.communicator.add_rpc_subscriber(rpc_subscriber, rpc_subscriber.__name__)
        result_future = yield self.communicator.rpc_send(rpc_subscriber.__name__, None)
        result = yield result_future
        self.assertTrue(result)

        yield self.communicator.remove_rpc_subscriber(rpc_subscriber.__name__)
        # Check that we're unsubscribed
        with self.assertRaises((kiwipy.UnroutableError, gen.TimeoutError)):
            to_await = self.communicator.rpc_send(rpc_subscriber.__name__, None)
            yield gen.with_timeout(timeout=2., future=to_await)

    # endregion

    # region Tasks

    @testing.gen_test
    def test_task_send(self):
        TASK = 'The meaning?'
        RESULT = 42

        tasks = []

        def on_task(_comm, task):
            tasks.append(task)
            return RESULT

        self.communicator.add_task_subscriber(on_task)
        result_future = yield self.communicator.task_send(TASK)
        result = yield result_future

        self.assertEqual(tasks[0], TASK)
        self.assertEqual(RESULT, result)

    @testing.gen_test
    def test_future_task(self):
        """
        Test a task that returns a future meaning that will be resolve to a value later
        """
        TASK = 'The meaning?'
        RESULT = 42
        result_future = concurrent.Future()

        tasks = []

        def on_task(_comm, task):
            tasks.append(task)
            return result_future

        self.communicator.add_task_subscriber(on_task)
        task_future = yield self.communicator.task_send(TASK)

        # The task has given us a future
        future_from_task = yield task_future
        self.assertTrue(gen.is_future(future_from_task))

        # Now resolve the future which should give us a result
        result_future.set_result(42)

        result = yield future_from_task

        self.assertEqual(tasks[0], TASK)
        self.assertEqual(RESULT, result)

    @testing.gen_test
    def test_task_exception(self):
        TASK = 'The meaning?'

        tasks = []

        def on_task(_comm, task):
            tasks.append(task)
            raise RuntimeError("I cannea do it Captain!")

        self.communicator.add_task_subscriber(on_task)
        with self.assertRaises(kiwipy.RemoteException):
            result_future = yield self.communicator.task_send(TASK)
            yield result_future

        self.assertEqual(tasks[0], TASK)

    @testing.gen_test
    def test_task_no_reply(self):
        """Test that we don't get a reply if we don't ask for one, i.e. fire-and-forget"""
        TASK = 'The meaning?'  # pylint: disable=invalid-name
        RESULT = 42  # pylint: disable=invalid-name

        tasks = []

        task_future = concurrent.Future()

        def on_task(_comm, task):
            tasks.append(task)
            task_future.set_result(RESULT)
            return RESULT

        self.communicator.add_task_subscriber(on_task)
        result = yield self.communicator.task_send(TASK, no_reply=True)

        # Make sure the task actually gets done
        yield task_future

        self.assertEqual(1, len(tasks))
        self.assertEqual(tasks[0], TASK)
        self.assertEqual(None, result)

    # endregion

    # region Broadcast

    @testing.gen_test
    def test_broadcast_send(self):
        SUBJECT = 'yo momma'
        BODY = 'so fat'
        SENDER_ID = 'me'
        FULL_MSG = {'body': BODY, 'subject': SUBJECT, 'sender': SENDER_ID, 'correlation_id': None}

        message1 = concurrent.Future()
        message2 = concurrent.Future()

        def on_broadcast_1(_comm, body, sender, subject, correlation_id):
            message1.set_result({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

        def on_broadcast_2(_comm, body, sender, subject, correlation_id):
            message2.set_result({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

        yield self.communicator.add_broadcast_subscriber(on_broadcast_1)
        yield self.communicator.add_broadcast_subscriber(on_broadcast_2)

        yield self.communicator.broadcast_send(**FULL_MSG)
        # Wait fot the send and receive
        yield [message1, message2]

        self.assertDictEqual(message1.result(), FULL_MSG)
        self.assertDictEqual(message2.result(), FULL_MSG)

    @testing.gen_test
    def test_broadcast_filter_subject(self):
        subjects = []
        EXPECTED_SUBJECTS = ['purchase.car', 'purchase.piano']

        done = concurrent.Future()

        def on_broadcast_1(_comm, _body, _sender=None, subject=None, _correlation_id=None):
            subjects.append(subject)
            if len(subjects) == len(EXPECTED_SUBJECTS):
                done.set_result(True)

        yield self.communicator.add_broadcast_subscriber(kiwipy.BroadcastFilter(on_broadcast_1, subject="purchase.*"))

        for subj in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
            yield self.communicator.broadcast_send(None, subject=subj)

        yield done

        self.assertEqual(len(subjects), 2)
        self.assertListEqual(EXPECTED_SUBJECTS, subjects)

    @testing.gen_test
    def test_broadcast_filter_sender(self):
        EXPECTED_SENDERS = ['bob.jones', 'alice.jones']
        senders = []

        done = concurrent.Future()

        def on_broadcast_1(_comm, _body, sender=None, _subject=None, _correlation_id=None):
            senders.append(sender)
            if len(senders) == len(EXPECTED_SENDERS):
                done.set_result(True)

        yield self.communicator.add_broadcast_subscriber(kiwipy.BroadcastFilter(on_broadcast_1, sender="*.jones"))

        for subj in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
            yield self.communicator.broadcast_send(None, sender=subj)

        yield done

        self.assertEqual(2, len(senders))
        self.assertListEqual(EXPECTED_SENDERS, senders)

    @testing.gen_test
    def test_broadcast_filter_sender_and_subject(self):
        senders_and_subects = set()
        EXPECTED = {
            ('bob.jones', 'purchase.car'),
            ('bob.jones', 'purchase.piano'),
            ('alice.jones', 'purchase.car'),
            ('alice.jones', 'purchase.piano'),
        }

        done = concurrent.Future()

        def on_broadcast_1(_comm, _body, sender=None, subject=None, _correlation_id=None):
            senders_and_subects.add((sender, subject))
            if len(senders_and_subects) == len(EXPECTED):
                done.set_result(True)

        filtered = kiwipy.BroadcastFilter(on_broadcast_1)
        filtered.add_sender_filter("*.jones")
        filtered.add_subject_filter("purchase.*")
        yield self.communicator.add_broadcast_subscriber(filtered)

        for sender in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
            for subj in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
                self.communicator.broadcast_send(None, sender=sender, subject=subj)

        yield done

        self.assertEqual(4, len(senders_and_subects))
        self.assertSetEqual(EXPECTED, senders_and_subects)

    @testing.gen_test
    def test_add_remove_broadcast_subscriber(self):
        broadcast_received = concurrent.Future()

        def broadcast_subscriber(_comm, _body, _sender=None, _subject=None, _correlation_id=None):
            broadcast_received.set_result(True)

        # Check we're getting messages
        identifier = yield self.communicator.add_broadcast_subscriber(broadcast_subscriber)
        yield self.communicator.broadcast_send(None)
        self.assertTrue((yield broadcast_received))

        yield self.communicator.remove_broadcast_subscriber(identifier)
        # Check that we're unsubscribed
        broadcast_received = concurrent.Future()
        with self.assertRaises(gen.TimeoutError):
            yield gen.with_timeout(timeout=2., future=broadcast_received)

    # endregion
