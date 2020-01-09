import asyncio
import pytest

import kiwipy

from . import utils

# pylint: disable=invalid-name

WAIT_TIMEOUT = 5.  # Wait timeout in seconds for async operations

# region RPC


@pytest.mark.asyncio
async def test_rpc_send_receive(communicator: kiwipy.rmq.RmqCommunicator):
    MESSAGE = "sup yo'"
    RESPONSE = "nuthin bra"

    messages = []

    def on_receive(_comm, msg):
        messages.append(msg)
        return RESPONSE

    await communicator.add_rpc_subscriber(on_receive, 'rpc')
    response_future = await communicator.rpc_send('rpc', MESSAGE)
    response = await response_future

    assert messages[0] == MESSAGE
    assert response == RESPONSE


@pytest.mark.asyncio
async def test_add_remove_rpc_subscriber(communicator):
    """ Test adding, sending to, and then removing an RPC subscriber """

    def rpc_subscriber(_comm, _msg):
        return True

    # Check we're getting messages
    await communicator.add_rpc_subscriber(rpc_subscriber, rpc_subscriber.__name__)
    result_future = await communicator.rpc_send(rpc_subscriber.__name__, None)
    result = await result_future
    assert result is True

    await communicator.remove_rpc_subscriber(rpc_subscriber.__name__)
    # Check that we're unsubscribed
    with pytest.raises((kiwipy.UnroutableError, asyncio.TimeoutError)):
        to_await = communicator.rpc_send(rpc_subscriber.__name__, None)
        await asyncio.wait_for(to_await, timeout=2.)


# endregion

# region Tasks


@pytest.mark.asyncio
async def test_task_send(communicator: kiwipy.rmq.RmqCommunicator):
    TASK = 'The meaning?'
    RESULT = 42

    tasks = []

    def on_task(_comm, task):
        tasks.append(task)
        return RESULT

    await communicator.add_task_subscriber(on_task)
    result_future = await communicator.task_send(TASK)
    result = await result_future

    assert tasks[0] == TASK
    assert RESULT == result


@pytest.mark.asyncio
async def test_future_task(communicator: kiwipy.rmq.RmqCommunicator):
    """
    Test a task that returns a future meaning that will be resolve to a value later
    """
    TASK = 'The meaning?'
    RESULT = 42
    result_future = asyncio.Future()

    tasks = []

    def on_task(_comm, task):
        tasks.append(task)
        return result_future

    await communicator.add_task_subscriber(on_task)
    task_future = await communicator.task_send(TASK)

    # The task has given us a future
    future_from_task = await task_future
    assert asyncio.isfuture(future_from_task)

    # Now resolve the future which should give us a result
    result_future.set_result(42)

    result = await future_from_task

    assert tasks[0] == TASK
    assert RESULT == result


@pytest.mark.asyncio
async def test_task_exception(communicator: kiwipy.rmq.RmqCommunicator):
    TASK = 'The meaning?'

    tasks = []

    def on_task(_comm, task):
        tasks.append(task)
        raise RuntimeError("I cannea do it Captain!")

    await communicator.add_task_subscriber(on_task)
    with pytest.raises(kiwipy.RemoteException):
        result_future = await communicator.task_send(TASK)
        await result_future

    assert tasks[0] == TASK


@pytest.mark.asyncio
async def test_task_no_reply(communicator: kiwipy.rmq.RmqCommunicator):
    """Test that we don't get a reply if we don't ask for one, i.e. fire-and-forget"""
    TASK = 'The meaning?'  # pylint: disable=invalid-name
    RESULT = 42  # pylint: disable=invalid-name

    tasks = []

    task_future = asyncio.Future()

    def on_task(_comm, task):
        tasks.append(task)
        task_future.set_result(RESULT)
        return RESULT

    await communicator.add_task_subscriber(on_task)
    result = await communicator.task_send(TASK, no_reply=True)

    # Make sure the task actually gets done
    await task_future

    assert len(tasks) == 1
    assert tasks[0] == TASK
    assert result is None


@pytest.mark.asyncio
async def test_custom_tasks_queue(communicator: kiwipy.rmq.RmqCommunicator):
    """Test that we don't get a reply if we don't ask for one, i.e. fire-and-forget"""
    TASK = 'The meaning?'  # pylint: disable=invalid-name
    RESULT = 42  # pylint: disable=invalid-name

    tasks = []

    task_future = asyncio.Future()

    def on_task(_comm, task):
        tasks.append(task)
        task_future.set_result(RESULT)
        return RESULT

    custom_queue = await communicator.task_queue('custom-queue')

    await custom_queue.add_task_subscriber(on_task)
    result = await custom_queue.task_send(TASK, no_reply=True)

    # Make sure the task actually gets done
    await task_future

    assert len(tasks) == 1
    assert tasks[0] == TASK
    assert result is None


# endregion

# region Broadcast


@pytest.mark.asyncio
async def test_broadcast_send(communicator: kiwipy.rmq.RmqCommunicator):
    SUBJECT = 'yo momma'
    BODY = 'so fat'
    SENDER_ID = 'me'
    FULL_MSG = {'body': BODY, 'subject': SUBJECT, 'sender': SENDER_ID, 'correlation_id': None}

    message1 = asyncio.Future()
    message2 = asyncio.Future()

    def on_broadcast_1(_comm, body, sender, subject, correlation_id):
        message1.set_result({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

    def on_broadcast_2(_comm, body, sender, subject, correlation_id):
        message2.set_result({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

    await communicator.add_broadcast_subscriber(on_broadcast_1)
    await communicator.add_broadcast_subscriber(on_broadcast_2)

    await communicator.broadcast_send(**FULL_MSG)
    # Wait fot the send and receive
    await asyncio.gather(message1, message2)

    assert message1.result() == FULL_MSG
    assert message2.result() == FULL_MSG


@pytest.mark.asyncio
async def test_broadcast_filter_subject(communicator: kiwipy.rmq.RmqCommunicator):
    subjects = []
    EXPECTED_SUBJECTS = ['purchase.car', 'purchase.piano']

    done = asyncio.Future()

    def on_broadcast_1(_comm, _body, _sender=None, subject=None, _correlation_id=None):
        subjects.append(subject)
        if len(subjects) == len(EXPECTED_SUBJECTS):
            done.set_result(True)

    await communicator.add_broadcast_subscriber(kiwipy.BroadcastFilter(on_broadcast_1, subject="purchase.*"))

    for subj in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
        await communicator.broadcast_send(None, subject=subj)

    await done

    assert len(subjects) == 2
    assert EXPECTED_SUBJECTS == subjects


@pytest.mark.asyncio
async def test_broadcast_filter_sender(communicator: kiwipy.rmq.RmqCommunicator):
    EXPECTED_SENDERS = ['bob.jones', 'alice.jones']
    senders = []

    done = asyncio.Future()

    def on_broadcast_1(_comm, _body, sender=None, _subject=None, _correlation_id=None):
        senders.append(sender)
        if len(senders) == len(EXPECTED_SENDERS):
            done.set_result(True)

    await communicator.add_broadcast_subscriber(kiwipy.BroadcastFilter(on_broadcast_1, sender="*.jones"))

    for subj in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
        await communicator.broadcast_send(None, sender=subj)

    await done

    assert len(senders) == 2
    assert senders == EXPECTED_SENDERS


@pytest.mark.asyncio
async def test_broadcast_filter_sender_and_subject(communicator: kiwipy.rmq.RmqCommunicator):
    senders_and_subects = set()
    EXPECTED = {
        ('bob.jones', 'purchase.car'),
        ('bob.jones', 'purchase.piano'),
        ('alice.jones', 'purchase.car'),
        ('alice.jones', 'purchase.piano'),
    }

    done = asyncio.Future()

    def on_broadcast_1(_comm, _body, sender=None, subject=None, _correlation_id=None):
        senders_and_subects.add((sender, subject))
        if len(senders_and_subects) == len(EXPECTED):
            done.set_result(True)

    filtered = kiwipy.BroadcastFilter(on_broadcast_1)
    filtered.add_sender_filter("*.jones")
    filtered.add_subject_filter("purchase.*")
    await communicator.add_broadcast_subscriber(filtered)

    for sender in ['bob.jones', 'bob.smith', 'martin.uhrin', 'alice.jones']:
        for subj in ['purchase.car', 'purchase.piano', 'sell.guitar', 'sell.house']:
            await communicator.broadcast_send(None, sender=sender, subject=subj)

    await done

    assert len(senders_and_subects) == 4
    assert senders_and_subects == EXPECTED


@pytest.mark.asyncio
async def test_add_remove_broadcast_subscriber(connection: kiwipy.rmq.RmqCommunicator):
    # Set the expiry to something small so we know that the queues expire after we unsubscribe
    communicator = await utils.new_communicator(connection, settings={'queue_expires': 1})

    async with communicator:
        broadcast_received = asyncio.Future()

        def broadcast_subscriber(_comm, _body, _sender=None, _subject=None, _correlation_id=None):
            broadcast_received.set_result(True)

        # Check we're getting messages
        await communicator.add_broadcast_subscriber(broadcast_subscriber, broadcast_subscriber.__name__)
        await communicator.broadcast_send(None)
        assert (await broadcast_received) is True

        await communicator.remove_broadcast_subscriber(broadcast_subscriber.__name__)
        # Check that we're unsubscribed
        broadcast_received = asyncio.Future()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(broadcast_received, timeout=2.)

        # Wait to make sure the queue is expired.  The queue_expires above is in milliseconds while below
        # it is in seconds so this should be enough for RMQ to get its ass in gear
        await asyncio.sleep(1.)

        # Now re-add and check we're getting messages
        broadcast_received = asyncio.Future()
        await communicator.add_broadcast_subscriber(broadcast_subscriber, broadcast_subscriber.__name__)
        await communicator.broadcast_send(None)
        assert (await broadcast_received) is True


# endregion
