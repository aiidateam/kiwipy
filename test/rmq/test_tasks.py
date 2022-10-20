# -*- coding: utf-8 -*-
import unittest
import uuid

import pytest

# pylint: disable=redefined-outer-name, invalid-name

WAIT_TIMEOUT = 5.

try:
    import asyncio

    import aio_pika
    import aio_pika.exceptions

    from kiwipy import rmq
    import kiwipy.rmq

    @pytest.fixture
    async def task_publisher(connection: aio_pika.Connection):
        exchange_name = f'{__file__}.{uuid.uuid4()}'
        task_queue_name = f'{__file__}.{uuid.uuid4()}'

        task_pub = rmq.RmqTaskPublisher(
            connection, queue_name=task_queue_name, exchange_name=exchange_name, testing_mode=True
        )
        await task_pub.connect()
        yield task_pub
        await task_pub.disconnect()

    @pytest.fixture
    async def task_queue(connection: aio_pika.Connection):
        exchange_name = f'{__file__}.{uuid.uuid4()}'
        task_queue_name = f'{__file__}.{uuid.uuid4()}'

        task_pub = rmq.RmqTaskQueue(
            connection, queue_name=task_queue_name, exchange_name=exchange_name, testing_mode=True
        )
        await task_pub.connect()
        yield task_pub
        await task_pub.disconnect()

except ImportError:
    aio_pika = None


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
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


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
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


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
@pytest.mark.asyncio
async def test_task_exception(communicator: kiwipy.rmq.RmqCommunicator):
    TASK = 'The meaning?'

    tasks = []

    def on_task(_comm, task):
        tasks.append(task)
        raise RuntimeError('I cannea do it Captain!')

    await communicator.add_task_subscriber(on_task)
    with pytest.raises(kiwipy.RemoteException):
        result_future = await communicator.task_send(TASK)
        await result_future

    assert tasks[0] == TASK


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
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


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
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


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
@pytest.mark.asyncio
async def test_send_no_subscribers(task_publisher: aio_pika.Connection):
    """ Test what happens when there are no task queues bound to the exchange """
    TASK = 'The meaning?'  # pylint: disable=invalid-name
    with pytest.raises(aio_pika.exceptions.DeliveryError):
        await task_publisher.task_send(TASK)


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
@pytest.mark.asyncio
async def test_queue_get_next(task_queue: rmq.RmqTaskQueue):
    """Test getting the next task from the queue"""
    result = await task_queue.task_send('Hello!')
    async with task_queue.next_task(timeout=1.) as task:
        async with task.processing() as outcome:
            assert task.body == 'Hello!'
            outcome.set_result('Goodbye')
    await result
    assert result.result() == 'Goodbye'


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
@pytest.mark.asyncio
async def test_queue_iter(task_queue: rmq.RmqTaskQueue):
    """Test iterating through a task queue"""
    results = []

    # Insert tasks
    for i in range(10):
        results.append(await task_queue.task_send(i))

    async for task in task_queue:
        async with task.processing() as outcome:
            outcome.set_result(task.body * 10)

    await asyncio.wait(results)
    assert all(result.done() for result in results)

    # Make sure there are no more tasks in the queue
    async for _ in task_queue:
        assert False, "Shouldn't get here"


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
@pytest.mark.asyncio
async def test_queue_iter_not_process(task_queue: rmq.RmqTaskQueue):
    """Check what happens when we iterate a queue but don't process all tasks"""
    outcomes = []

    # Insert tasks
    for i in range(10):
        outcomes.append(await task_queue.task_send(i))

    # Now let's see what happens when we have tasks but don't process some of them
    async for task in task_queue:
        if task.body < 5:
            future = task.process()
            future.set_result(task.body * 10)

    await asyncio.wait(outcomes[:5])
    for i, outcome in enumerate(outcomes[:5]):
        assert outcome.result() == i * 10

    # Now, to through and process the rest
    async for task in task_queue:
        future = task.process()
        future.set_result(task.body * 10)

    await asyncio.wait(outcomes)
    for i, outcome in enumerate(outcomes):
        assert outcome.result() == i * 10


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
@pytest.mark.asyncio
async def test_queue_task_forget(task_queue: rmq.RmqTaskQueue):
    """
    Check what happens when we forget to process a task we said we would
    WARNING: This test may fail when running with a debugger as it relies on the 'outcome'
    reference count dropping to zero but the debugger may be preventing this.
    """
    outcomes = [await task_queue.task_send(1)]

    # Get the first task and say that we will process it
    outcome = None
    async with task_queue.next_task() as task:
        outcome = task.process()

    with pytest.raises(kiwipy.exceptions.QueueEmpty):
        async with task_queue.next_task(timeout=1.):
            pass

    # Now let's 'forget' i.e. lose the outcome
    del outcome

    # Now the task should be back in the queue
    async with task_queue.next_task() as task:
        future = task.process()
        future.set_result(10)

    await asyncio.wait(outcomes)
    assert outcomes[0].result() == 10


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
@pytest.mark.asyncio
async def test_empty_queue(task_queue: rmq.RmqTaskQueue):
    with pytest.raises(kiwipy.exceptions.QueueEmpty):
        async with task_queue.next_task(timeout=WAIT_TIMEOUT):
            pass


@unittest.skipIf(not aio_pika, 'Requires aio_pika library and RabbitMQ')
@pytest.mark.asyncio
async def test_task_processing_exception(task_queue: rmq.RmqTaskQueue):
    """Check that if there is an exception processing a task that it is removed from the queue"""
    task_future = await task_queue.task_send('Do this')

    # The error should still get propageted in the 'worker'
    with pytest.raises(RuntimeError):
        async with task_queue.next_task() as task:
            async with task.processing():
                raise RuntimeError('Cannea do it captain!')

    # And the task sender should get a remote exception to inform them of the problem
    with pytest.raises(kiwipy.RemoteException):
        await task_future

    # The queue should now be empty
    with pytest.raises(kiwipy.QueueEmpty):
        async with task_queue.next_task(timeout=1.):
            pass
