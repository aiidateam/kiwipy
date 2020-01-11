import unittest
import uuid

import pytest

# pylint: disable=redefined-outer-name

try:
    import asyncio

    from async_generator import yield_, async_generator
    import aio_pika
    import aio_pika.exceptions
    from kiwipy import rmq

    @pytest.fixture
    @async_generator
    async def task_publisher(connection: aio_pika.Connection):
        exchange_name = "{}.{}".format(__file__, uuid.uuid4())
        task_queue_name = "{}.{}".format(__file__, uuid.uuid4())

        task_pub = rmq.RmqTaskPublisher(connection,
                                        queue_name=task_queue_name,
                                        exchange_name=exchange_name,
                                        testing_mode=True)
        await task_pub.connect()
        await yield_(task_pub)
        await task_pub.disconnect()

    @pytest.fixture
    @async_generator
    async def task_queue(connection: aio_pika.Connection):
        exchange_name = "{}.{}".format(__file__, uuid.uuid4())
        task_queue_name = "{}.{}".format(__file__, uuid.uuid4())

        task_pub = rmq.RmqTaskQueue(connection,
                                    queue_name=task_queue_name,
                                    exchange_name=exchange_name,
                                    testing_mode=True)
        await task_pub.connect()
        await yield_(task_pub)
        await task_pub.disconnect()

except ImportError:
    aio_pika = None


@unittest.skipIf(not aio_pika, "Requires aio_pika library and RabbitMQ")
@pytest.mark.asyncio
async def test_send_no_subscribers(task_publisher: aio_pika.Connection):
    """ Test what happens when there are no task queues bound to the exchange """
    TASK = 'The meaning?'  # pylint: disable=invalid-name
    with pytest.raises(aio_pika.exceptions.DeliveryError):
        await task_publisher.task_send(TASK)


@unittest.skipIf(not aio_pika, "Requires aio_pika library and RabbitMQ")
@pytest.mark.asyncio
async def test_queue_get_next(task_queue: rmq.RmqTaskQueue):
    """Test getting the next task from the queue"""
    result = await task_queue.task_send("Hello!")
    async with task_queue.next_task(timeout=1.) as task:
        with task.processing() as outcome:
            assert task.body == 'Hello!'
            outcome.set_result('Goodbye')
    await result
    assert result.result() == 'Goodbye'


@unittest.skipIf(not aio_pika, "Requires aio_pika library and RabbitMQ")
@pytest.mark.asyncio
async def test_queue_iter(task_queue: rmq.RmqTaskQueue):
    """Test iterating through a task queue"""
    results = []

    # Insert tasks
    for i in range(10):
        results.append(await task_queue.task_send(i))

    async for task in task_queue:
        with task.processing() as outcome:
            outcome.set_result(task.body * 10)

    await asyncio.wait(results)
    assert all(result.done() for result in results)

    # Make sure there are no more tasks in the queue
    async for _ in task_queue:
        assert False, "Shouldn't get here"


@unittest.skipIf(not aio_pika, "Requires aio_pika library and RabbitMQ")
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
            task.process().set_result(task.body * 10)

    await asyncio.wait(outcomes[:5])
    for i, outcome in enumerate(outcomes[:5]):
        assert outcome.result() == i * 10

    # Now, to through and process the rest
    async for task in task_queue:
        task.process().set_result(task.body * 10)

    await asyncio.wait(outcomes)
    for i, outcome in enumerate(outcomes):
        assert outcome.result() == i * 10


@unittest.skipIf(not aio_pika, "Requires aio_pika library and RabbitMQ")
@pytest.mark.asyncio
async def test_queue_task_forget(task_queue: rmq.RmqTaskQueue):
    """
    Check what happens when we forget to process a task we said we would
    WARNING: This test mail fail when running with a debugger as it relies on the 'outcome' reference
    count dropping to zero but the debugger may be preventing this.
    """
    outcomes = list()

    outcomes.append(await task_queue.task_send(1))

    # Get the first task and say that we will process it
    outcome = None
    async with task_queue.next_task() as task:
        outcome = task.process()

    with pytest.raises(aio_pika.exceptions.QueueEmpty):
        async with task_queue.next_task():
            pass

    # Now let's 'forget' i.e. lose the outcome
    del outcome

    # Now the task should be back in the queue
    async with task_queue.next_task() as task:
        task.process().set_result(10)

    await asyncio.wait(outcomes)
    assert outcomes[0].result() == 10
