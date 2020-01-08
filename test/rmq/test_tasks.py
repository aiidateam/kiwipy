import unittest
import uuid

import pytest

# pylint: disable=redefined-outer-name

try:
    from async_generator import yield_, async_generator
    import aio_pika
    import aio_pika.exceptions
    from kiwipy import rmq

    @pytest.fixture
    @async_generator
    async def task_publisher(connection):
        exchange_name = "{}.{}".format(__file__, uuid.uuid4())
        task_queue_name = "{}.{}".format(__file__, uuid.uuid4())

        task_pub = rmq.RmqTaskPublisher(connection,
                                        task_queue_name=task_queue_name,
                                        exchange_name=exchange_name,
                                        testing_mode=True)
        await task_pub.connect()
        await yield_(task_pub)
        await task_pub.disconnect()

except ImportError:
    aio_pika = None


@unittest.skipIf(not aio_pika, "Requires pika library and RabbitMQ")
@pytest.mark.asyncio
async def test_send_no_subscribers(task_publisher):
    """ Test what happens when there are no task queues bound to the exchange """
    TASK = 'The meaning?'  # pylint: disable=invalid-name
    with pytest.raises(aio_pika.exceptions.DeliveryError):
        await task_publisher.task_send(TASK)
