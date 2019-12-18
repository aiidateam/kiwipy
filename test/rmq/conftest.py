from __future__ import absolute_import
import pytest

from . import utils

try:
    import aio_pika
    from async_generator import yield_, async_generator

    # pylint: disable=redefined-outer-name

    @pytest.fixture
    @async_generator
    async def connection():
        conn = await aio_pika.connect_robust('amqp://guest:guest@localhost:5672/')
        await yield_(conn)
        await conn.close()

    @pytest.fixture
    @async_generator
    async def communicator(connection):
        communicator = await utils.new_communicator(connection)
        await yield_(communicator)
        await communicator.disconnect()

except ImportError:
    pass
