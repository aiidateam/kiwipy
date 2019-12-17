from __future__ import absolute_import
import pytest

from . import utils

try:
    import aio_pika

    # pylint: disable=redefined-outer-name

    @pytest.fixture
    async def connection():
        conn = await aio_pika.connect_robust('amqp://guest:guest@localhost:5672/')
        yield conn
        await conn.close()

    @pytest.fixture
    async def communicator(connection):
        communicator = await utils.new_communicator(connection)
        yield communicator
        await communicator.disconnect()

except ImportError:
    pass
