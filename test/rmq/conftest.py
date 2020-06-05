# -*- coding: utf-8 -*-
import os

import pytest

from . import utils

ENV_KIWI_RMQ_URI = 'KIWIPY_TEST_RMQ_URI'
DEFAULT_RMQ_URI = 'amqp://guest:guest@127.0.0.1:5672/'

try:
    import aio_pika
    from async_generator import yield_, async_generator

    # pylint: disable=redefined-outer-name

    @pytest.fixture
    def connection_params() -> dict:
        return {'url': os.environ.get(ENV_KIWI_RMQ_URI, DEFAULT_RMQ_URI)}

    @pytest.fixture
    @async_generator
    async def connection(connection_params: dict):
        conn = await aio_pika.connect_robust(**connection_params)
        await yield_(conn)
        await conn.close()

    @pytest.fixture
    @async_generator
    async def communicator(connection_params):
        communicator = await utils.new_communicator(connection_params)
        await yield_(communicator)
        await communicator.disconnect()

except ImportError:
    pass
