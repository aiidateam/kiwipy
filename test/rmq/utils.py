# -*- coding: utf-8 -*-
import random
import string

import shortuuid

import kiwipy
from kiwipy import rmq


async def new_communicator(connection_params, settings=None) -> kiwipy.rmq.RmqCommunicator:
    settings = settings or {}

    message_exchange = f'{__file__}.{shortuuid.uuid()}'
    task_exchange = f'{__file__}.{shortuuid.uuid()}'
    task_queue = f'{__file__}.{shortuuid.uuid()}'

    return await rmq.async_connect(
        connection_params,
        message_exchange=message_exchange,
        task_exchange=task_exchange,
        task_queue=task_queue,
        testing_mode=True,
        **settings
    )


def rand_string(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))
