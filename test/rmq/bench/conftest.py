# -*- coding: utf-8 -*-
import functools

import msgpack
import pika
import pytest

import kiwipy
from kiwipy import rmq

from . import support


@pytest.fixture
def comm(connection_params):  # pylint: disable=redefined-outer-name
    encoder = functools.partial(msgpack.packb, use_bin_type=True)
    decoder = functools.partial(msgpack.unpackb, raw=False)

    with kiwipy.connect(connection_params['url'], encoder=encoder, decoder=decoder) as communicator:
        yield communicator


@pytest.fixture
def bench_queue(comm: rmq.RmqThreadCommunicator):  # pylint: disable=redefined-outer-name
    queue = comm.task_queue('bench-queue')
    try:
        yield queue
    finally:
        support.clear_all_tasks(queue)


@pytest.fixture
def pika_connection(connection_params):
    connection = pika.BlockingConnection(pika.URLParameters(connection_params['url']))
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def pika_channel(pika_connection):  # pylint: disable=redefined-outer-name
    channel = pika_connection.channel()
    try:
        yield channel
    finally:
        channel.close()


@pytest.fixture
def pika_queue(pika_channel):  # pylint: disable=redefined-outer-name
    queue_name = 'pika-bench-queue'
    pika_channel.queue_declare(queue=queue_name, durable=True)
    try:
        yield queue_name
    finally:
        pika_channel.queue_delete(queue_name)
