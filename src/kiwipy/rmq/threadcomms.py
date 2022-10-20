# -*- coding: utf-8 -*-
import asyncio
import concurrent.futures
from concurrent.futures import Future as ThreadFuture
from contextlib import contextmanager
import functools
import logging
from typing import Dict, Union

import aio_pika
import pamqp
from pytray import aiothreads

import kiwipy
from kiwipy import __version__

from . import communicator, defaults, tasks

__all__ = 'RmqThreadCommunicator', 'RmqThreadTaskQueue', 'connect'

_LOGGER = logging.getLogger(__name__)


class RmqThreadCommunicator(kiwipy.Communicator):
    """
    RabbitMQ communicator that runs an event loop on a separate thread to do communication.
    This also means that heartbeats are not missed and the main program is free to block for
    as long as it wants.
    """

    TASK_TIMEOUT = 5.

    @classmethod
    def connect(
        cls,
        connection_params: Union[str, dict] = None,
        connection_factory=aio_pika.connect_robust,
        message_exchange=defaults.MESSAGE_EXCHANGE,
        task_exchange=defaults.TASK_EXCHANGE,
        task_queue=defaults.TASK_QUEUE,
        task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
        task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
        encoder=defaults.ENCODER,
        decoder=defaults.DECODER,
        testing_mode=False,
        async_task_timeout=TASK_TIMEOUT,
    ):
        # pylint: disable=too-many-arguments
        comm = cls(
            connection_params,
            connection_factory,
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            task_prefetch_size=task_prefetch_size,
            task_prefetch_count=task_prefetch_count,
            encoder=encoder,
            decoder=decoder,
            testing_mode=testing_mode,
            async_task_timeout=async_task_timeout
        )

        # Start the communicator
        return comm

    def __init__(
        self,
        connection_params: Union[str, dict] = None,
        connection_factory=aio_pika.connect_robust,
        message_exchange: str = defaults.MESSAGE_EXCHANGE,
        queue_expires: int = defaults.QUEUE_EXPIRES,
        task_exchange=defaults.TASK_EXCHANGE,
        task_queue: str = defaults.TASK_QUEUE,
        task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
        task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
        encoder=defaults.ENCODER,
        decoder=defaults.DECODER,
        testing_mode=False,
        async_task_timeout=TASK_TIMEOUT,
    ):
        # pylint: disable=too-many-arguments
        """
        :param connection_params: parameters that will be passed to the connection factory to create the connection
        :param connection_factory: the factory method to open the aio_pika connection with
        :param message_exchange: The name of the RMQ message exchange to use
        :param queue_expires: the expiry time for standard queues in milliseconds. This is the time after which, if
            there are no subscribers, a queue will automatically be deleted by RabbitMQ.
        :param task_exchange: The name of the RMQ task exchange to use
        :param task_queue: The name of the task queue to use
        :param task_prefetch_count: the number of tasks this communicator can fetch simultaneously
        :param task_prefetch_size: the total size of the messages that the default queue can fetch simultaneously
        :param encoder: The encoder to call for encoding a message
        :param decoder: The decoder to call for decoding a message
        :param testing_mode: Run in testing mode: all queues and exchanges will be temporary
        """
        # Always use a separate loop
        self._loop = asyncio.new_event_loop()
        self._loop.set_debug(testing_mode)
        self._loop_scheduler = aiothreads.LoopScheduler(self._loop, 'RMQ communicator', async_task_timeout)
        self._stop_signal = None
        self._closed = False

        self._loop_scheduler.start()  # Star the loop scheduler (i.e. the event loop thread)

        # Establish the connection and get a communicator running on our thread
        self._communicator: communicator.RmqCommunicator = self._loop_scheduler.await_(
            communicator.async_connect(
                connection_params=connection_params,
                connection_factory=connection_factory,
                # Messages
                message_exchange=message_exchange,
                queue_expires=queue_expires,

                # Tasks
                task_exchange=task_exchange,
                task_queue=task_queue,
                task_prefetch_size=task_prefetch_size,
                task_prefetch_count=task_prefetch_count,
                encoder=encoder,
                decoder=decoder,
                testing_mode=testing_mode
            )
        )

    @property
    def server_properties(self) -> Dict:
        """
        A dictionary containing server properties as returned by the RMQ server at connection time.

        The details are defined by the RMQ standard and can be found here:

        https://www.rabbitmq.com/amqp-0-9-1-reference.html#connection.start.server-properties

        The protocol states that this dictionary SHOULD contain at least:
            'host'          - specifying the server host name or address
            'product'       - giving the name of the server product
            'version'       - giving the name of the server version
            'platform'      - giving the name of the operating system
            'copyright'     - if appropriate, and,
            'information'   - giving other general information

        .. note:: In testing it seems like 'host' is not always returned.  Host information may be found in
            'cluster_name' but clients shouldn't rely on this.

        :return: the server properties dictionary
        """
        return self._communicator.server_properties

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def loop(self):
        return self._loop_scheduler.loop()

    def is_closed(self) -> bool:
        return self._closed

    def close(self):
        """Close down the communicator and clean up all resources.

        After this call it cannot be used again.
        """
        if self.is_closed():
            return

        self._loop_scheduler.await_(self._communicator.disconnect())
        self._loop_scheduler.close()
        self._loop.close()

        # Clean up
        del self._communicator
        del self._loop_scheduler
        del self._loop
        self._closed = True

    def add_close_callback(self, callback: aio_pika.abc.ConnectionCloseCallback, weak: bool = False) -> None:
        """Add a callable to be called each time (after) the connection is closed.

        :param weak: If True, the callback will be added to a `WeakSet`
        """
        self._ensure_open()
        self._communicator.add_close_callback(callback, weak)

    def add_rpc_subscriber(self, subscriber, identifier=None):
        self._ensure_open()
        return self._loop_scheduler.await_(
            self._communicator.add_rpc_subscriber(self._wrap_subscriber(subscriber), identifier)
        )

    def remove_rpc_subscriber(self, identifier):
        self._ensure_open()
        return self._loop_scheduler.await_(self._communicator.remove_rpc_subscriber(identifier))

    def add_task_subscriber(self, subscriber, identifier=None):
        self._ensure_open()
        return self._loop_scheduler.await_(
            self._communicator.add_task_subscriber(self._wrap_subscriber(subscriber), identifier)
        )

    def remove_task_subscriber(self, identifier):
        self._ensure_open()
        return self._loop_scheduler.await_(self._communicator.remove_task_subscriber(identifier))

    def add_broadcast_subscriber(self, subscriber, identifier=None):
        self._ensure_open()
        return self._loop_scheduler.await_(self._communicator.add_broadcast_subscriber(subscriber, identifier))

    def remove_broadcast_subscriber(self, identifier):
        self._ensure_open()
        return self._loop_scheduler.await_(self._communicator.remove_broadcast_subscriber(identifier))

    def task_send(self, task, no_reply=False):
        self._ensure_open()
        return self._loop_scheduler.await_(self._communicator.task_send(task, no_reply))

    def task_queue(
        self, queue_name: str, prefetch_size=defaults.TASK_PREFETCH_SIZE, prefetch_count=defaults.TASK_PREFETCH_COUNT
    ):
        self._ensure_open()
        aioqueue = self._loop_scheduler.await_(self._communicator.task_queue(queue_name, prefetch_size, prefetch_count))
        return RmqThreadTaskQueue(aioqueue, self._loop_scheduler, self._wrap_subscriber)

    def rpc_send(self, recipient_id, msg):
        self._ensure_open()
        return self._loop_scheduler.await_(self._communicator.rpc_send(recipient_id, msg))

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        self._ensure_open()
        result = self._loop_scheduler.await_(
            self._communicator.broadcast_send(body=body, sender=sender, subject=subject, correlation_id=correlation_id)
        )
        return isinstance(result, pamqp.commands.Basic.Ack)

    def _wrap_subscriber(self, subscriber):
        """"
        We need to convert any kiwipy.Futures we get from a subscriber call into asyncio ones for
        the event loop based communicator.  Do this by wrapping any subscriber methods and
        intercepting the return values.
        """

        @functools.wraps(subscriber)
        def wrapper(*args, **kwargs):
            result = subscriber(*args, **kwargs)
            if isinstance(result, ThreadFuture):
                result = self._wrap_future(result)
            return result

        return wrapper

    def _wrap_future(self, kiwi_future: kiwipy.Future):
        aio_future = self._loop.create_future()

        def done(_):
            try:
                result = kiwi_future.result()
            except concurrent.futures.CancelledError:
                self._loop.call_soon_threadsafe(aio_future.cancel)
            except Exception as exc:  # pylint: disable=broad-except
                self._loop.call_soon_threadsafe(aio_future.set_exception, exc)
            else:
                if isinstance(result, kiwipy.Future):
                    result = self._wrap_future(result)
                self._loop.call_soon_threadsafe(aio_future.set_result, result)

        kiwi_future.add_done_callback(done)
        return aio_future

    def _ensure_open(self):
        if self.is_closed():
            raise kiwipy.CommunicatorClosed


class RmqThreadTaskQueue:
    """
    Thread task queue.
    """

    def __init__(self, task_queue: tasks.RmqTaskQueue, loop_scheduler: aiothreads.LoopScheduler, wrap_subscriber):
        self._task_queue = task_queue
        self._loop_scheduler = loop_scheduler
        self._wrap_subscriber = wrap_subscriber

    def __iter__(self):
        for task in self._loop_scheduler.async_iter(self._task_queue):
            yield RmqThreadIncomingTask(task, self._loop_scheduler)

    def task_send(self, task, no_reply=False):
        return self._loop_scheduler.await_(self._task_queue.task_send(task, no_reply))

    def add_task_subscriber(self, subscriber):
        return self._loop_scheduler.await_(self._task_queue.add_task_subscriber(self._wrap_subscriber(subscriber)))

    def remove_task_subscriber(self, subscriber):
        # Note: This probably doesn't work as in add_task_subscriber we wrap it and so
        # it will be a different function here
        return self._loop_scheduler.await_(self._task_queue.remove_task_subscriber(subscriber))

    @contextmanager
    def next_task(self, timeout=5., fail=True):
        with self._loop_scheduler.async_ctx(self._task_queue.next_task(timeout=timeout, fail=fail)) as task:
            yield RmqThreadIncomingTask(task, self._loop_scheduler)


class RmqThreadIncomingTask:

    def __init__(self, task: tasks.RmqIncomingTask, loop_scheduler):
        self._task = task
        self._loop_scheduler = loop_scheduler

    @property
    def body(self) -> str:
        return self._task.body

    @property
    def no_reply(self) -> bool:
        return self._task.no_reply

    @property
    def state(self) -> str:
        return self._task.state

    def process(self) -> ThreadFuture:
        return aiothreads.aio_future_to_thread(self._task.process())

    def requeue(self):
        """Requeue the task.

        This call is blocking and by the time function returns the task will be back in the queue.
        """
        self._loop_scheduler.await_(self._task.requeue())

    @contextmanager
    def processing(self):
        with self._loop_scheduler.async_ctx(self._task.processing()) as outcome:
            yield outcome


def connect(
    connection_params: Union[str, dict] = None,
    connection_factory=aio_pika.connect_robust,
    message_exchange=defaults.MESSAGE_EXCHANGE,
    task_exchange=defaults.TASK_EXCHANGE,
    task_queue=defaults.TASK_QUEUE,
    task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
    task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
    encoder=defaults.ENCODER,
    decoder=defaults.DECODER,
    testing_mode=False,
) -> RmqThreadCommunicator:
    """
    Establish a RabbitMQ communicator connection
    """
    # pylint: disable=too-many-arguments
    _communicator = RmqThreadCommunicator.connect(
        connection_params=connection_params,
        connection_factory=connection_factory,
        message_exchange=message_exchange,
        task_exchange=task_exchange,
        task_queue=task_queue,
        task_prefetch_size=task_prefetch_size,
        task_prefetch_count=task_prefetch_count,
        encoder=encoder,
        decoder=decoder,
        testing_mode=testing_mode
    )
    return _communicator
