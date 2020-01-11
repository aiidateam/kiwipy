import asyncio
from contextlib import contextmanager
import concurrent.futures
from concurrent.futures import Future as ThreadFuture
import functools
from functools import partial
import logging
import threading

import aio_pika
import pamqp

import kiwipy
from . import defaults
from . import communicator
from . import tasks

__all__ = ('RmqThreadCommunicator',)

_LOGGER = logging.getLogger(__name__)


class RmqThreadCommunicator(kiwipy.Communicator):
    """
    RabbitMQ communicator that runs an event loop on a separate thread to do communication.
    This also means that heartbeats are not missed and the main program is free to block for
    as long as it wants.
    """

    TASK_TIMEOUT = 5.

    @classmethod
    def connect(cls,
                connection_params=None,
                connection_factory=aio_pika.connect_robust,
                loop=None,
                message_exchange=defaults.MESSAGE_EXCHANGE,
                task_exchange=defaults.TASK_EXCHANGE,
                task_queue=defaults.TASK_QUEUE,
                task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
                task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
                encoder=defaults.ENCODER,
                decoder=defaults.DECODER,
                testing_mode=False):
        # pylint: disable=too-many-arguments
        connection_params = connection_params or {}
        # Create a new loop if one isn't supplied
        loop = loop or asyncio.new_event_loop()
        loop.set_debug(testing_mode)
        connection_params['loop'] = loop

        # Run the loop to create the connection
        connection = loop.run_until_complete(connection_factory(**connection_params))
        comm = cls(connection,
                   message_exchange=message_exchange,
                   task_exchange=task_exchange,
                   task_queue=task_queue,
                   task_prefetch_size=task_prefetch_size,
                   task_prefetch_count=task_prefetch_count,
                   encoder=encoder,
                   decoder=decoder,
                   testing_mode=testing_mode)

        # Start the communicator
        comm.start()
        return comm

    def __init__(self,
                 connection,
                 message_exchange: str = defaults.MESSAGE_EXCHANGE,
                 task_exchange=defaults.TASK_EXCHANGE,
                 task_queue: str = defaults.TASK_QUEUE,
                 task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
                 task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
                 encoder=defaults.ENCODER,
                 decoder=defaults.DECODER,
                 testing_mode=False):
        # pylint: disable=too-many-arguments
        """
        :param connection: The RMQ connector object
        :type connection: :class:`aio_pika.Connection`
        :param message_exchange: The name of the RMQ message exchange to use
        :type message_exchange: str
        :param task_exchange: The name of the RMQ task exchange to use
        :type task_exchange: str
        :param task_queue: The name of the task queue to use
        :type task_queue: str
        :param encoder: The encoder to call for encoding a message
        :param decoder: The decoder to call for decoding a message
        :param testing_mode: Run in testing mode: all queues and exchanges
            will be temporary
        """
        self._communicator = communicator.RmqCommunicator(connection,
                                                          message_exchange=message_exchange,
                                                          task_exchange=task_exchange,
                                                          task_queue=task_queue,
                                                          encoder=encoder,
                                                          decoder=decoder,
                                                          task_prefetch_size=task_prefetch_size,
                                                          task_prefetch_count=task_prefetch_count,
                                                          testing_mode=testing_mode)
        self._loop = self._communicator.loop  # type: asyncio.AbstractEventLoop
        self._communicator_thread = None
        self._stop_signal = None
        self._closed = False

    def __enter__(self):
        self._ensure_running()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def loop(self):
        return self._loop

    def is_closed(self) -> bool:
        return self._closed

    def close(self):
        if self.is_closed():
            return
        self.stop()
        self._closed = True

    def start(self):
        assert self._communicator_thread is None, "Already running"

        start_future = ThreadFuture()

        self._communicator_thread = threading.Thread(target=self._run_loop,
                                                     name="Communications thread for '{}'".format(self._communicator),
                                                     args=(start_future,),
                                                     daemon=True)
        self._communicator_thread.start()
        start_future.result()

    def stop(self):
        comm_thread = self._communicator_thread
        if comm_thread is None:
            return

        stop_future = ThreadFuture()
        # Send the stop signal
        self._loop.call_soon_threadsafe(partial(self._stop_signal.set_result, stop_future))
        # Wait for the result in case there was an exception
        stop_future.result()
        comm_thread.join()

    def add_rpc_subscriber(self, subscriber, identifier=None):
        self._ensure_open()
        coro = self._communicator.add_rpc_subscriber(self._wrap_subscriber(subscriber), identifier)
        return self._run_task(coro)

    def remove_rpc_subscriber(self, identifier):
        self._ensure_open()
        coro = self._communicator.remove_rpc_subscriber(identifier)
        return self._run_task(coro)

    def add_task_subscriber(self, subscriber):
        self._ensure_open()
        coro = self._communicator.add_task_subscriber(self._wrap_subscriber(subscriber))
        return self._run_task(coro)

    def remove_task_subscriber(self, subscriber):
        self._ensure_open()
        coro = self._communicator.remove_task_subscriber(subscriber)
        return self._run_task(coro)

    def add_broadcast_subscriber(self, subscriber, identifier=None):
        self._ensure_open()
        coro = self._communicator.add_broadcast_subscriber(subscriber, identifier)
        return self._run_task(coro)

    def remove_broadcast_subscriber(self, identifier):
        self._ensure_open()
        coro = self._communicator.remove_broadcast_subscriber(identifier)
        return self._run_task(coro)

    def task_send(self, task, no_reply=False):
        self._ensure_open()
        self._ensure_running()
        future = self._run_task(self._communicator.task_send(task, no_reply))
        if no_reply:
            return None

        return future

    def task_queue(self,
                   queue_name: str,
                   prefetch_size=defaults.TASK_PREFETCH_SIZE,
                   prefetch_count=defaults.TASK_PREFETCH_COUNT):
        self._ensure_open()
        self._ensure_running()
        coro = self._communicator.task_queue(queue_name, prefetch_size, prefetch_count)
        aioqueue = self._run_task(coro)
        return RmqThreadTaskQueue(aioqueue, self._run_task, self._wrap_subscriber)

    def rpc_send(self, recipient_id, msg):
        self._ensure_open()
        self._ensure_running()
        return self._run_task(self._communicator.rpc_send(recipient_id, msg))

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        self._ensure_open()
        self._ensure_running()
        coro = self._communicator.broadcast_send(body=body,
                                                 sender=sender,
                                                 subject=subject,
                                                 correlation_id=correlation_id)
        result = self._run_task(coro)
        return isinstance(result, pamqp.specification.Basic.Ack)

    def _send_message(self, coro):
        send_future = kiwipy.Future()

        async def do_task():
            with kiwipy.capture_exceptions(send_future):
                send_future.set_result((await coro()))

        self._loop.call_soon(do_task)
        return send_future.result()

    def _wrap_subscriber(self, subscriber):
        """"
        We need to convert any kiwipy.Futures we get from a subscriber call into asyncio ones for the event loop
        based communicator.  Do this by wrapping any subscriber methods and intercepting the return values.
        """

        @functools.wraps(subscriber)
        def wrapper(*args, **kwargs):
            result = subscriber(*args, **kwargs)
            if isinstance(result, kiwipy.Future):
                result = self._wrap_future(result)
            return result

        return wrapper

    def _wrap_future(self, kiwi_future: kiwipy.Future):
        aio_future = self._loop.create_future()

        def done(_):
            try:
                result = kiwi_future.result()
            except concurrent.futures.CancelledError:
                aio_future.cancel()
            except Exception as exc:  # pylint: disable=broad-except
                aio_future.set_exception(exc)
            else:
                if isinstance(result, kiwipy.Future):
                    result = self._wrap_future(result)
                self._loop.call_soon_threadsafe(aio_future.set_result, result)

        kiwi_future.add_done_callback(done)
        return aio_future

    def _loop_chain_future(self, aio_future: asyncio.Future, kiwi_future: kiwipy.Future):

        def done(done_future: asyncio.Future):
            # Copy over the future
            try:
                result = done_future.result()
                if asyncio.isfuture(result):
                    # Change the future type to a kiwi one
                    fut = kiwipy.Future()
                    self._loop_chain_future(result, fut)
                    result = fut

                kiwi_future.set_result(result)
            except asyncio.CancelledError:
                kiwi_future.cancel()
            except Exception as exception:  # pylint: disable=broad-except
                kiwi_future.set_exception(exception)

        aio_future.add_done_callback(done)
        return kiwi_future

    def _run_task(self, coro):
        """
        Run a coroutine on the event loop and return the result.  It may take a little time for the loop
        to get around to scheduling it so we use a timeout as set by the TASK_TIMEOUT class constant.

        :param coro: the coroutine to run
        :return: the result of running the coroutine
        """
        return self._schedule_task(coro).result(timeout=self.TASK_TIMEOUT)

    def _schedule_task(self, coro) -> kiwipy.Future:
        """
        Schedule a coroutine on the loop and return the corresponding future
        """
        kiwi_future = kiwipy.Future()

        def loop_schedule():
            # Here we're on the comms thread again
            async def proxy():
                if not kiwi_future.cancelled():
                    return await coro

            coro_future = asyncio.ensure_future(proxy(), loop=self._loop)
            self._loop_chain_future(coro_future, kiwi_future)

        handle = self._loop.call_soon_threadsafe(loop_schedule)

        def handle_cancel(done_future: kiwipy.Future):
            """Function to propagate a cancellation of the kiwipy future up to the loop callback"""
            if done_future.cancelled():
                # Don't know if we're allowed to call cancel from a thread other than comms thread!
                # If not, we'll need to call_soon_threadsafe
                handle.cancel()

        kiwi_future.add_done_callback(handle_cancel)

        return kiwi_future

    def _ensure_running(self):
        if self._communicator_thread is not None:
            return
        self.start()

    def _run_loop(self, start_future):
        """Here we are on the comms thread"""
        _LOGGER.debug('Starting event loop (id %s) on %s', id(self._loop), threading.current_thread())

        self._stop_signal = self._loop.create_future()

        async def do_connect():
            try:
                # Connect
                await self._communicator.connect()
                start_future.set_result(True)
            except Exception as exception:  # pylint: disable=broad-except
                start_future.set_exception(exception)
            else:
                # Wait to stop
                stop_future = await self._stop_signal
                try:
                    # Now disconnect
                    with kiwipy.capture_exceptions(stop_future):
                        await self._communicator.disconnect()
                finally:
                    stop_future.set_result(True)

        self._loop.run_until_complete(do_connect())

        # The loop is finished and the connection has been disconnected
        self._communicator_thread = None

        _LOGGER.debug('Event loop stopped on %s', threading.current_thread())
        asyncio.set_event_loop(None)

    def _ensure_open(self):
        if self.is_closed():
            raise kiwipy.CommunicatorClosed


class RmqThreadTaskQueue:

    def __init__(self, task_queue: tasks.RmqTaskQueue, run_task, wrap_subscriber):
        self._task_queue = task_queue
        self._run_task = run_task
        self._wrap_subscriber = wrap_subscriber

    def task_send(self, task, no_reply=False):
        future = self._run_task(self._task_queue.task_send(task, no_reply))
        if no_reply:
            return None

        return future

    def add_task_subscriber(self, subscriber):
        coro = self._task_queue.add_task_subscriber(self._wrap_subscriber(subscriber))
        return self._run_task(coro)

    def remove_task_subscriber(self, subscriber):
        coro = self._task_queue.remove_task_subscriber(subscriber)
        return self._run_task(coro)

    @contextmanager
    def next_task(self):

        async def get_next_task():
            async with self._task_queue.next_task() as task:
                task.process()
                return task

        task = self._run_task(get_next_task())
        try:
            yield task
        finally:
            pass


def connect(connection_params=None,
            connection_factory=aio_pika.connect_robust,
            loop=None,
            message_exchange=defaults.MESSAGE_EXCHANGE,
            task_exchange=defaults.TASK_EXCHANGE,
            task_queue=defaults.TASK_QUEUE,
            task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
            task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
            encoder=defaults.ENCODER,
            decoder=defaults.DECODER,
            testing_mode=False) -> RmqThreadCommunicator:
    # pylint: disable=too-many-arguments
    return RmqThreadCommunicator.connect(connection_params=connection_params,
                                         connection_factory=connection_factory,
                                         loop=loop,
                                         message_exchange=message_exchange,
                                         task_exchange=task_exchange,
                                         task_queue=task_queue,
                                         task_prefetch_size=task_prefetch_size,
                                         task_prefetch_count=task_prefetch_count,
                                         encoder=encoder,
                                         decoder=decoder,
                                         testing_mode=testing_mode)
