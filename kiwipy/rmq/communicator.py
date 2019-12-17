from __future__ import absolute_import
from __future__ import print_function
import asyncio
import concurrent.futures
from concurrent.futures import Future as ThreadFuture
import functools
from functools import partial
import copy
import logging
import threading

import shortuuid
import aio_pika

import kiwipy
from . import defaults
from . import tasks
from . import messages
from . import utils

__all__ = ['RmqCommunicator', 'RmqThreadCommunicator', 'connect']

_LOGGER = logging.getLogger(__name__)

# The exchange properties use by the publisher and subscriber.  These have to match
# which is why they're declare her
EXCHANGE_PROPERTIES = {'type': aio_pika.ExchangeType.TOPIC}


class RmqPublisher(messages.BasePublisherWithReplyQueue):
    """
    Publisher for sending a range of message types over RMQ
    """
    DEFAULT_EXCHANGE_PARAMS = EXCHANGE_PROPERTIES

    async def rpc_send(self, recipient_id, msg):
        message = aio_pika.Message(body=self._encode(msg), reply_to=self._reply_queue.name)
        published, response_future = await self.publish_expect_response(message,
                                                                        routing_key="{}.{}".format(
                                                                            defaults.RPC_TOPIC, recipient_id),
                                                                        mandatory=True)
        assert published, "The message was not published to the exchanges"
        return response_future

    async def broadcast_send(self, msg, sender=None, subject=None, correlation_id=None):
        message_dict = messages.BroadcastMessage.create(
            body=msg,
            sender=sender,
            subject=subject,
            correlation_id=correlation_id,
        )
        message = aio_pika.Message(
            body=self._encode(message_dict),
            delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT,
        )
        try:
            result = await self.publish(message, routing_key=defaults.BROADCAST_TOPIC)
        except aio_pika.exceptions.DeliveryError as exception:
            _LOGGER.debug('Broadcast message sent but was unroutable, probably no queues: %s', str(exception))
            # Doesn't matter because broadcasts are meant to be fire-and-forget anyway
            result = True
        return result


class RmqSubscriber:
    """
    Subscriber for receiving a range of messages over RMQ
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self,
                 connection,
                 message_exchange=defaults.MESSAGE_EXCHANGE,
                 queue_expires=defaults.QUEUE_EXPIRES,
                 decoder=defaults.DECODER,
                 encoder=defaults.ENCODER,
                 testing_mode=False):
        # pylint: disable=too-many-arguments
        """
        Subscribes and listens for process control messages and acts on them
        by calling the corresponding methods of the process manager.

        :param connection: The tokpia connection
        :type connection: :class:`aio_pika.Connection`
        :param message_exchange: The name of the exchange to use
        :param queue_expires: the expiry time for standard queues in milliseconds.  This is the time after which, if
            there are no subscribers, a queue will automatically be deleted by RabbitMQ.
        :type queue_expires: int
        :param encoder: The encoder to call for encoding a message
        :param decoder: The decoder to call for decoding a message
        :param testing_mode: Run in testing mode: all queues and exchanges
            will be temporary
        """
        super(RmqSubscriber, self).__init__()

        self._connection = connection
        self._channel = None  # type: aio_pika.Channel
        self._exchange = None  # type: aio_pika.Exchange
        self._exchange_name = message_exchange
        self._decode = decoder
        self._testing_mode = testing_mode
        self._response_encode = encoder

        self._broadcast_queue_arguments = {'x-message-ttl': defaults.MESSAGE_TTL}

        self._rmq_queue_arguments = {'x-message-ttl': defaults.MESSAGE_TTL}
        if queue_expires:
            self._rmq_queue_arguments['x-expires'] = queue_expires

        self._rpc_subscribers = {}
        self._broadcast_subscribers = {}
        self._broadcast_queue = None  # type: aio_pika.Queue
        self._broadcast_consumer_tag = None

        self._active = False

    async def add_rpc_subscriber(self, subscriber, identifier=None):
        # Create an RPC queue
        rpc_queue = await self._channel.declare_queue(exclusive=True, arguments=self._rmq_queue_arguments)
        try:
            identifier = await rpc_queue.consume(partial(self._on_rpc, subscriber), consumer_tag=identifier)
        except aio_pika.exceptions.DuplicateConsumerTag:
            raise kiwipy.DuplicateSubscriberIdentifier("RPC identifier '{}'".format(identifier))
        else:
            await rpc_queue.bind(self._exchange, routing_key='{}.{}'.format(defaults.RPC_TOPIC, identifier))
            # Save the queue so we can cancel and unbind later
            self._rpc_subscribers[identifier] = rpc_queue
            return identifier

    async def remove_rpc_subscriber(self, identifier):
        try:
            rpc_queue = self._rpc_subscribers.pop(identifier)
        except KeyError:
            raise ValueError("Unknown subscriber '{}'".format(identifier))
        else:
            await rpc_queue.cancel(identifier)
            await rpc_queue.unbind(self._exchange, routing_key='{}.{}'.format(defaults.RPC_TOPIC, identifier))

    async def add_broadcast_subscriber(self, subscriber, identifier=None):
        identifier = identifier or shortuuid.uuid()
        if identifier in self._broadcast_subscribers:
            raise kiwipy.DuplicateSubscriberIdentifier("Broadcast identifier '{}'".format(identifier))

        self._broadcast_subscribers[identifier] = subscriber
        if self._broadcast_consumer_tag is None:
            # Consume on the broadcast queue
            self._broadcast_consumer_tag = await self._broadcast_queue.consume(self._on_broadcast)
        return identifier

    async def remove_broadcast_subscriber(self, identifier):
        try:
            del self._broadcast_subscribers[identifier]
        except KeyError:
            raise ValueError("Broadcast subscriber '{}' unknown".format(identifier))
        if not self._broadcast_subscribers:
            await self._broadcast_queue.cancel(self._broadcast_consumer_tag)
            self._broadcast_consumer_tag = None

    def channel(self):
        return self._channel

    async def connect(self):
        """Get a channel and set up all the exchanges/queues we need"""
        if self._channel:
            # Already connected
            return

        exchange_params = copy.copy(EXCHANGE_PROPERTIES)

        if self._testing_mode:
            exchange_params.setdefault('auto_delete', self._testing_mode)

        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(name=self._exchange_name, **exchange_params)

        await self._create_broadcast_queue()

    async def _create_broadcast_queue(self):
        """
        Create and bind the broadcast queue

        One is used for all broadcasts on this exchange
        """
        # Create a new such that we can see this is the broadcast queue
        name = "broadcast-{}".format(shortuuid.uuid())
        self._broadcast_queue = await self._channel.declare_queue(name=name,
                                                                  exclusive=True,
                                                                  arguments=self._broadcast_queue_arguments)
        await self._broadcast_queue.bind(self._exchange, routing_key=defaults.BROADCAST_TOPIC)

    async def disconnect(self):
        await self._channel.close()
        self._exchange = None
        self._channel = None

    async def _on_rpc(self, subscriber, message):
        """
        :param subscriber: the subscriber function or coroutine that will get the RPC message
        :param message: the RMQ message
        :type message: :class:`aio_pika.message.IncomingMessage`
        """
        with message.process(ignore_processed=True):
            # Tell the sender that we've dealt with it
            message.ack()
            msg = self._decode(message.body)

            try:
                receiver = utils.ensure_coroutine(subscriber)
                result = await receiver(self, msg)
            except Exception as exception:  # pylint: disable=broad-except
                # We had an exception in  calling the receiver
                await self._send_response(message.reply_to, message.correlation_id, utils.exception_response(exception))
            else:
                if asyncio.isfuture(result):
                    await self._send_future_response(result, message.reply_to, message.correlation_id)
                else:
                    # All good, send the response out
                    await self._send_response(message.reply_to, message.correlation_id, utils.result_response(result))

    async def _on_broadcast(self, message):
        with message.process():
            msg = self._decode(message.body)
            for receiver in self._broadcast_subscribers.values():
                try:
                    receiver = utils.ensure_coroutine(receiver)
                    await receiver(self, msg[messages.BroadcastMessage.BODY], msg[messages.BroadcastMessage.SENDER],
                                   msg[messages.BroadcastMessage.SUBJECT],
                                   msg[messages.BroadcastMessage.CORRELATION_ID])
                except Exception:  # pylint: disable=broad-except
                    _LOGGER.exception('Exception in broadcast receiver')

    async def _send_future_response(self, future, reply_to, correlation_id):
        """
        The RPC call returned a future which means we need to send a pending response
        and send a further message when the future resolves.  If it resolves to another future
        we should send out a further pending response and so on.

        :param future: the future from the RPC call
        :type future: :class:`asyncio.Future`
        :param reply_to: the recipient
        :param correlation_id: the correlation id
        """
        try:
            # Keep looping in case we're in a situation where a future resolves to a future etc.
            while asyncio.isfuture(future):
                # Send out a message saying that we're waiting for a future to complete
                await self._send_response(reply_to, correlation_id, utils.pending_response())
                future = await future
        except kiwipy.CancelledError as exception:
            # Send out a cancelled response
            await self._send_response(reply_to, correlation_id, utils.cancelled_response(str(exception)))
        except Exception as exception:  # pylint: disable=broad-except
            # Send out an exception response
            await self._send_response(reply_to, correlation_id, utils.exception_response(exception))
        else:
            # We have a final result so send that as the response
            await self._send_response(reply_to, correlation_id, utils.result_response(future))

    async def _send_response(self, reply_to, correlation_id, response):
        assert reply_to, "Must provide an identifier for the recipient"

        message = aio_pika.Message(body=self._response_encode(response), correlation_id=correlation_id)
        result = await self._exchange.publish(message, routing_key=reply_to)
        return result


class RmqCommunicator:
    """
    A publisher and subscriber using aio_pika and an asyncio event loop
    """

    def __init__(self,
                 connection,
                 message_exchange=defaults.MESSAGE_EXCHANGE,
                 task_exchange=defaults.TASK_EXCHANGE,
                 task_queue=defaults.TASK_QUEUE,
                 task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
                 task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
                 queue_expires=defaults.QUEUE_EXPIRES,
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
        :param queue_expires: the expiry time for standard queues in milliseconds.  This is the time after which, if
            there are no subscribers, a queue will automatically be deleted by RabbitMQ.
        :type queue_expires: int
        :param encoder: The encoder to call for encoding a message
        :param decoder: The decoder to call for decoding a message
        :param testing_mode: Run in testing mode: all queues and exchanges
            will be temporary
        """
        super(RmqCommunicator, self).__init__()

        self._connection = connection
        self._loop = connection.loop

        self._message_subscriber = RmqSubscriber(connection,
                                                 message_exchange=message_exchange,
                                                 queue_expires=queue_expires,
                                                 encoder=encoder,
                                                 decoder=decoder,
                                                 testing_mode=testing_mode)
        self._message_publisher = RmqPublisher(connection,
                                               exchange_name=message_exchange,
                                               encoder=encoder,
                                               decoder=decoder,
                                               testing_mode=testing_mode)
        self._task_subscriber = tasks.RmqTaskSubscriber(connection,
                                                        exchange_name=task_exchange,
                                                        queue_name=task_queue,
                                                        testing_mode=testing_mode,
                                                        decoder=decoder,
                                                        encoder=encoder,
                                                        prefetch_size=task_prefetch_size,
                                                        prefetch_count=task_prefetch_count)
        self._task_publisher = tasks.RmqTaskPublisher(
            connection,
            exchange_name=task_exchange,
            task_queue_name=task_queue,
            encoder=encoder,
            decoder=decoder,
            testing_mode=testing_mode,
        )

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    def __str__(self):
        return "RMQCommunicator ({})".format(self._connection)

    @property
    def loop(self):
        return self._connection.loop

    async def connect(self):
        if self._connection.is_closed:
            await self._connection.connect()

        await self._message_subscriber.connect()
        await self._task_subscriber.connect()
        await self._message_publisher.connect()
        await self._task_publisher.connect()

    async def disconnect(self):
        await self._message_publisher.disconnect()
        await self._message_subscriber.disconnect()
        await self._task_publisher.disconnect()
        await self._task_subscriber.disconnect()
        await self._connection.close()

    async def add_rpc_subscriber(self, subscriber, identifier=None):
        identifier = await self._message_subscriber.add_rpc_subscriber(subscriber, identifier)
        return identifier

    async def remove_rpc_subscriber(self, identifier):
        await self._message_subscriber.remove_rpc_subscriber(identifier)

    async def add_task_subscriber(self, subscriber):
        await self._task_subscriber.add_task_subscriber(subscriber)

    async def remove_task_subscriber(self, subscriber):
        await self._task_subscriber.remove_task_subscriber(subscriber)

    async def add_broadcast_subscriber(self, subscriber, identifier=None):
        identifier = await self._message_subscriber.add_broadcast_subscriber(subscriber, identifier)
        return identifier

    async def remove_broadcast_subscriber(self, identifier):
        await self._message_subscriber.remove_broadcast_subscriber(identifier)

    async def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        """
        try:
            response_future = await self._message_publisher.rpc_send(recipient_id, msg)
            return response_future
        except aio_pika.exceptions.DeliveryError as exception:
            raise kiwipy.UnroutableError(str(exception))

    async def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        result = await self._message_publisher.broadcast_send(body, sender, subject, correlation_id)
        return result

    async def task_send(self, task, no_reply=False):
        try:
            result = await self._task_publisher.task_send(task, no_reply)
            return result
        except aio_pika.exceptions.DeliveryError as exception:
            raise kiwipy.UnroutableError(str(exception))
        except aio_pika.exceptions.AMQPError as exception:
            # Find out what the exception is when a nack is generated!
            raise kiwipy.TaskRejected(str(exception))


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
        communicator = cls(connection,
                           message_exchange=message_exchange,
                           task_exchange=task_exchange,
                           task_queue=task_queue,
                           task_prefetch_size=task_prefetch_size,
                           task_prefetch_count=task_prefetch_count,
                           encoder=encoder,
                           decoder=decoder,
                           testing_mode=testing_mode)

        # Start the communicator
        communicator.start()
        return communicator

    def __init__(self,
                 connection,
                 message_exchange=defaults.MESSAGE_EXCHANGE,
                 task_exchange=defaults.TASK_EXCHANGE,
                 task_queue=defaults.TASK_QUEUE,
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
        self._communicator = RmqCommunicator(connection,
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

    def __enter__(self):
        self._ensure_running()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def loop(self):
        return self._loop

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

        coro = self._communicator.add_rpc_subscriber(self._wrap_subscriber(subscriber), identifier)
        return self._run_task(coro)

    def remove_rpc_subscriber(self, identifier):
        coro = self._communicator.remove_rpc_subscriber(identifier)
        return self._run_task(coro)

    def add_task_subscriber(self, subscriber):
        coro = self._communicator.add_task_subscriber(self._wrap_subscriber(subscriber))
        return self._run_task(coro)

    def remove_task_subscriber(self, subscriber):
        coro = self._communicator.remove_task_subscriber(subscriber)
        return self._run_task(coro)

    def add_broadcast_subscriber(self, subscriber, identifier=None):
        coro = self._communicator.add_broadcast_subscriber(subscriber, identifier)
        return self._run_task(coro)

    def remove_broadcast_subscriber(self, identifier):
        coro = self._communicator.remove_broadcast_subscriber(identifier)
        return self._run_task(coro)

    def task_send(self, task, no_reply=False):
        self._ensure_running()
        future = self._run_task(self._communicator.task_send(task, no_reply))
        if no_reply:
            return None

        return future

    def rpc_send(self, recipient_id, msg):
        self._ensure_running()
        return self._run_task(self._communicator.rpc_send(recipient_id, msg))

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        self._ensure_running()
        coro = self._communicator.broadcast_send(body=body,
                                                 sender=sender,
                                                 subject=subject,
                                                 correlation_id=correlation_id)
        return self._run_task(coro)

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
            testing_mode=False):
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
