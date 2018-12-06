from __future__ import absolute_import
from __future__ import print_function
from concurrent.futures import Future as ThreadFuture
from functools import partial
import copy
import logging
import threading

import pika
import pika.exceptions
import shortuuid
from tornado import gen, concurrent, ioloop
import topika

import kiwipy
from . import defaults
from . import tasks
from . import messages
from . import utils

__all__ = ['RmqCommunicator', 'RmqThreadCommunicator', 'connect']

_LOGGER = logging.getLogger(__name__)

# The exchange properties use by the publisher and subscriber.  These have to match
# which is why they're declare her
EXCHANGE_PROPERTIES = {'type': topika.ExchangeType.TOPIC}


class RmqPublisher(messages.BasePublisherWithReplyQueue):
    """
    Publisher for sending a range of message types over RMQ
    """
    DEFAULT_EXCHANGE_PARAMS = EXCHANGE_PROPERTIES

    @gen.coroutine
    def rpc_send(self, recipient_id, msg):
        message = topika.Message(body=self._encode(msg), reply_to=self._reply_queue.name)
        published, response_future = yield self.publish_expect_response(
            message, routing_key="{}.{}".format(defaults.RPC_TOPIC, recipient_id), mandatory=True)
        assert published, "The message was not published to the exchanges"
        raise gen.Return(response_future)

    @gen.coroutine
    def broadcast_send(self, msg, sender=None, subject=None, correlation_id=None):
        message_dict = messages.BroadcastMessage.create(
            body=msg,
            sender=sender,
            subject=subject,
            correlation_id=correlation_id,
        )
        message = topika.Message(
            body=self._encode(message_dict),
            delivery_mode=topika.DeliveryMode.NOT_PERSISTENT,
        )
        try:
            result = yield self.publish(message, routing_key=defaults.BROADCAST_TOPIC)
        except pika.exceptions.UnroutableError as exception:
            _LOGGER.debug('Broadcast message sent but was unroutable, probably no queues: %s', str(exception))
            # Doesn't matter because broadcasts are meant to be fire-and-forget anyway
            result = True
        raise gen.Return(result)


class RmqSubscriber(object):
    """
    Subscriber for receiving a range of messages over RMQ
    """

    BROADCAST_QUEUE_ARGUMENTS = {'x-message-ttl': defaults.MESSAGE_TTL, 'x-expires': defaults.QUEUE_EXPIRES}

    RPC_QUEUE_ARGUMENTS = {'x-message-ttl': defaults.MESSAGE_TTL, 'x-expires': defaults.QUEUE_EXPIRES}

    def __init__(self,
                 connection,
                 message_exchange=defaults.MESSAGE_EXCHANGE,
                 decoder=defaults.DECODER,
                 encoder=defaults.ENCODER,
                 testing_mode=False):
        """
        Subscribes and listens for process control messages and acts on them
        by calling the corresponding methods of the process manager.

        :param connection: The tokpia connection
        :type connection: :class:`topika.Connection`
        :param message_exchange: The name of the exchange to use
        :param decoder:
        """
        super(RmqSubscriber, self).__init__()

        self._connection = connection
        self._channel = None  # type: topika.Channel
        self._exchange = None  # type: topika.Exchange
        self._exchange_name = message_exchange
        self._decode = decoder
        self._testing_mode = testing_mode
        self._response_encode = encoder

        self._rpc_subscribers = {}
        self._broadcast_subscribers = {}
        self._broadcast_queue = None  # type: topika.Queue
        self._broadcast_consumer_tag = None

        self._active = False

    @gen.coroutine
    def add_rpc_subscriber(self, subscriber, identifier=None):
        # Create an RPC queue
        rpc_queue = yield self._channel.declare_queue(exclusive=True, arguments=self.RPC_QUEUE_ARGUMENTS)
        try:
            identifier = yield rpc_queue.consume(partial(self._on_rpc, subscriber), consumer_tag=identifier)
        except pika.exceptions.DuplicateConsumerTag:
            raise kiwipy.DuplicateSubscriberIdentifier("RPC identifier '{}'".format(identifier))
        else:
            yield rpc_queue.bind(self._exchange, routing_key='{}.{}'.format(defaults.RPC_TOPIC, identifier))
            # Save the queue so we can cancel and unbind later
            self._rpc_subscribers[identifier] = rpc_queue
            raise gen.Return(identifier)

    @gen.coroutine
    def remove_rpc_subscriber(self, identifier):
        try:
            rpc_queue = self._rpc_subscribers.pop(identifier)
        except KeyError:
            raise ValueError("Unknown subscriber '{}'".format(identifier))
        else:
            yield rpc_queue.cancel(identifier)
            yield rpc_queue.unbind(self._exchange, routing_key='{}.{}'.format(defaults.RPC_TOPIC, identifier))

    @gen.coroutine
    def add_broadcast_subscriber(self, subscriber, identifier=None):
        identifier = identifier or shortuuid.uuid()
        if identifier in self._broadcast_subscribers:
            raise kiwipy.DuplicateSubscriberIdentifier("Broadcast identifier '{}'".format(identifier))

        self._broadcast_subscribers[identifier] = subscriber
        if self._broadcast_consumer_tag is None:
            # Consume on the broadcast queue
            self._broadcast_consumer_tag = yield self._broadcast_queue.consume(self._on_broadcast)
        raise gen.Return(identifier)

    @gen.coroutine
    def remove_broadcast_subscriber(self, identifier):
        try:
            del self._broadcast_subscribers[identifier]
        except KeyError:
            raise ValueError("Broadcast subscriber '{}' unknown".format(identifier))
        if not self._broadcast_subscribers:
            yield self._broadcast_queue.cancel(self._broadcast_consumer_tag)
            self._broadcast_consumer_tag = None

    def channel(self):
        return self._channel

    @gen.coroutine
    def connect(self):
        """Get a channel and set up all the exchanges/queues we need"""
        if self._channel:
            # Already connected
            return

        exchange_params = copy.copy(EXCHANGE_PROPERTIES)

        if self._testing_mode:
            exchange_params.setdefault('auto_delete', self._testing_mode)

        self._channel = yield self._connection.channel()
        self._exchange = yield self._channel.declare_exchange(name=self._exchange_name, **exchange_params)

        yield self._create_broadcast_queue()

    @gen.coroutine
    def _create_broadcast_queue(self):
        """
        Create and bind the broadcast queue

        One is used for all broadcasts on this exchange
        """
        self._broadcast_queue = yield self._channel.declare_queue(
            exclusive=True, arguments=self.BROADCAST_QUEUE_ARGUMENTS)
        yield self._broadcast_queue.bind(self._exchange, routing_key=defaults.BROADCAST_TOPIC)

    @gen.coroutine
    def disconnect(self):
        yield self._channel.close()
        self._exchange = None
        self._channel = None

    @gen.coroutine
    def _on_rpc(self, subscriber, message):
        """
        :param subscriber: the subscriber function or coroutine that will get the RPC message
        :param message: the RMQ message
        :type message: :class:`topika.message.IncomingMessage`
        """
        with message.process(ignore_processed=True):
            # Tell the sender that we've dealt with it
            message.ack()
            msg = self._decode(message.body)

            try:
                receiver = utils.ensure_coroutine(subscriber)
                result = yield receiver(self, msg)
            except Exception as exception:  # pylint: disable=broad-except
                # We had an exception in  calling the receiver
                yield self._send_response(message.reply_to, message.correlation_id, utils.exception_response(exception))
            else:
                if concurrent.is_future(result):
                    yield self._send_future_response(result, message.reply_to, message.correlation_id)
                else:
                    # All good, send the response out
                    yield self._send_response(message.reply_to, message.correlation_id, utils.result_response(result))

    @gen.coroutine
    def _on_broadcast(self, message):
        with message.process():
            msg = self._decode(message.body)
            for receiver in self._broadcast_subscribers.values():
                try:
                    receiver = utils.ensure_coroutine(receiver)
                    yield receiver(self, msg[messages.BroadcastMessage.BODY], msg[messages.BroadcastMessage.SENDER],
                                   msg[messages.BroadcastMessage.SUBJECT],
                                   msg[messages.BroadcastMessage.CORRELATION_ID])
                except Exception:  # pylint: disable=broad-except
                    _LOGGER.exception('Exception in broadcast receiver')

    @gen.coroutine
    def _send_future_response(self, future, reply_to, correlation_id):
        """
        The RPC call returned a future which means we need to keep send a pending response
        and send a further message when the future resolves.  If it resolves to another future
        we should send out a further pending response and so on.

        :param future: the future from the RPC call
        :type future: :class:`tornado.concurrent.Future`
        :param reply_to: the recipient
        :param correlation_id: the correlation id
        """
        try:
            # Keep looping in case we're in a situation where a futrue resolves to a future etc.
            while concurrent.is_future(future):
                # Send out a message saying that we're waiting for a future to complete
                yield self._send_response(reply_to, correlation_id, utils.pending_response())
                future = yield future
        except kiwipy.CancelledError as exception:
            # Send out a cancelled response
            yield self._send_response(reply_to, correlation_id, utils.cancelled_response(str(exception)))
        except Exception as exception:  # pylint: disable=broad-except
            # Send out an exception response
            yield self._send_response(reply_to, correlation_id, utils.exception_response(exception))
        else:
            # We have a final result so send that as the response
            yield self._send_response(reply_to, correlation_id, utils.result_response(future))

    @gen.coroutine
    def _send_response(self, reply_to, correlation_id, response):
        assert reply_to, "Must provide an identifier for the recipient"

        message = topika.Message(body=self._response_encode(response), correlation_id=correlation_id)
        result = yield self._exchange.publish(message, routing_key=reply_to)
        raise gen.Return(result)


class RmqCommunicator(object):
    """
    A publisher and subscriber using topika and a tornado event loop
    """

    def __init__(self,
                 connection,
                 message_exchange=defaults.MESSAGE_EXCHANGE,
                 task_exchange=defaults.TASK_EXCHANGE,
                 task_queue=defaults.TASK_QUEUE,
                 encoder=defaults.ENCODER,
                 decoder=defaults.DECODER,
                 task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
                 task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
                 testing_mode=False):
        # pylint: disable=too-many-arguments
        """
        :param connection: The RMQ connector object
        :type connection: :class:`topika.Connection`
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
        super(RmqCommunicator, self).__init__()

        self._connection = connection
        self._loop = connection.loop

        self._message_subscriber = RmqSubscriber(
            connection, message_exchange=message_exchange, encoder=encoder, decoder=decoder, testing_mode=testing_mode)
        self._message_publisher = RmqPublisher(
            connection, exchange_name=message_exchange, encoder=encoder, decoder=decoder, testing_mode=testing_mode)
        self._task_subscriber = tasks.RmqTaskSubscriber(
            connection,
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

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __str__(self):
        return "RMQCommunicator ({})".format(self._connection)

    @property
    def loop(self):
        return self._connection.loop

    @gen.coroutine
    def connect(self):
        if not self._connection.is_open:
            yield self._connection.connect()

        yield self._message_subscriber.connect()
        yield self._task_subscriber.connect()
        yield self._message_publisher.connect()
        yield self._task_publisher.connect()

    @gen.coroutine
    def disconnect(self):
        yield self._message_publisher.disconnect()
        yield self._message_subscriber.disconnect()
        yield self._task_publisher.disconnect()
        yield self._task_subscriber.disconnect()
        yield self._connection.close()

    @gen.coroutine
    def add_rpc_subscriber(self, subscriber, identifier=None):
        identifier = yield self._message_subscriber.add_rpc_subscriber(subscriber, identifier)
        raise gen.Return(identifier)

    @gen.coroutine
    def remove_rpc_subscriber(self, identifier):
        yield self._message_subscriber.remove_rpc_subscriber(identifier)

    def add_task_subscriber(self, subscriber):
        self._task_subscriber.add_task_subscriber(subscriber)

    def remove_task_subscriber(self, subscriber):
        self._task_subscriber.remove_task_subscriber(subscriber)

    @gen.coroutine
    def add_broadcast_subscriber(self, subscriber, identifier=None):
        identifier = yield self._message_subscriber.add_broadcast_subscriber(subscriber, identifier)
        raise gen.Return(identifier)

    @gen.coroutine
    def remove_broadcast_subscriber(self, identifier):
        yield self._message_subscriber.remove_broadcast_subscriber(identifier)

    @gen.coroutine
    def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        """
        try:
            response_future = yield self._message_publisher.rpc_send(recipient_id, msg)
            raise gen.Return(response_future)
        except pika.exceptions.UnroutableError as exception:
            raise kiwipy.UnroutableError(str(exception))

    @gen.coroutine
    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        result = yield self._message_publisher.broadcast_send(body, sender, subject, correlation_id)
        raise gen.Return(result)

    @gen.coroutine
    def task_send(self, task, no_reply=False):
        try:
            result = yield self._task_publisher.task_send(task, no_reply)
            raise gen.Return(result)
        except pika.exceptions.UnroutableError as exception:
            raise kiwipy.UnroutableError(str(exception))
        except pika.exceptions.NackError as exception:
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
                connection_factory=topika.connect_robust,
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
        loop = loop or ioloop.IOLoop()
        connection_params['loop'] = loop

        # Run the loop to create the connection
        connection = loop.run_sync(lambda: connection_factory(**connection_params))
        communicator = cls(
            connection,
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
        :type connection: :class:`topika.Connection`
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
        self._communicator = RmqCommunicator(
            connection,
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            encoder=encoder,
            decoder=decoder,
            task_prefetch_size=task_prefetch_size,
            task_prefetch_count=task_prefetch_count,
            testing_mode=testing_mode)
        self._loop = self._communicator.loop
        self._communicator_thread = None

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

        def run_loop():
            _LOGGER.debug('Starting event loop on %s', threading.current_thread())

            @gen.coroutine
            def do_connect():
                try:
                    yield self._communicator.connect()
                    start_future.set_result(True)
                except Exception as exception:  # pylint: disable=broad-except
                    import traceback
                    _LOGGER.error("Error starting the communicator:\n%s", traceback.format_exc())
                    start_future.set_exception(exception)

            self._loop.add_callback(do_connect)
            self._loop.start()
            self._communicator_thread = None

            _LOGGER.debug('Event loop stopped on %s', threading.current_thread())
            ioloop.IOLoop.current(False)

        self._communicator_thread = threading.Thread(
            target=run_loop, name="Communications thread for '{}'".format(self._communicator))
        self._communicator_thread.daemon = True
        self._communicator_thread.start()
        start_future.result()

    def stop(self):
        comm_thread = self._communicator_thread
        if comm_thread is None:
            return

        stop_future = ThreadFuture()

        @gen.coroutine
        def stop_loop():
            try:
                with kiwipy.capture_exceptions(stop_future):
                    yield self._communicator.disconnect()
            finally:
                self._loop.stop()
                stop_future.set_result(True)

        # The stop will end up setting self._communicator_thread to None
        self._loop.add_callback(stop_loop)
        comm_thread.join()
        stop_future.result()

    def add_rpc_subscriber(self, subscriber, identifier=None):
        coro = partial(self._communicator.add_rpc_subscriber, subscriber, identifier)
        return self._run_task(coro)

    def remove_rpc_subscriber(self, identifier):
        coro = partial(self._communicator.remove_rpc_subscriber, identifier)
        return self._run_task(coro)

    def add_task_subscriber(self, subscriber):
        self._communicator.add_task_subscriber(subscriber)

    def remove_task_subscriber(self, subscriber):
        self._communicator.remove_task_subscriber(subscriber)

    def add_broadcast_subscriber(self, subscriber, identifier=None):
        coro = partial(self._communicator.add_broadcast_subscriber, subscriber, identifier)
        return self._run_task(coro)

    def remove_broadcast_subscriber(self, identifier):
        coro = partial(self._communicator.remove_broadcast_subscriber, identifier)
        return self._run_task(coro)

    def task_send(self, task, no_reply=False):
        self._ensure_running()
        future = self._send_message(partial(self._communicator.task_send, task, no_reply))
        if no_reply:
            return None

        return self.tornado_to_kiwi_future(future)

    def rpc_send(self, recipient_id, msg):
        self._ensure_running()
        future = self._send_message(partial(self._communicator.rpc_send, recipient_id, msg))
        return self.tornado_to_kiwi_future(future)

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        self._ensure_running()
        return self._send_message(
            partial(
                self._communicator.broadcast_send,
                body=body,
                sender=sender,
                subject=subject,
                correlation_id=correlation_id))

    def _send_message(self, coro):
        send_future = kiwipy.Future()

        @gen.coroutine
        def do_task():
            with kiwipy.capture_exceptions(send_future):
                send_future.set_result((yield coro()))

        self._loop.add_callback(do_task)
        return send_future.result()

    def tornado_to_kiwi_future(self, tornado_future):
        """
        Convert a tornado future to a kiwipy future

        :param tornado_future: the tornado future to convert
        :type tornado_future: :class:`tornado.concurrent.Future`
        :return: the kiwipy future
        :rtype: :class:`kiwipy.Future`
        """
        kiwi_future = kiwipy.Future()

        def done(done_future):
            # Copy over the future
            try:
                result = done_future.result()
                if concurrent.is_future(result):
                    # Change the future type to a kiwi one
                    result = self.tornado_to_kiwi_future(result)
                kiwi_future.set_result(result)
            except kiwipy.CancelledError:
                kiwi_future.cancel()
            except Exception as exception:  # pylint: disable=broad-except
                kiwi_future.set_exception(exception)

        self.loop().add_future(tornado_future, done)
        return kiwi_future

    def _create_task(self, coro):
        """
        Create a task to be run on our event loop

        :param coro: the coroutine to run
        :return: a future corresponding to the execution of the coroutine
        :rtype: :class:`tornado.concurrent.Future`
        """
        return utils.create_task(coro, self._loop)

    def _run_task(self, coro):
        """
        Run a coroutine on the event loop and return the result.  It may take a little time for the loop
        to get around to scheduling it so we use a timeout as set by the TASK_TIMEOUT class constant.

        :param coro: the coroutine to run
        :return: the result of running the coroutine
        """
        return self.tornado_to_kiwi_future(self._create_task(coro)).result(timeout=self.TASK_TIMEOUT)

    def _ensure_running(self):
        if self._communicator_thread is not None:
            return
        self.start()


def connect(connection_params=None,
            connection_factory=topika.connect_robust,
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
    return RmqThreadCommunicator.connect(
        connection_params=connection_params,
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
