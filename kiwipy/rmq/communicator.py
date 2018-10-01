from __future__ import absolute_import
from __future__ import print_function
from concurrent.futures import Future as ThreadFuture
from functools import partial
import logging
import threading

import pika
import pika.exceptions
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
        result = yield self.publish(message, routing_key=defaults.BROADCAST_TOPIC)
        raise gen.Return(result)


class RmqSubscriber(object):
    """
    Subscriber for receiving a range of messages over RMQ
    """

    def __init__(self,
                 connection,
                 message_exchange=defaults.MESSAGE_EXCHANGE,
                 decoder=defaults.DECODER,
                 encoder=defaults.ENCODER):
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
        self._channel = None
        self._exchange = None
        self._exchange_name = message_exchange
        self._decode = decoder
        self._response_encode = encoder

        self._rpc_subscribers = {}
        self._broadcast_subscribers = []

        self._active = False

    def add_rpc_subscriber(self, subscriber, identifier):
        self._rpc_subscribers[identifier] = subscriber

    def remove_rpc_subscriber(self, identifier):
        try:
            self._rpc_subscribers.pop(identifier)
        except KeyError:
            raise ValueError("Unknown subscriber '{}'".format(identifier))

    def add_broadcast_subscriber(self, subscriber):
        self._broadcast_subscribers.append(subscriber)

    def remove_broadcast_subscriber(self, subscriber):
        self._broadcast_subscribers.remove(subscriber)

    def channel(self):
        return self._channel

    @gen.coroutine
    def connect(self):
        if self._channel:
            # Already connected
            return

        self._channel = yield self._connection.channel()
        self._exchange = yield self._channel.declare_exchange(name=self._exchange_name, **EXCHANGE_PROPERTIES)

        # RPC queue
        rpc_queue = yield self._channel.declare_queue(
            exclusive=True, arguments={
                "x-message-ttl": defaults.MESSAGE_TTL,
                "x-expires": defaults.QUEUE_EXPIRES
            })

        yield rpc_queue.bind(self._exchange, routing_key='{}.*'.format(defaults.RPC_TOPIC))
        rpc_queue.consume(self._on_rpc)

        # Broadcast queue
        broadcast_queue = yield self._channel.declare_queue(
            exclusive=True, arguments={
                "x-message-ttl": defaults.MESSAGE_TTL,
                "x-expires": defaults.QUEUE_EXPIRES
            })
        yield broadcast_queue.bind(self._exchange, routing_key=defaults.BROADCAST_TOPIC)
        broadcast_queue.consume(self._on_broadcast)

    @gen.coroutine
    def disconnect(self):
        yield self._channel.close()
        self._exchange = None
        self._channel = None

    @gen.coroutine
    def _on_rpc(self, message):
        """
        :param message: The RMQ message
        :type message: :class:`topika.message.IncomingMessage`
        """
        with message.process(ignore_processed=True):
            identifier = message.routing_key[len('{}.'.format(defaults.RPC_TOPIC)):]
            receiver = self._rpc_subscribers.get(identifier, None)
            if receiver is None:
                message.reject(requeue=True)
            else:
                # Tell the sender that we've dealt with it
                message.ack()
                msg = self._decode(message.body)

                try:
                    receiver = utils.ensure_coroutine(receiver)
                    result = yield receiver(self, msg)
                except Exception as exception:  # pylint: disable=broad-except
                    # We had an exception in  calling the receiver
                    yield self._send_response(message.reply_to, message.correlation_id,
                                              utils.exception_response(exception))
                else:
                    if concurrent.is_future(result):
                        yield self._send_future_response(result, message.reply_to, message.correlation_id)
                    else:
                        # All good, send the response out
                        yield self._send_response(message.reply_to, message.correlation_id,
                                                  utils.result_response(result))

    @gen.coroutine
    def _on_broadcast(self, message):
        with message.process():
            msg = self._decode(message.body)
            for receiver in self._broadcast_subscribers:
                try:
                    receiver = utils.ensure_coroutine(receiver)
                    yield receiver(self, msg[messages.BroadcastMessage.BODY], msg[messages.BroadcastMessage.SENDER],
                                   msg[messages.BroadcastMessage.SUBJECT],
                                   msg[messages.BroadcastMessage.CORRELATION_ID])
                except Exception:  # pylint: disable=broad-except
                    import traceback
                    _LOGGER.error("Exception in broadcast receiver!\n"
                                  "msg: %s\n"
                                  "traceback:\n%s", msg, traceback.format_exc())

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
        self._connected = False

        self._message_subscriber = RmqSubscriber(
            connection, message_exchange=message_exchange, encoder=encoder, decoder=decoder)
        self._message_publisher = RmqPublisher(
            connection,
            exchange_name=message_exchange,
            encoder=encoder,
            decoder=decoder,
        )
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
        if self._connected:
            return

        yield self._message_subscriber.connect()
        yield self._task_subscriber.connect()
        yield self._message_publisher.connect()
        yield self._task_publisher.connect()

        self._connected = True

    @gen.coroutine
    def disconnect(self):
        yield self._message_publisher.disconnect()
        yield self._message_subscriber.disconnect()
        yield self._task_publisher.disconnect()
        yield self._task_subscriber.disconnect()
        yield self._connection.close()

    def add_rpc_subscriber(self, subscriber, identifier):
        self._message_subscriber.add_rpc_subscriber(subscriber, identifier)

    def remove_rpc_subscriber(self, identifier):
        self._message_subscriber.remove_rpc_subscriber(identifier)

    def add_task_subscriber(self, subscriber):
        self._task_subscriber.add_task_subscriber(subscriber)

    def remove_task_subscriber(self, subscriber):
        self._task_subscriber.remove_task_subscriber(subscriber)

    def add_broadcast_subscriber(self, subscriber):
        self._message_subscriber.add_broadcast_subscriber(subscriber)

    def remove_broadcast_subscriber(self, subscriber):
        self._message_subscriber.remove_broadcast_subscriber(subscriber)

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
    def task_send(self, msg):
        try:
            response_future = yield self._task_publisher.task_send(msg)
            raise gen.Return(response_future)
        except pika.exceptions.UnroutableError as exception:
            raise kiwipy.UnroutableError(str(exception))
        except pika.exceptions.NackError as exception:
            raise kiwipy.TaskRejected(str(exception))


class RmqThreadCommunicator(kiwipy.Communicator):

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
        self._subscribers = {}

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def loop(self):
        return self._loop

    def start(self):
        if self._communicator_thread is not None:
            return

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
                    self._loop.stop()
            finally:
                stop_future.set_result(True)

        # The stop will end up setting self._communicator_thread to None
        self._loop.add_callback(stop_loop)
        comm_thread.join()
        stop_future.result()

    def add_rpc_subscriber(self, subscriber, identifier):
        return self._communicator.add_rpc_subscriber(self._convert_callback(subscriber), identifier)

    def remove_rpc_subscriber(self, identifier):
        return self._communicator.remove_rpc_subscriber(identifier)

    def add_task_subscriber(self, subscriber):
        converted = self._convert_callback(subscriber)
        self._communicator.add_task_subscriber(converted)
        self._subscribers[subscriber] = converted

    def remove_task_subscriber(self, subscriber):
        try:
            converted = self._subscribers.pop(subscriber)
        except KeyError:
            raise ValueError("Subscriber '{}' is unknown".format(subscriber))
        else:
            self._communicator.remove_task_subscriber(converted)

    def add_broadcast_subscriber(self, subscriber):
        converted = self._convert_callback(subscriber)
        self._communicator.add_broadcast_subscriber(converted)
        self._subscribers[subscriber] = converted

    def remove_broadcast_subscriber(self, subscriber):
        try:
            converted = self._subscribers.pop(subscriber)
        except KeyError:
            raise ValueError("Subscriber '{}' is unknown".format(subscriber))
        else:
            self._communicator.remove_broadcast_subscriber(converted)

    def task_send(self, msg):
        self.start()
        future = self._send_message(partial(self._communicator.task_send, msg))
        return self.tornado_to_kiwi_future(future)

    def rpc_send(self, recipient_id, msg):
        self.start()
        future = self._send_message(partial(self._communicator.rpc_send, recipient_id, msg))
        return self.tornado_to_kiwi_future(future)

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        self.start()
        return self._send_message(
            partial(
                self._communicator.broadcast_send,
                body=body,
                sender=sender,
                subject=subject,
                correlation_id=correlation_id))

    def wait_for(self, future, timeout=None):
        self.start()
        thread_result = ThreadFuture()
        do_copy = partial(kiwipy.copy_future, target=thread_result)
        future.add_done_callback(do_copy)

        try:
            return thread_result.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.remove_done_callback(do_copy)
            raise kiwipy.TimeoutError()

    def _send_message(self, coro):
        send_future = ThreadFuture()

        @gen.coroutine
        def do_task():
            with kiwipy.capture_exceptions(send_future):
                result_future = yield coro()
                send_future.set_result(result_future)

        self._loop.add_callback(do_task)
        return send_future.result()

    def _convert_callback(self, callback):
        """
        :param callback: The callback to convert
        :return: A new function that conforms to that which a Communicator expects
        """

        def converted(_comm, *args, **kwargs):
            result = callback(self, *args, **kwargs)
            if isinstance(result, kiwipy.Future):
                result = self.kiwi_to_tornado_future(result)
            return result

        return converted

    def kiwi_to_tornado_future(self, kiwi_future):
        """
        Convert a kiwipy future to a tornado future

        :param kiwi_future: the kiwipy future to convert
        :type kiwi_future: :class:`kiwipy.Future`
        :return: the tornado future
        :rtype: :class:`tornado.concurrent.Future`
        """
        tornado_future = concurrent.Future()

        def done(done_future):
            if done_future.cancelled():
                tornado_future.cancel()

            with kiwipy.capture_exceptions(tornado_future):
                result = done_future.result()
                if isinstance(result, kiwipy.Future):
                    result = self.kiwi_to_tornado_future(result)

                # Schedule the callback to set the result because this method may be called
                # from outside the event loop in which case any done callbacks would be called
                # using the wrong event loop
                self.loop().add_callback(tornado_future.set_result, result)

        kiwi_future.add_done_callback(done)
        return tornado_future

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
