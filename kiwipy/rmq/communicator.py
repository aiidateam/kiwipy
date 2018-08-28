from concurrent.futures import Future as ThreadFuture
from functools import partial
import kiwipy
import logging
import pika
import pika.exceptions
import threading
import topika
from tornado import gen, concurrent, ioloop

from . import defaults
from . import tasks
from . import messages
from . import utils

__all__ = ['RmqCommunicator', 'RmqThreadCommunicator', 'connect']

_LOGGER = logging.getLogger(__name__)

# The exchange properties use by the publisher and subscriber.  These have to match
# which is why they're declare her
EXCHANGE_PROPERTIES = {
    'type': topika.ExchangeType.TOPIC
}


class RmqPublisher(messages.BasePublisherWithReplyQueue):
    """
    Publisher for sending a range of message types over RMQ
    """
    DEFAULT_EXCHANGE_PARAMS = EXCHANGE_PROPERTIES

    @gen.coroutine
    def rpc_send(self, recipient_id, msg):
        message = topika.Message(
            body=self._encode(msg),
            reply_to=self._reply_queue.name
        )
        published, response_future = yield self.publish_expect_response(
            message,
            routing_key="{}.{}".format(defaults.RPC_TOPIC, recipient_id),
            mandatory=True)
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

    def __init__(self, connection,
                 message_exchange=defaults.MESSAGE_EXCHANGE,
                 decoder=defaults.decoder,
                 encoder=defaults.encoder):
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
        self._exchange = yield self._channel.declare_exchange(
            name=self._exchange_name, **EXCHANGE_PROPERTIES)

        # RPC queue
        rpc_queue = yield self._channel.declare_queue(
            exclusive=True,
            arguments={
                "x-message-ttl": defaults.MESSAGE_TTL,
                "x-expires": defaults.QUEUE_EXPIRES
            })

        yield rpc_queue.bind(self._exchange, routing_key='{}.*'.format(defaults.RPC_TOPIC))
        rpc_queue.consume(self._on_rpc)

        # Broadcast queue
        broadcast_queue = yield self._channel.declare_queue(
            exclusive=True,
            arguments={
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
                    result = yield receiver(msg)

                    if isinstance(result, concurrent.Future):
                        response = utils.pending_response()
                        yield self._send_response(message.reply_to, message.correlation_id, response)
                        try:
                            response = yield result
                            response = utils.result_response(response)
                        except kiwipy.CancelledError as e:
                            response = utils.cancelled_response()
                        except Exception as e:
                            response = utils.exception_response(e)
                    else:
                        response = utils.result_response(result)
                except Exception as e:
                    response = utils.exception_response(e)

                yield self._send_response(message.reply_to, message.correlation_id, response)

    @gen.coroutine
    def _on_broadcast(self, message):
        with message.process():
            msg = self._decode(message.body)
            for receiver in self._broadcast_subscribers:
                try:
                    receiver = utils.ensure_coroutine(receiver)
                    yield receiver(**msg)
                except BaseException:
                    import traceback
                    _LOGGER.error("Exception in broadcast receiver!\n"
                                  "msg: {}\n"
                                  "traceback:\n{}".format(msg, traceback.format_exc()))

    @gen.coroutine
    def _send_response(self, reply_to, correlation_id, response):
        assert reply_to, "Must provide an identifier for the recipient"

        message = topika.Message(
            body=self._response_encode(response),
            correlation_id=correlation_id
        )
        result = yield self._exchange.publish(message, routing_key=reply_to)
        raise gen.Return(result)


class RmqCommunicator(object):
    """
    A publisher and subscriber that implements the Communicator interface.
    """

    def __init__(self, connection,
                 message_exchange=defaults.MESSAGE_EXCHANGE,
                 task_exchange=defaults.TASK_EXCHANGE,
                 task_queue=defaults.TASK_QUEUE,
                 encoder=defaults.encoder,
                 decoder=defaults.decoder,
                 task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
                 task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
                 testing_mode=False):
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
            connection,
            message_exchange=message_exchange,
            encoder=encoder,
            decoder=decoder
        )
        self._message_publisher = RmqPublisher(
            connection,
            exchange_name=message_exchange,
            encoder=encoder,
            decoder=decoder,
        )
        self._task_subscriber = tasks.RmqTaskSubscriber(
            connection,
            exchange_name=task_exchange,
            task_queue_name=task_queue,
            encoder=encoder,
            decoder=decoder,
            prefetch_size=task_prefetch_size,
            prefetch_count=task_prefetch_count,
            testing_mode=testing_mode,
        )
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
        except pika.exceptions.UnroutableError as e:
            raise kiwipy.UnroutableError(str(e))

    @gen.coroutine
    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        result = yield self._message_publisher.broadcast_send(body, sender, subject, correlation_id)
        raise gen.Return(result)

    @gen.coroutine
    def task_send(self, msg):
        try:
            response_future = yield self._task_publisher.task_send(msg)
            raise gen.Return(response_future)
        except pika.exceptions.UnroutableError as e:
            raise kiwipy.UnroutableError(str(e))
        except pika.exceptions.NackError as e:
            raise kiwipy.TaskRejected(str(e))


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
                encoder=defaults.encoder,
                decoder=defaults.decoder,
                testing_mode=False
                ):
        connection_params = connection_params or {}
        # Create a new loop if one isn't supplied
        loop = loop or ioloop.IOLoop()
        connection_params['loop'] = loop

        # Run the loop to create the connection
        connection = loop.run_sync(lambda: connection_factory(**connection_params))
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
                 encoder=defaults.encoder,
                 decoder=defaults.decoder,
                 testing_mode=False):
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
            testing_mode=testing_mode
        )
        self._loop = self._communicator.loop
        self._communicator_thread = None

    def start(self):
        if self._communicator_thread is not None:
            return

        start_future = ThreadFuture()

        def run_loop():
            @gen.coroutine
            def connect():
                try:
                    yield self._communicator.connect()
                    start_future.set_result(True)
                except Exception as e:
                    import traceback
                    _LOGGER.error("Error starting the communicator:\n%s", traceback.format_exc())
                    start_future.set_exception(e)

            self._loop.add_callback(connect)
            self._loop.start()
            self._communicator_thread = None

        self._communicator_thread = threading.Thread(target=run_loop)
        self._communicator_thread.start()
        return start_future.result()

    def stop(self):
        comm_thread = self._communicator_thread
        if comm_thread is None:
            return

        stop_future = ThreadFuture()

        @gen.coroutine
        def stop_loop():
            try:
                yield self._communicator.disconnect()
                stop_future.set_result(True)
            except Exception as e:
                stop_future.set_exception(e)
            finally:
                self._loop.stop()

        # The stop will end up setting self._communicator_thread to None
        self._loop.add_callback(stop_loop)
        comm_thread.join()
        return stop_future.result()

    def add_rpc_subscriber(self, subscriber, identifier):
        return self._communicator.add_rpc_subscriber(subscriber, identifier)

    def remove_rpc_subscriber(self, identifier):
        return self._communicator.remove_rpc_subscriber(identifier)

    def add_task_subscriber(self, subscriber):
        return self._communicator.add_task_subscriber(subscriber)

    def remove_task_subscriber(self, subscriber):
        return self._communicator.remove_task_subscriber(subscriber)

    def add_broadcast_subscriber(self, subscriber):
        return self._communicator.add_broadcast_subscriber(subscriber)

    def remove_broadcast_subscriber(self, subscriber):
        return self._communicator.remove_broadcast_subscriber(subscriber)

    def task_send(self, msg):
        self.start()
        future = self._send_message(partial(self._communicator.task_send, msg))
        return utils.tornado_to_kiwi_future(future)

    def rpc_send(self, recipient_id, msg):
        self.start()
        future = self._send_message(partial(self._communicator.rpc_send, recipient_id, msg))
        return utils.tornado_to_kiwi_future(future)

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        self.start()
        return self._send_message(partial(self._communicator.broadcast_send,
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
            try:
                result_future = yield coro()
            except Exception as e:
                send_future.set_exception(e)
            else:
                send_future.set_result(result_future)

        self._loop.add_callback(do_task)
        return send_future.result()


def connect(connection_params=None,
            connection_factory=topika.connect_robust,
            loop=None,
            message_exchange=defaults.MESSAGE_EXCHANGE,
            task_exchange=defaults.TASK_EXCHANGE,
            task_queue=defaults.TASK_QUEUE,
            task_prefetch_size=defaults.TASK_PREFETCH_SIZE,
            task_prefetch_count=defaults.TASK_PREFETCH_COUNT,
            encoder=defaults.encoder,
            decoder=defaults.decoder,
            testing_mode=False
            ):
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
        testing_mode=testing_mode
    )
