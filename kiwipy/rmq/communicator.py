import asyncio
from functools import partial
import copy
import logging
import typing

import shortuuid
import aio_pika

import kiwipy
from . import defaults
from . import tasks
from . import messages
from . import utils

__all__ = ('RmqCommunicator',)

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
        super().__init__()

        self._connection = connection
        self._channel: typing.Optional[aio_pika.Channel] = None
        self._exchange: typing.Optional[aio_pika.Exchange] = None
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
        self._broadcast_queue: typing.Optional[aio_pika.Queue] = None  # type:
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
        super().__init__()

        self._connection = connection
        self._loop = connection.loop

        # Save some of these settings for later
        self._task_exchange = task_exchange
        self._testing_mode = testing_mode
        self._decoder = decoder
        self._encoder = encoder
        self._task_queues = []

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

        self._default_task_queue = tasks.TaskQueue(connection,
                                                   exchange_name=task_exchange,
                                                   queue_name=task_queue,
                                                   decoder=decoder,
                                                   encoder=encoder,
                                                   prefetch_size=task_prefetch_size,
                                                   prefetch_count=task_prefetch_count,
                                                   testing_mode=testing_mode)

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
        await self._message_publisher.connect()
        await self._default_task_queue.connect()

    async def disconnect(self):
        await self._message_publisher.disconnect()
        await self._message_subscriber.disconnect()
        await self._default_task_queue.disconnect()
        await self._connection.close()

    async def add_rpc_subscriber(self, subscriber, identifier=None):
        identifier = await self._message_subscriber.add_rpc_subscriber(subscriber, identifier)
        return identifier

    async def remove_rpc_subscriber(self, identifier):
        await self._message_subscriber.remove_rpc_subscriber(identifier)

    async def add_task_subscriber(self, subscriber):
        await self._default_task_queue.add_task_subscriber(subscriber)

    async def remove_task_subscriber(self, subscriber):
        await self._default_task_queue.remove_task_subscriber(subscriber)

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
            result = await self._default_task_queue.task_send(task, no_reply)
            return result
        except aio_pika.exceptions.DeliveryError as exception:
            raise kiwipy.UnroutableError(str(exception))
        except aio_pika.exceptions.AMQPError as exception:
            # Find out what the exception is when a nack is generated!
            raise kiwipy.TaskRejected(str(exception))

    async def task_queue(self,
                         queue_name: str,
                         prefetch_size=defaults.TASK_PREFETCH_SIZE,
                         prefetch_count=defaults.TASK_PREFETCH_COUNT) -> tasks.TaskQueue:
        """Create a new task queue"""
        queue = tasks.TaskQueue(self._connection,
                                exchange_name=self._task_exchange,
                                queue_name=queue_name,
                                decoder=self._decoder,
                                encoder=self._encoder,
                                prefetch_size=prefetch_size,
                                prefetch_count=prefetch_count,
                                testing_mode=self._testing_mode)
        await queue.connect()
        self._task_queues.append(queue)
        return queue
