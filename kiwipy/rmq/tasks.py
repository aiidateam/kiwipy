import asyncio
import collections
import logging
import uuid
import sys
import traceback
import typing

import aio_pika

import kiwipy
from . import defaults
from . import messages
from . import utils

_LOGGER = logging.getLogger(__name__)

__all__ = ['RmqTaskSubscriber', 'RmqTaskPublisher']

TaskBody = collections.namedtuple('TaskBody', ('task', 'no_reply'))


class RmqTaskSubscriber(messages.BaseConnectionWithExchange):
    """
    Listens for tasks coming in on the RMQ task queue
    """
    TASK_QUEUE_ARGUMENTS = {"x-message-ttl": defaults.TASK_MESSAGE_TTL}

    def __init__(self,
                 connection,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 queue_name=defaults.TASK_QUEUE,
                 testing_mode=False,
                 decoder=defaults.DECODER,
                 encoder=defaults.ENCODER,
                 exchange_params=None,
                 prefetch_size=defaults.TASK_PREFETCH_SIZE,
                 prefetch_count=defaults.TASK_PREFETCH_COUNT):
        # pylint: disable=too-many-arguments
        """
        :param connection: An RMQ connection
        :type connection: :class:`aio_pika.Connection`
        :param exchange_name: the name of the exchange to use
        :type exchange_name: :class:`str`
        :param queue_name: the name of the task queue to use
        :type queue_name: :class:`str`
        :param decoder: A message decoder
        :param encoder: A response encoder
        """
        super().__init__(connection,
                         exchange_name=exchange_name,
                         exchange_params=exchange_params,
                         testing_mode=testing_mode)

        self._task_queue_name = queue_name
        self._testing_mode = testing_mode
        self._decode = decoder
        self._encode = encoder
        self._prefetch_size = prefetch_size
        self._prefetch_count = prefetch_count
        self._consumer_tag = None

        self._task_queue = None  # type: typing.Optional[aio_pika.Queue]
        self._subscribers = []
        self._pending_tasks = []

    async def add_task_subscriber(self, subscriber):
        self._subscribers.append(subscriber)
        if self._consumer_tag is None:
            self._consumer_tag = await self._task_queue.consume(self._on_task)

    async def remove_task_subscriber(self, subscriber):
        self._subscribers.remove(subscriber)
        if not self._subscribers:
            await self._task_queue.cancel(self._consumer_tag)
            self._consumer_tag = None

    async def connect(self):
        if self.channel():
            # Already connected
            return

        await super().connect()
        await self.channel().set_qos(prefetch_count=self._prefetch_count, prefetch_size=self._prefetch_size)

        await self._create_task_queue()

    async def _create_task_queue(self):
        """Create and bind the task queue"""
        arguments = dict(self.TASK_QUEUE_ARGUMENTS)
        if self._testing_mode:
            arguments['x-expires'] = defaults.TEST_QUEUE_EXPIRES

        # x-expires means how long does the queue stay alive after no clients
        # x-message-ttl means what is the default ttl for a message arriving in the queue
        self._task_queue = await self._channel.declare_queue(name=self._task_queue_name,
                                                             durable=not self._testing_mode,
                                                             auto_delete=self._testing_mode,
                                                             arguments=arguments)
        await self._task_queue.bind(self._exchange, routing_key=self._task_queue.name)

    async def _on_task(self, message):
        """
        :param message: The aio_pika RMQ message
        :type message: :class:`aio_pika.IncomingMessage`
        """
        # pylint: disable=too-many-branches

        with message.process(ignore_processed=True):
            handled = False
            # Decode the message tuple into a task body for easier use
            task_body = TaskBody(*self._decode(message.body))  # type: TaskBody
            for subscriber in self._subscribers:
                try:
                    subscriber = utils.ensure_coroutine(subscriber)
                    result = await subscriber(self, task_body.task)

                    # If a task returns a future it is not considered done until the chain of futures
                    # (i.e. if the first future resolves to a future and so on) finishes and produces
                    # a concrete result
                    while asyncio.isfuture(result):
                        if not task_body.no_reply:
                            await self._send_response(utils.pending_response(), message)
                        result = await result
                except kiwipy.TaskRejected:
                    # Keep trying to find one that will accept the task
                    continue
                except kiwipy.CancelledError as exception:
                    if not task_body.no_reply:
                        reply_body = utils.cancelled_response(str(exception))
                    handled = True  # Finished
                except KeyboardInterrupt:  # pylint: disable=try-except-raise
                    raise
                except Exception as exc:  # pylint: disable=broad-except
                    if task_body.no_reply:
                        # The user has asked for no reply so log an error so they see this exception
                        _LOGGER.exception('The task excepted')
                    else:
                        # Send the exception back to the other side but log here at INFO level also
                        reply_body = utils.exception_response(*sys.exc_info()[1:])
                        _LOGGER.info('There was an exception in a task %s:\n%s', exc, traceback.format_exc())
                    handled = True  # Finished
                else:
                    # Create a reply message
                    if not task_body.no_reply:
                        reply_body = utils.result_response(result)
                    handled = True  # Finished

                if handled:
                    message.ack()
                    if not task_body.no_reply:
                        await self._send_response(reply_body, message)
                    break  # Done, do break the loop

            if not handled:
                # No one handled the task
                message.reject(requeue=True)

    def _build_response_message(self, body, incoming_message):
        """
        Create a aio_pika Message as a response to a task being deal with.

        :param body: The message body dictionary
        :type body: dict
        :param incoming_message: The original message we are responding to
        :type incoming_message: :class:`aio_pika.IncomingMessage`
        :return: The response message
        :rtype: :class:`aio_pika.Message`
        """
        # Add host info
        body[utils.HOST_KEY] = utils.get_host_info()
        message = aio_pika.Message(body=self._encode(body), correlation_id=incoming_message.correlation_id)

        return message

    async def _send_response(self, msg_body, incoming_message):
        msg = self._build_response_message(msg_body, incoming_message)
        await self._exchange.publish(msg, routing_key=incoming_message.reply_to)


class RmqTaskPublisher(messages.BasePublisherWithReplyQueue):
    """
    Publishes messages to the RMQ task queue and gets the response
    """

    def __init__(self,
                 connection,
                 task_queue_name=defaults.TASK_QUEUE,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 encoder=defaults.ENCODER,
                 decoder=defaults.DECODER,
                 confirm_deliveries=True,
                 testing_mode=False):
        # pylint: disable=too-many-arguments
        super().__init__(connection,
                         exchange_name=exchange_name,
                         exchange_params=exchange_params,
                         encoder=encoder,
                         decoder=decoder,
                         confirm_deliveries=confirm_deliveries,
                         testing_mode=testing_mode)
        self._task_queue_name = task_queue_name

    async def task_send(self, task, no_reply=False):
        """Send a task for processing by a task subscriber.

        All task messages will be set to be persistent by setting `delivery_mode=2`.

        :param task: The task payload
        :param no_reply: Don't send a reply containing the result of the task
        :type no_reply: bool
        :return: A future representing the result of the task
        :rtype: :class:`asyncio.Future`
        """
        # Build the full message body and encode as a tuple
        body = self._encode((task, no_reply))
        # Now build up the full aio_pika message
        task_msg = aio_pika.Message(
            body=body,
            correlation_id=str(uuid.uuid4()),
            reply_to=self._reply_queue.name,
            delivery_mode=2  # Task messages need to be persistent
        )

        result_future = None
        if no_reply:
            published = await self.publish(task_msg, routing_key=self._task_queue_name, mandatory=True)
        else:
            published, result_future = await self.publish_expect_response(task_msg,
                                                                          routing_key=self._task_queue_name,
                                                                          mandatory=True)

        assert published, "The task was not published to the exchange"
        return result_future
