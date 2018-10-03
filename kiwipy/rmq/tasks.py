from __future__ import absolute_import
import logging
import uuid
import sys
import traceback

from tornado import gen, concurrent
import topika

import kiwipy
from . import defaults
from . import messages
from . import utils

_LOGGER = logging.getLogger(__name__)

__all__ = ['RmqTaskSubscriber', 'RmqTaskPublisher']


class RmqTaskSubscriber(messages.BaseConnectionWithExchange):
    """
    Listens for tasks coming in on the RMQ task queue
    """

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
        :type connection: :class:`topika.Connection`
        :param exchange_name: the name of the exchange to use
        :type exchange_name: :class:`six.string_types`
        :param queue_name: the name of the task queue to use
        :type queue_name: :class:`six.string_types`
        :param decoder: A message decoder
        :param encoder: A response encoder
        """
        super(RmqTaskSubscriber, self).__init__(
            connection, exchange_name=exchange_name, exchange_params=exchange_params)

        self._task_queue_name = queue_name
        self._testing_mode = testing_mode
        self._decode = decoder
        self._encode = encoder
        self._prefetch_size = prefetch_size
        self._prefetch_count = prefetch_count
        self._consumer_tag = None

        self._task_queue = None  # type: topika.Queue
        self._subscribers = []
        self._pending_tasks = []

    def add_task_subscriber(self, subscriber):
        self._subscribers.append(subscriber)

    def remove_task_subscriber(self, subscriber):
        self._subscribers.remove(subscriber)

    @gen.coroutine
    def connect(self):
        if self.channel():
            # Already connected
            return

        yield super(RmqTaskSubscriber, self).connect()
        yield self.channel().set_qos(prefetch_count=self._prefetch_count, prefetch_size=self._prefetch_size)

        # Set up task queue
        self._task_queue = yield self._channel.declare_queue(
            name=self._task_queue_name,
            durable=not self._testing_mode,
            auto_delete=self._testing_mode,
            arguments={"x-message-ttl": defaults.TASK_MESSAGE_TTL})
        # x-expires means how long does the queue stay alive after no clients
        # x-message-ttl means what is the default ttl for a message arriving in the queue
        yield self._task_queue.bind(self._exchange, routing_key=self._task_queue.name)

        self._consumer_tag = self._task_queue.consume(self._on_task)

    @gen.coroutine
    def _on_task(self, message):
        """
        :param message: The topika RMQ message
        :type message: :class:`topika.IncomingMessage`
        """
        with message.process(ignore_processed=True):
            handled = False
            task = self._decode(message.body)
            for subscriber in self._subscribers:
                try:
                    subscriber = utils.ensure_coroutine(subscriber)
                    result = yield subscriber(self, task)
                    # If a task returns a future it is not considered until the chain of futrues
                    # (i.e. if the first future resolves to a future and so on) finishes and produces
                    # a concrete result
                    while concurrent.is_future(result):
                        yield self._send_response(utils.pending_response(), message)
                        result = yield result
                except kiwipy.TaskRejected:
                    continue
                except kiwipy.CancelledError as exception:
                    reply_body = utils.cancelled_response(str(exception))
                    handled = True  # Finished
                except KeyboardInterrupt:  # pylint: disable=try-except-raise
                    raise
                except Exception as exc:  # pylint: disable=broad-except
                    _LOGGER.debug('There was an exception in task %s:\n%s', exc, traceback.format_exc())
                    reply_body = utils.exception_response(sys.exc_info()[1:])
                    handled = True  # Finished
                else:
                    # Create a reply message
                    reply_body = utils.result_response(result)
                    handled = True  # Finished

                if handled:
                    message.ack()
                    yield self._send_response(reply_body, message)
                    break  # Done, do break the loop

            if not handled:
                # No one handled the task
                message.reject(requeue=True)

    def _build_response_message(self, body, incoming_message):
        """
        Create a topika Message as a response to a task being deal with.

        :param body: The message body dictionary
        :type body: dict
        :param incoming_message: The original message we are responding to
        :type incoming_message: :class:`topika.IncomingMessage`
        :return: The response message
        :rtype: :class:`topika.Message`
        """
        # Add host info
        body[utils.HOST_KEY] = utils.get_host_info()
        message = topika.Message(body=self._encode(body), correlation_id=incoming_message.correlation_id)

        return message

    @gen.coroutine
    def _send_response(self, msg_body, incoming_message):
        msg = self._build_response_message(msg_body, incoming_message)
        yield self._exchange.publish(msg, routing_key=incoming_message.reply_to)


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
        super(RmqTaskPublisher, self).__init__(
            connection,
            exchange_name=exchange_name,
            exchange_params=exchange_params,
            encoder=encoder,
            decoder=decoder,
            confirm_deliveries=confirm_deliveries,
            testing_mode=testing_mode)
        self._task_queue_name = task_queue_name

    @gen.coroutine
    def task_send(self, msg):
        """
        Send a task for processing by a task subscriber
        :param msg: The task payload
        :return: A future representing the result of the task
        :rtype: :class:`tornado.concurrent.Future`
        """
        task_msg = topika.Message(
            body=self._encode(msg), correlation_id=str(uuid.uuid4()), reply_to=self._reply_queue.name)
        published, result_future = yield self.publish_expect_response(
            task_msg, routing_key=self._task_queue_name, mandatory=True)
        assert published, "The task was not published to the exchange"
        raise gen.Return(result_future)
