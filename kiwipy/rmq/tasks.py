from __future__ import absolute_import
import collections
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
        :type connection: :class:`topika.Connection`
        :param exchange_name: the name of the exchange to use
        :type exchange_name: :class:`six.string_types`
        :param queue_name: the name of the task queue to use
        :type queue_name: :class:`six.string_types`
        :param decoder: A message decoder
        :param encoder: A response encoder
        """
        super(RmqTaskSubscriber, self).__init__(
            connection, exchange_name=exchange_name, exchange_params=exchange_params, testing_mode=testing_mode)

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
        if self._consumer_tag is None:
            self._consumer_tag = self._task_queue.consume(self._on_task)

    def remove_task_subscriber(self, subscriber):
        self._subscribers.remove(subscriber)
        if not self._subscribers:
            self._task_queue.cancel(self._consumer_tag)
            self._consumer_tag = None

    @gen.coroutine
    def connect(self):
        if self.channel():
            # Already connected
            return

        yield super(RmqTaskSubscriber, self).connect()
        yield self.channel().set_qos(prefetch_count=self._prefetch_count, prefetch_size=self._prefetch_size)

        yield self._create_task_queue()

    @gen.coroutine
    def _create_task_queue(self):
        """Create and bind the task queue"""
        arguments = dict(self.TASK_QUEUE_ARGUMENTS)
        if self._testing_mode:
            arguments['x-expires'] = defaults.TEST_QUEUE_EXPIRES

        # x-expires means how long does the queue stay alive after no clients
        # x-message-ttl means what is the default ttl for a message arriving in the queue
        self._task_queue = yield self._channel.declare_queue(
            name=self._task_queue_name,
            durable=not self._testing_mode,
            auto_delete=self._testing_mode,
            arguments=arguments)
        yield self._task_queue.bind(self._exchange, routing_key=self._task_queue.name)

    @gen.coroutine
    def _on_task(self, message):
        """
        :param message: The topika RMQ message
        :type message: :class:`topika.IncomingMessage`
        """
        # pylint: disable=too-many-branches

        with message.process(ignore_processed=True):
            handled = False
            # Decode the message tuple into a task body for easier use
            task_body = TaskBody(*self._decode(message.body))  # type: TaskBody
            for subscriber in self._subscribers:
                try:
                    subscriber = utils.ensure_coroutine(subscriber)
                    result = yield subscriber(self, task_body.task)

                    # If a task returns a future it is not considered done until the chain of futures
                    # (i.e. if the first future resolves to a future and so on) finishes and produces
                    # a concrete result
                    while concurrent.is_future(result):
                        if not task_body.no_reply:
                            yield self._send_response(utils.pending_response(), message)
                        result = yield result
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
                        reply_body = utils.exception_response(sys.exc_info()[1:])
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
    def task_send(self, task, no_reply=False):
        """
        Send a task for processing by a task subscriber
        :param task: The task payload
        :param no_reply: Don't send a reply containing the result of the task
        :type no_reply: bool
        :return: A future representing the result of the task
        :rtype: :class:`tornado.concurrent.Future`
        """
        # Build the full message body and encode as a tuple
        body = self._encode((task, no_reply))
        # Now build up the full topika message
        task_msg = topika.Message(body=body, correlation_id=str(uuid.uuid4()), reply_to=self._reply_queue.name)

        result_future = None
        if no_reply:
            published = yield self.publish(task_msg, routing_key=self._task_queue_name, mandatory=True)
        else:
            published, result_future = yield self.publish_expect_response(
                task_msg, routing_key=self._task_queue_name, mandatory=True)

        assert published, "The task was not published to the exchange"
        raise gen.Return(result_future)
