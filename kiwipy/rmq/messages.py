from __future__ import absolute_import
from collections import deque
import copy
import logging
import traceback
import uuid

from tornado import gen, concurrent
import topika

from . import defaults
from . import utils

_LOGGER = logging.getLogger(__name__)


class BroadcastMessage(object):
    BODY = 'body'
    SENDER = 'sender'
    SUBJECT = 'subject'
    CORRELATION_ID = 'correlation_id'

    @staticmethod
    def create(body, sender=None, subject=None, correlation_id=None):
        message_dict = {
            BroadcastMessage.BODY: body,
            BroadcastMessage.SENDER: sender,
            BroadcastMessage.SUBJECT: subject,
            BroadcastMessage.CORRELATION_ID: correlation_id,
        }
        return message_dict


class BaseConnectionWithExchange(object):
    """
    An RMQ connection with a channel and exchange
    """
    DEFAULT_EXCHANGE_PARAMS = {'type': topika.ExchangeType.TOPIC}

    def __init__(self, connection, exchange_name=defaults.MESSAGE_EXCHANGE, exchange_params=None, testing_mode=False):
        """
        :type connection: :class:`topika.Connection`
        :type exchange_name: str
        :type exchange_params: dict or NoneType
        """
        super(BaseConnectionWithExchange, self).__init__()

        if exchange_params is None:
            exchange_params = copy.copy(self.DEFAULT_EXCHANGE_PARAMS)

        if testing_mode:
            exchange_params.setdefault('auto_delete', testing_mode)

        self._connection = connection
        self._exchange_name = exchange_name
        self._exchange_params = exchange_params
        self._loop = self._connection.loop

        self._channel = None  # type: topika.Channel
        self._exchange = None  # type: topika.Exchange
        self._is_closing = False

    @property
    def is_closing(self):
        return self._is_closing

    def get_exchange_name(self):
        return self._exchange_name

    def channel(self):
        return self._channel

    @gen.coroutine
    def connect(self):
        if self._channel:
            return

        # Create the channel
        self._channel = yield self._connection.channel()
        # Create the exchange
        self._exchange = yield self._channel.declare_exchange(name=self.get_exchange_name(), **self._exchange_params)

    @gen.coroutine
    def disconnect(self):
        if not self.is_closing:
            self._is_closing = True
            if self.channel() is not None:
                yield self.channel().close()
                self._channel = None


class BasePublisherWithReplyQueue(object):
    """
    A base class for any object that needs to be able to publish a message and to potentially expect a reply.
    """
    # pylint: disable=too-many-instance-attributes

    DEFAULT_EXCHANGE_PARAMS = {'type': topika.ExchangeType.TOPIC}

    def __init__(self,
                 connection,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 encoder=defaults.ENCODER,
                 decoder=defaults.DECODER,
                 confirm_deliveries=True,
                 testing_mode=False):
        # pylint: disable=too-many-arguments
        """
        :param connection: The topika RMQ connection
        :type connection: :class:`topika.connection.Connection`
        :param exchange_name:
        :param exchange_params:
        :param encoder:
        :param decoder:
        :param confirm_deliveries:
        """
        super(BasePublisherWithReplyQueue, self).__init__()

        if exchange_params is None:
            exchange_params = copy.copy(self.DEFAULT_EXCHANGE_PARAMS)

        if testing_mode:
            exchange_params.setdefault('auto_delete', testing_mode)

        self._exchange_name = exchange_name
        self._exchange_params = exchange_params
        self._encode = encoder
        self._response_decode = decoder
        self._confirm_deliveries = confirm_deliveries
        if self._confirm_deliveries:
            self._num_published = 0
            self._delivery_info = deque()
        self._testing_mode = testing_mode

        self._awaiting_response = {}

        self._connection = connection
        self._channel = None  # type: topika.Channel
        self._exchange = None  # type: topika.Exchange
        self._reply_queue = None  # type: topika.Queue

        self._is_closing = False

    @property
    def is_closing(self):
        return self._is_closing

    @property
    def is_connected(self):
        return self._channel

    @gen.coroutine
    def connect(self):
        if self.is_connected:
            return

        self._channel = yield self._connection.channel(
            publisher_confirms=self._confirm_deliveries, on_return_raises=True)
        self._channel.add_close_callback(self._on_channel_close)

        self._exchange = yield self._channel.declare_exchange(name=self.get_exchange_name(), **self._exchange_params)

        # Declare the reply queue
        reply_queue_name = "{}-reply-{}".format(self._exchange_name, str(uuid.uuid4()))
        self._reply_queue = yield self._channel.declare_queue(
            name=reply_queue_name,
            exclusive=True,
            auto_delete=self._testing_mode,
            arguments={"x-expires": defaults.REPLY_QUEUE_EXPIRES})

        yield self._reply_queue.bind(self._exchange, routing_key=reply_queue_name)
        yield self._reply_queue.consume(self._on_response, no_ack=True)

    @gen.coroutine
    def disconnect(self):
        if not self.is_closing:
            self._is_closing = True
            if self.channel() is not None and not self.channel().is_closed:
                yield self._channel.close()
            self._channel = None

    def action_message(self, message):
        """
        Execute a message that involves communication.  This could mean that the
        message gets queued first and then sent as soon as the connection is open.
        In any case the method returns a future for the message.

        :param message: The message to execute
        :return: A future corresponding to action
        :rtype: :class:`kiwi.Future`
        """
        message.send(self)
        return message.future

    @gen.coroutine
    def publish(self, message, routing_key, mandatory=True, immediate=False):
        """
        Send a fire-and-forget message i.e. no response expected.

        :param message: The message to send
        :param routing_key: The routing key
        :param mandatory: If the message cannot be routed this will raise an UnroutableException
        :param immediate: Return the message if it cannot be queued immediately
        :return:
        """
        result = yield self._exchange.publish(
            message, routing_key=routing_key, mandatory=mandatory, immediate=immediate)
        raise gen.Return(result)

    @gen.coroutine
    def publish_expect_response(self, message, routing_key, mandatory=True):
        # If there is no correlation id we have to set on so that we know what the response will be to
        if not message.correlation_id:
            message.correlation_id = str(uuid.uuid4()).encode()
        correlation_id = message.correlation_id

        response_future = concurrent.Future()
        self._awaiting_response[correlation_id] = response_future
        result = yield self.publish(message, routing_key=routing_key, mandatory=mandatory)
        raise gen.Return((result, response_future))

    def get_exchange_name(self):
        return self._exchange_name

    def channel(self):
        return self._channel

    # region RMQ communications

    @gen.coroutine
    def _on_response(self, message):
        """
        Called when we get a message on our response queue

        :param message: The response message
        :type message: :class:`topika.message.IncomingMessage`
        """
        correlation_id = message.correlation_id
        try:
            response_future = self._awaiting_response.pop(correlation_id)
        except KeyError:
            _LOGGER.error("Got a response for an unknown id '%s':\n%s", correlation_id, message)
        else:
            try:
                response = self._response_decode(message.body)
            except Exception:
                _LOGGER.error("Failed to decode message body:\n%s%s", message.body, traceback.format_exc())
                raise
            else:
                utils.response_to_future(response, response_future)
                try:
                    # If the response was a future it means we should another message that resolves
                    # that future
                    if concurrent.is_future(response_future.result()):
                        self._awaiting_response[correlation_id] = response_future.result()
                except Exception:  # pylint: disable=broad-except
                    pass

    def _on_channel_close(self, _closing_future):
        """ Reset all channel specific members """
        if self._confirm_deliveries:
            self._num_published = 0
            self._delivery_info = deque()

    # endregion
