import abc
from collections import deque
import collections
import functools
from future.utils import with_metaclass
import inspect
import kiwi
import pika
import uuid
import yaml

from . import pubsub
from . import defaults
from . import utils


class Publisher(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def publish_msg(self, body, routing_key, correlation_id):
        pass

    @abc.abstractmethod
    def await_response(self, correlation_id, callback):
        pass


class Message(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def send(self, publisher):
        """
        :return:
        """
        pass

    @abc.abstractmethod
    def on_delivered(self, publisher):
        pass

    @abc.abstractmethod
    def on_delivery_failed(self, publisher, reason):
        """
        The delivery of the message failed
        :param reason: Text containing the error
        :type reason: str
        """
        pass


class RpcMessage(Message):
    """
    A Remote Procedure Call message that waits for a response from the recipient.
    """

    def __init__(self, recipient_id, body):
        self.recipient_id = recipient_id
        self.body = body
        self.correlation_id = str(uuid.uuid4())
        self.future = kiwi.Future()
        self._publisher = None

    def send(self, publisher):
        self._publisher = publisher
        routing_key = "rpc.{}".format(self.recipient_id)
        publisher.publish_msg(self.body, routing_key, self.correlation_id)
        return self.future

    def on_delivered(self, publisher):
        self._publisher.await_response(self.correlation_id, self.on_response)

    def on_delivery_failed(self, publisher, reason):
        self.future.set_exception(
            kiwi.DeliveryFailed("Message could not be delivered ({})".format(reason)))

    def on_response(self, done_future):
        kiwi.copy_future(done_future, self.future)


def initialiser():
    def wrapper(wrapped):
        @functools.wraps(wrapped)
        def init_fn(self, *a, **kw):
            if not self._in_init_method:
                self._in_init_method = True
                try:
                    wrapped(self, *a, **kw)
                except Exception:
                    import sys
                    self._initialising.set_exc_info(sys.exc_info())
                else:
                    self._initialised(wrapped)
                finally:
                    self._in_init_method = False
            else:
                wrapped(self, *a, **kw)

        init_fn.__is_initailiser = True
        return init_fn

    return wrapper


def get_num_initialisers(rmq_instance):
    def is_initialiser(fn):
        try:
            return inspect.ismethod(fn) and fn.__is_initailiser
        except AttributeError:
            return False

    return len(inspect.getmembers(rmq_instance, predicate=is_initialiser))


class BaseConnectionWithExchange(pubsub.ConnectionListener):
    """
    An RMQ connection with an exchange
    """
    DEFAULT_EXCHANGE_PARAMS = {
        'exchange_type': 'topic',
        'auto_delete': True
    }

    def __init__(self, connector,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None):
        if exchange_params is None:
            exchange_params = self.DEFAULT_EXCHANGE_PARAMS

        self._connector = connector
        self._exchange_name = exchange_name
        self._exchange_params = exchange_params

        self._in_init_method = False
        self._num_initialisers = get_num_initialisers(self)

        self._reset_channel()
        connector.add_connection_listener(self)
        if connector.is_connected:
            connector.open_channel(self.on_channel_open)

    def initialised_future(self):
        return self._initialising

    def on_connection_opened(self, connector, connection):
        connector.open_channel(self.on_channel_open)

    def get_exchange_name(self):
        return self._exchange_name

    def get_channel(self):
        return self._channel

    def close(self):
        self._connector.remove_connection_listener(self)
        if self._channel is not None:
            self._connector.close_channel(self.get_channel())
        self._connector = None
        self._channel = None
        self._initialised_future = None

    # region RMQ communications
    def _reset_channel(self):
        """ Reset all channel specific members """
        self._channel = None
        self._num_initialised = 0
        self._initialisation_state = 0
        self._initialising = kiwi.Future()

    @initialiser()
    def on_channel_open(self, channel):
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_close)
        channel.exchange_declare(
            self.on_exchange_declareok, exchange=self.get_exchange_name(),
            **self._exchange_params)

    def _on_channel_close(self, channel, reply_code, reply_text):
        self._reset_channel()

    @initialiser()
    def on_exchange_declareok(self, unused_frame):
        pass

    def _initialised(self, fn):
        self._num_initialised += 1
        if self._num_initialised == self._num_initialisers:
            self._initialising.set_result(True)


class BasePublisherWithReplyQueue(pubsub.ConnectionListener, Publisher):
    """

    """
    DEFAULT_EXCHANGE_PARAMS = {
        'exchange_type': 'topic',
        'auto_delete': True
    }

    def __init__(self, connector,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 encoder=yaml.dump,
                 decoder=yaml.load,
                 confirm_deliveries=True):
        if exchange_params is None:
            exchange_params = self.DEFAULT_EXCHANGE_PARAMS

        self._exchange_name = exchange_name
        self._exchange_params = exchange_params
        self._encode = encoder
        self._response_decode = decoder
        self._confirm_deliveries = confirm_deliveries

        self._in_init_method = False
        self._num_initialisers = get_num_initialisers(self)

        self._queued_messages = []
        self._awaiting_response = {}
        self._returned_messages = set()

        self._reset_channel()
        self._connector = connector
        connector.add_connection_listener(self)
        if connector.is_connected:
            connector.open_channel(self.on_channel_open)

    def initialised_future(self):
        return self._initialising

    def action_message(self, message):
        """
        Execute an action that involves communication.

        :param message: The message to execute
        :return: A future corresponding to action
        :rtype: :class:`kiwi.Future`
        """
        if self._initialising.done():
            self._send_message(message)
        else:
            self._queued_messages.append(message)
        return message.future

    def await_response(self, correlation_id, callback):
        self._awaiting_response[correlation_id] = callback

    def publish_msg(self, msg, routing_key, correlation_id):
        self._channel.basic_publish(
            exchange=self._exchange_name, routing_key=routing_key,
            properties=pika.BasicProperties(
                reply_to=self._reply_queue_name, correlation_id=correlation_id,
                delivery_mode=1,
                content_type='text/json',
                # expiration="600"
            ),
            body=self._encode(msg),
            mandatory=True
        )

    def on_connection_opened(self, connector, connection):
        connector.open_channel(self.on_channel_open)

    def close(self):
        self._connector.remove_connection_listener(self)
        if self.get_channel() is not None:
            self._connector.close_channel(self.get_channel())
        self._channel = None
        self._connector = None

    def get_reply_queue_name(self):
        return self._reply_queue_name

    def get_exchange_name(self):
        return self._exchange_name

    def get_channel(self):
        return self._channel

    def _on_response(self, ch, method, props, body):
        """ Called when we get a message on our response queue """
        correlation_id = props.correlation_id
        try:
            callback = self._awaiting_response[correlation_id]
        except KeyError:
            # TODO: Log
            pass
        else:
            response = self._response_decode(body)
            response_future = kiwi.Future()
            utils.response_to_future(response, response_future)
            if response_future.done():
                self._awaiting_response.pop(correlation_id)
                callback(response_future)
            else:
                pass  # Keep waiting

    def _send_queued_messages(self):
        for msg in self._queued_messages:
            self._send_message(msg)
        self._queued_messages = []

    def _send_message(self, message):
        message.send(self)
        self._num_published += 1
        self._sent_messages.append((self._num_published, message))

    # region RMQ communications
    def _reset_channel(self):
        """ Reset all channel specific members """
        self._channel = None
        self._reply_queue_name = None
        self._num_initialised = 0
        self._num_published = 0
        if self._confirm_deliveries:
            self._sent_messages = deque()
        self._initialising = kiwi.Future()
        self._initialising.add_done_callback(lambda x: self._send_queued_messages())

    @initialiser()
    def on_channel_open(self, channel):
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_close)
        channel.add_on_return_callback(self._on_channel_return)
        if self._confirm_deliveries:
            channel.confirm_delivery(self._on_delivery_confirmed)
        channel.exchange_declare(
            self.on_exchange_declareok, exchange=self.get_exchange_name(),
            **self._exchange_params)

        # Declare the response queue
        channel.queue_declare(self._on_queue_declareok, exclusive=True, auto_delete=True)

    def _on_channel_close(self, channel, reply_code, reply_text):
        self._reset_channel()

    @initialiser()
    def on_exchange_declareok(self, frame):
        pass

    @initialiser()
    def _on_queue_declareok(self, frame):
        self._reply_queue_name = frame.method.queue
        self._channel.basic_consume(
            self._on_response, no_ack=True, queue=self._reply_queue_name)

    def _on_channel_return(self, channel, method, props, body):
        self._returned_messages.add(props.correlation_id)
        # After this we will get a delivery ack so there we deal with the failure

    def _on_delivery_confirmed(self, frame):
        # All messages up and and including this tag have been confirmed
        delivery_tag = frame.method.delivery_tag

        while self._sent_messages and self._sent_messages[0][0] <= delivery_tag:
            tag, message = self._sent_messages.popleft()
            try:
                self._returned_messages.remove(message.correlation_id)
                message.on_delivery_failed(self, "Channel returned the message")
            except KeyError:
                message.on_delivered(self)


                # endregion

    def _initialised(self, fn):
        self._num_initialised += 1
        if self._num_initialised == self._num_initialisers:
            self._initialising.set_result(True)
