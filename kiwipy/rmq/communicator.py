from functools import partial
import kiwipy
import logging
import pika
import pika.exceptions
from tornado.gen import coroutine
import yaml

from . import defaults
from . import tasks
from . import messages
from . import pubsub
from . import utils

__all__ = ['RmqCommunicator']

_LOGGER = logging.getLogger(__name__)


def declare_exchange(channel, name, done_callback):
    channel.exchange_declare(
        done_callback, exchange=name, exchange_type='topic', auto_delete=True)


EXCHANGE_PROPERTIES = {
    'exchange_type': 'topic',
    'auto_delete': True
}


class RmqPublisher(messages.BasePublisherWithReplyQueue):
    """

    """
    DEFAULT_EXCHANGE_PARAMS = EXCHANGE_PROPERTIES

    def rpc_send(self, recipient_id, msg):
        message = messages.RpcMessage(recipient_id, body=msg)
        self.action_message(message)
        return message.future

    def broadcast_send(self, msg, sender=None, subject=None, correlation_id=None):
        message = messages.BroadcastMessage(
            body=msg,
            sender=sender,
            subject=subject,
            correlation_id=correlation_id)
        self.action_message(message)


class RmqSubscriber(pubsub.ConnectionListener):
    def __init__(self, connector,
                 exchange_name=defaults.TASK_EXCHANGE,
                 decoder=yaml.load,
                 encoder=yaml.dump):
        """
        Subscribes and listens for process control messages and acts on them
        by calling the corresponding methods of the process manager.

        :param connector: The RMQ connector
        :param exchange_name: The name of the exchange to use
        :param decoder:
        """
        super(RmqSubscriber, self).__init__()

        self._connector = connector
        self._exchange_name = exchange_name
        self._decode = decoder
        self._response_encode = encoder
        self._channel = None

        self._rpc_subscribers = {}
        self._broadcast_subscribers = []

        self._connector = connector
        self._active = False

    def add_rpc_subscriber(self, subscriber, identifier):
        self._rpc_subscribers[identifier] = subscriber

    def remove_rpc_subscriber(self, subscriber):
        for identifier, sub in self._rpc_subscribers:
            if sub is subscriber:
                self._rpc_subscribers.pop(identifier)
                return
        raise ValueError("Unknown subscriber '{}'".format(subscriber))

    def add_broadcast_subscriber(self, subscriber):
        self._broadcast_subscribers.append(subscriber)

    def remove_broadcast_subscriber(self, subscriber):
        self._broadcast_subscribers.remove(subscriber)

    def on_connection_opened(self, connector, connection):
        self.init()

    def close(self):
        self._connector.remove_connection_listener(self)
        if self._channel is not None:
            self._connector.close_channel(self._channel)
        self._channel = None
        self._connector = None

    # region RMQ methods

    @coroutine
    def init(self):
        if self._channel:
            # Already connected
            return

        connector = self._connector
        channel = yield connector.open_channel()
        channel.add_on_close_callback(self._on_channel_closed)
        yield connector.exchange_declare(channel, exchange=self._exchange_name, **EXCHANGE_PROPERTIES)

        # RPC queue
        frame = yield connector.queue_declare(channel, exclusive=True, auto_delete=True)
        rpc_queue = frame.method.queue
        result = yield connector.queue_bind(
            channel,
            queue=rpc_queue,
            exchange=self._exchange_name,
            routing_key='{}.*'.format(defaults.RPC_TOPIC))
        channel.basic_consume(self._on_rpc, queue=rpc_queue)

        # Broadcast queue
        frame = yield connector.queue_declare(channel, exclusive=True, auto_delete=True)
        broadcast_queue = frame.method.queue
        yield connector.queue_bind(
            channel,
            queue=broadcast_queue,
            exchange=self._exchange_name,
            routing_key=defaults.BROADCAST_TOPIC)
        channel.basic_consume(self._on_broadcast, queue=broadcast_queue)

        self._channel = channel
        # Have we been called externally?  In which case activate
        if not self._active:
            self._connector.add_connection_listener(self)
            self._active = True

    def _on_channel_closed(self, ch, reply_code, reply_text):
        assert self._channel is ch
        self._channel = None

    # end region

    def _on_rpc(self, ch, method, props, body):
        identifier = method.routing_key[len('{}.'.format(defaults.RPC_TOPIC)):]
        receiver = self._rpc_subscribers.get(identifier, None)
        if receiver is None:
            self._channel.basic_reject(method.delivery_tag)
        else:
            # Tell the sender that we've dealt with it
            self._channel.basic_ack(method.delivery_tag)

            msg = self._decode(body)

            try:
                result = receiver(msg)
                if isinstance(result, kiwipy.Future):
                    response = utils.pending_response()
                else:
                    response = utils.result_response(result)
            except BaseException as e:
                response = utils.exception_response(e)

            self._send_response(ch, props.reply_to, props.correlation_id, response)

    def _on_broadcast(self, ch, method, props, body):
        msg = self._decode(body)
        for receiver in self._broadcast_subscribers:
            try:
                receiver(**msg)
            except BaseException:
                import traceback
                _LOGGER.error("Exception in broadcast receiver!\n"
                              "msg: {}\n"
                              "traceback:\n{}".format(msg, traceback.format_exc()))

    def _send_response(self, ch, reply_to, correlation_id, response):
        ch.basic_publish(
            exchange='', routing_key=reply_to,
            properties=pika.BasicProperties(correlation_id=correlation_id),
            body=self._response_encode(response)
        )


class RmqCommunicator(kiwipy.Communicator):
    """
    A publisher and subscriber that implements the Communicator interface.
    """

    def __init__(self, connector,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 task_exchange=defaults.TASK_EXCHANGE,
                 task_queue=defaults.TASK_QUEUE,
                 encoder=yaml.dump,
                 decoder=yaml.load,
                 testing_mode=False):
        """

        :param connector: The RMQ connector object
        :param exchange_name: The name of the RMQ message exchange to use
        :type exchange_name: str
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

        self._connector = connector
        self._loop = connector._loop

        # Create a shared publish connection
        self._publish_connection = pika.BlockingConnection(connector.get_connection_params())

        self._message_subscriber = RmqSubscriber(
            connector,
            exchange_name,
            encoder=encoder,
            decoder=decoder
        )
        self._message_publisher = RmqPublisher(
            connector,
            exchange_name=exchange_name,
            encoder=encoder,
            decoder=decoder,
            publish_connection=self._publish_connection
        )
        self._task_subscriber = tasks.RmqTaskSubscriber(
            connector,
            exchange_name=task_exchange,
            task_queue_name=task_queue,
            encoder=encoder,
            decoder=decoder,
            testing_mode=testing_mode,
        )
        self._task_publisher = tasks.RmqTaskPublisher(
            connector,
            exchange_name=task_exchange,
            task_queue_name=task_queue,
            encoder=encoder,
            decoder=decoder,
            testing_mode=testing_mode,
            publish_connection=self._publish_connection
        )

    def init(self):
        self._loop.run_sync(self._message_subscriber.init)
        self._loop.run_sync(self._task_subscriber.init)
        self._loop.run_sync(self._message_publisher.init)
        self._loop.run_sync(self._task_publisher.init)

    def close(self):
        self._message_publisher.close()
        self._message_subscriber.close()
        self._task_publisher.close()
        self._task_subscriber.close()
        self._publish_connection.close()

    def add_rpc_subscriber(self, subscriber, identifier):
        self._message_subscriber.add_rpc_subscriber(subscriber, identifier)

    def remove_rpc_subscriber(self, subscriber):
        self._message_subscriber.remove_rpc_subscriber(subscriber)

    def add_task_subscriber(self, subscriber):
        self._task_subscriber.add_task_subscriber(subscriber)

    def remove_task_subscriber(self, subscriber):
        self._task_subscriber.remove_task_subscriber(subscriber)

    def add_broadcast_subscriber(self, subscriber):
        self._message_subscriber.add_broadcast_subscriber(subscriber)

    def remove_broadcast_subscriber(self, subscriber):
        self._message_subscriber.remove_broadcast_subscriber(subscriber)

    def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        """
        try:
            return self._message_publisher.rpc_send(recipient_id, msg)
        except pika.exceptions.UnroutableError as e:
            raise kiwipy.UnroutableError(str(e))

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        return self._message_publisher.broadcast_send(body, sender, subject, correlation_id)

    def task_send(self, msg):
        try:
            return self._task_publisher.task_send(msg)
        except pika.exceptions.UnroutableError as e:
            raise kiwipy.UnroutableError(str(e))
        except pika.exceptions.NackError as e:
            raise kiwipy.TaskRejected(str(e))

    def await(self, future=None, timeout=None):
        return self._loop.run_sync(lambda: future, timeout=timeout)
