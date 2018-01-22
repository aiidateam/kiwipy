from functools import partial

import kiwipy
import pika
import yaml

from . import defaults
from . import tasks
from . import messages
from . import pubsub
from . import utils

__all__ = ['RmqCommunicator']

_RESULT_KEY = 'result'
# This means that the intent has been actioned but not yet completed
_ACTION_SCHEDULED = 'SCHEDULED'
# This means that the intent has been completed
_ACTION_DONE = 'DONE'
# The action failed to be completed
_ACTION_FAILED = 'ACTION_FAILED'


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


class RmqSubscriber(utils.InitialisationMixin, pubsub.ConnectionListener):
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

        self._reset_channel()

        self._connector = connector
        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel(connector.connection())

    def add_rpc_subscriber(self, subscriber, identifier):
        self._rpc_subscribers[identifier] = subscriber

    def remove_rpc_subscriber(self, subscriber):
        for identifier, sub in self._rpc_subscribers:
            if sub is subscriber:
                self._rpc_subscribers.pop(identifier)
                return
        raise ValueError("Unknown subscriber '{}'".format(subscriber))

    def on_connection_opened(self, connector, connection):
        self._open_channel(connection)

    def close(self):
        self._connector.remove_connection_listener(self)
        if self._channel is not None:
            self._connector.close_channel(self._channel)
        self._channel = None
        self._connector = None

    # region RMQ methods
    def _reset_channel(self):
        self.reinitialising()

    @utils.initialiser()
    def _open_channel(self, connection):
        """ We have a connection, now create a channel """
        self._connector.open_channel(self._on_channel_open)

    @utils.initialiser()
    def _on_channel_open(self, channel):
        """ We have a channel, now declare the exchange """
        self._channel = channel
        channel.exchange_declare(
            self._on_exchange_declareok, exchange=self._exchange_name, **EXCHANGE_PROPERTIES)

    @utils.initialiser()
    def _on_exchange_declareok(self, unused_frame):
        """
        The exchange is up, now create an temporary, exclusive queue for us
        to receive messages on.
        """
        # RPC queue
        self._channel.queue_declare(self._on_rpc_queue_declareok, exclusive=True, auto_delete=True)
        # Broadcast queue
        self._channel.queue_declare(self._on_broadcast_queue_declareok, exclusive=True, auto_delete=True)

    @utils.initialiser()
    def _on_rpc_queue_declareok(self, frame):
        """
        The queue as been declared, now bind it to the exchange using the
        routing keys we're listening for.
        """
        queue_name = frame.method.queue
        self._channel.queue_bind(
            partial(self._on_rpc_bindok, queue_name), queue_name, self._exchange_name,
            routing_key='rpc.*')

    @utils.initialiser()
    def _on_rpc_bindok(self, queue_name, unused_frame):
        """ The queue has been bound, we can start consuming. """
        self._channel.basic_consume(self._on_rpc, queue=queue_name)

    @utils.initialiser()
    def _on_broadcast_queue_declareok(self, frame):
        queue_name = frame.method.queue
        self._channel.queue_bind(
            partial(self._on_broadcast_bindok, queue_name), queue_name, self._exchange_name,
            routing_key="broadcast")

    @utils.initialiser()
    def _on_broadcast_bindok(self, queue_name, unused_frame):
        """ The queue has been bound, we can start consuming. """
        self._channel.basic_consume(self._on_broadcast, queue=queue_name)
    # end region

    def _on_rpc(self, ch, method, props, body):
        identifier = method.routing_key[len('rpc.'):]
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
                receiver.on_broadcast_receive(msg)
            except BaseException:
                # TODO: Log
                pass

    def _send_response(self, ch, reply_to, correlation_id, response):
        ch.basic_publish(
            exchange='', routing_key=reply_to,
            properties=pika.BasicProperties(correlation_id=correlation_id),
            body=self._response_encode(response)
        )


class RmqCommunicator(kiwipy.CommunicatorHelper):
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
        self._connector = connector

        self._message_publisher = RmqPublisher(
            connector,
            exchange_name=exchange_name,
            encoder=encoder,
            decoder=decoder)
        self._message_subscriber = RmqSubscriber(
            connector,
            exchange_name,
            encoder=encoder,
            decoder=decoder)
        self._task_publisher = tasks.RmqTaskPublisher(
            connector,
            exchange_name=task_exchange,
            task_queue_name=task_queue,
            encoder=encoder,
            decoder=decoder,
            testing_mode=testing_mode
        )
        self._task_subscriber = tasks.RmqTaskSubscriber(
            connector,
            exchange_name=task_exchange,
            task_queue_name=task_queue,
            encoder=encoder,
            decoder=decoder,
            testing_mode=testing_mode,
        )

    def close(self):
        self._message_publisher.close()
        self._message_subscriber.close()
        self._task_publisher.close()
        self._task_subscriber.close()

    def initialised_future(self):
        return kiwipy.gather(
            self._message_publisher.initialised_future(),
            self._message_subscriber.initialised_future(),
            self._task_publisher.initialised_future(),
            self._task_subscriber.initialised_future()
        )

    def add_rpc_subscriber(self, subscriber, identifier):
        self._message_subscriber.add_rpc_subscriber(subscriber, identifier)

    def remove_rpc_subscriber(self, subscriber):
        self._message_subscriber.remove_rpc_subscriber(subscriber)

    def add_task_subscriber(self, subscriber):
        self._task_subscriber.add_task_subscriber(subscriber)

    def remove_task_subscriber(self, task_receiver):
        self._task_subscriber.remove_task_subscriber(task_receiver)

    def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        """
        return self._message_publisher.rpc_send(recipient_id, msg)

    def broadcast_msg(self, msg, reply_to=None, correlation_id=None):
        return self._message_publisher.broadcast_send(msg, reply_to, correlation_id)

    def task_send(self, msg):
        return self._task_publisher.task_send(msg)

    def await_response(self, future):
        loop = self._connector._loop

        def stop():
            loop.stop()
        try:
            future.add_done_callback(lambda _: loop.stop())
            loop.start()
        except RuntimeError:
            future.remove_done_callback(stop)
            raise RuntimeError("Cannot await a response inside the running event loop")
        else:
            return future.result()

