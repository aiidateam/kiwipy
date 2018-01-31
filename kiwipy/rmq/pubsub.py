from enum import Enum
import logging
from functools import partial
import pika
import pika.exceptions
import kiwipy
from tornado import gen

from . import loops

__all__ = ['RmqConnector', 'ConnectionListener']

LOGGER = logging.getLogger(__name__)


class ConnectionListener(object):
    def on_connection_opened(self, connector, connection):
        pass

    def on_connection_closed(self, connector, reconnecting):
        pass


class ConnectorState(Enum):
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2
    WAITING_TO_RECONNECT = 3


class RmqConnector(object):
    """
    A basic RMQ client that opens a connection and one channel.
    If an auto reconnect timeout is given it will try to keep the connection
    open by reopening if it is closed.
    """
    _connection = None

    def __init__(self,
                 amqp_url,
                 auto_reconnect_timeout=None,
                 loop=None):
        self._connection_params = pika.URLParameters(amqp_url)
        self._reconnect_timeout = auto_reconnect_timeout
        self._loop = loop if loop is not None else loops.new_event_loop()
        self._channels = []

        self._event_helper = kiwipy.EventHelper(ConnectionListener)
        self._running_future = None
        self._stopping = False

        self._connection_future = kiwipy.Future()

        self._state = ConnectorState.DISCONNECTED

    @property
    def is_connected(self):
        return self._connection is not None and self._connection.is_open

    def get_connection_params(self):
        return self._connection_params

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.
        """
        LOGGER.info('Connecting to %s', self.get_connection_params())
        if self._state is ConnectorState.DISCONNECTED:
            self._state = ConnectorState.CONNECTING
            self._connection = pika.TornadoConnection(
                self._connection_params,
                on_open_callback=self._on_connection_open,
                on_close_callback=self._on_connection_closed,
                stop_ioloop_on_close=False,
                custom_ioloop=self._loop)

        return self._connection_future

    def close(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """
        LOGGER.info('Stopping')
        self._stopping = True
        self._close_channels()
        self._close_connection()

    def open_channel(self):
        connection_future = self.connect()
        future = kiwipy.Future()
        connection_future.add_done_callback(
            lambda _: self._open_channel(connection_future.result(), future))
        return future

    def _open_channel(self, connection, future):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command.
        """
        LOGGER.info('Creating a new channel')
        connection.channel(on_open_callback=partial(self._on_channel_open, future))
        return future

    def close_channel(self, channel):
        self._channels.remove(channel)
        channel.close()

    def exchange_declare(self, channel, nowait=False, **kwargs):
        params = dict(kwargs)
        params['nowait'] = nowait
        if nowait:
            callback = None
            future = None
        else:
            future = kiwipy.Future()
            callback = future.set_result
        params['callback'] = callback

        LOGGER.info('Declaring exchange {}'.format(params))
        channel.exchange_declare(**params)
        return future

    def queue_declare(self, channel, nowait=False, **kwargs):
        params = dict(kwargs)
        params['nowait'] = nowait
        if nowait:
            callback = None
            future = None
        else:
            future = kiwipy.Future()
            callback = future.set_result
        params['callback'] = callback

        LOGGER.info('Declaring queue {}'.format(params))
        channel.queue_declare(**params)
        return future

    def queue_bind(self, channel, nowait=False, **kwargs):
        params = dict(kwargs)
        params['nowait'] = nowait
        if nowait:
            callback = None
            future = None
        else:
            future = kiwipy.Future()
            callback = future.set_result
        params['callback'] = callback

        LOGGER.info('Binding queue {}'.format(params))
        channel.queue_bind(**params)
        return future

    def connection(self):
        return self._connection

    def add_connection_listener(self, listener):
        self._event_helper.add_listener(listener)

    def remove_connection_listener(self, listener):
        self._event_helper.remove_listener(listener)

    def _on_connection_open(self, connection):
        """Called when the RMQ connection has been opened

        :type connection: pika.BaseConnection
        """
        LOGGER.info('Connection opened')
        self._connection = connection
        self._connection_future.set_result(connection)
        self._state = ConnectorState.CONNECTED

        self._event_helper.fire_event(ConnectionListener.on_connection_opened, self, connection)

    def _on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channels = []
        self._connection = None
        self._state = ConnectorState.DISCONNECTED
        self._connection_future = None

        if not self._stopping and self._reconnect_timeout is not None:
            self._connection_future = kiwipy.Future()
            self._state = ConnectorState.WAITING_TO_RECONNECT
            LOGGER.warning(
                "Connection closed, reopening in {} seconds: ({}) {}".format(
                    self._reconnect_timeout, reply_code, reply_text
                ))
            self._connection.add_timeout(self._reconnect_timeout, self._reconnect)
        else:
            self._connection_future = None

        self._event_helper.fire_event(
            ConnectionListener.on_connection_closed, self,
            self._state == ConnectorState.WAITING_TO_RECONNECT)

    def _on_channel_open(self, future, channel):
        self._channels.append(channel)
        channel.add_on_close_callback(self._on_channel_closed)
        future.set_result(channel)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        try:
            self._channels.remove(channel)
            LOGGER.info("Channel '{}' closed.  Code '{}', text '{}'".format(
                channel.channel_number, reply_code, reply_text))
        except ValueError:
            pass

    def _reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        if not self._stopping:
            # Create a new connection
            self.connect()

    def _close_channels(self):
        LOGGER.info('Closing channels')
        for ch in self._channels:
            try:
                ch.close()
            except pika.exceptions.ChannelAlreadyClosing:
                pass

    def _close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None:
            LOGGER.info('Closing connection')
            self._connection.close()
            self._connection = None
