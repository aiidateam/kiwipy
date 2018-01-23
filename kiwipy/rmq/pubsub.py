from functools import partial
import kiwipy
import pika
import pika.exceptions
import logging
import traceback

from . import loops

__all__ = ['RmqConnector', 'ConnectionListener']

LOGGER = logging.getLogger(__name__)


class _ElasticFuture(kiwipy.Future):
    def __init__(self, primary):
        super(_ElasticFuture, self).__init__()
        self._primary = primary
        self._nchildren = 0
        self._nfinished = 0

        primary.add_done_callback(self._primary_done)

    def add(self, future):
        if self.done():
            raise kiwipy.InvalidStateError("Already done")
        future.add_done_callback(self._completed)
        self._nchildren += 1

    def _primary_done(self, primary):
        if self._children_done() or primary.exception() or primary.cancelled():
            kiwipy.copy_future(primary, self)

    def _completed(self, unused_future):
        if not self.done():
            # Check if we're all done
            self._nfinished += 1
            if self._children_done() and self._primary.done():
                kiwipy.copy_future(self._primary, self)

    def _children_done(self):
        return self._nfinished == self._nchildren


class ConnectionListener(object):
    def on_connection_opened(self, connector, connection):
        pass

    def on_connection_closed(self, connector, reconnecting):
        pass


class RmqConnector(object):
    """
    An basic RMQ client that opens a connection and one channel.
    If an auto reconnect timeout is given it will try to keep the connection
    open by reopening if it is closed.
    """
    _connection = None

    def __init__(self, amqp_url,
                 auto_reconnect_timeout=None,
                 loop=None):
        self._url = amqp_url
        self._reconnect_timeout = auto_reconnect_timeout
        self._loop = loop
        self._channels = []

        self._event_helper = kiwipy.EventHelper(ConnectionListener)
        self._running_future = None
        self._stopping = False

    @property
    def is_connected(self):
        return self._connection is not None and self._connection.is_open

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.
        """
        LOGGER.info('Connecting to %s', self._url)
        pika.TornadoConnection(pika.URLParameters(self._url),
                               on_open_callback=self._on_connection_open,
                               on_close_callback=self._on_connection_closed,
                               stop_ioloop_on_close=False,
                               custom_ioloop=self._loop)

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

    def open_channel(self, callback):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the callback method
        will be invoked.
        """
        assert self.connection() is not None, \
            "Can't open channel, not connected"
        if callback is None:
            raise ValueError("Must supply a callback")

        LOGGER.info('Creating a new channel')
        self._connection.channel(
            on_open_callback=partial(self._on_channel_open, callback))

    def close_channel(self, channel):
        self._channels.remove(channel)
        channel.close()

    def connection(self):
        return self._connection

    def add_connection_listener(self, listener):
        self._event_helper.add_listener(listener)

    def remove_connection_listener(self, listener):
        self._event_helper.remove_listener(listener)

    def run_until_complete(self, future):
        assert self._running_future is None, "Loop already running!"
        try:
            self._running_future = _ElasticFuture(future)
            return loops.run_until_complete(self._running_future, self._loop)
        finally:
            self._running_future = None

    def ensure_completes(self, future):
        if self._running_future:
            self._running_future.add(future)
            return False
        else:
            return self.run_until_complete(future)

    def _on_connection_open(self, connection):
        """Called when the RMQ connection has been opened

        :type connection: pika.BaseConnection
        """
        LOGGER.info('Connection opened')
        self._connection = connection

        self._event_helper.fire_event(
            ConnectionListener.on_connection_opened, self, connection)

    def _on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channels = []

        reconnecting = False
        if not self._stopping and self._reconnect_timeout is not None:
            LOGGER.warning(
                "Connection closed, reopening in {} seconds: ({}) {}".format(
                    self._reconnect_timeout, reply_code, reply_text
                ))
            self._connection.add_timeout(self._reconnect_timeout, self._reconnect)
            reconnecting = True

        self._event_helper.fire_event(
            ConnectionListener.on_connection_closed,
            self, reconnecting)

    def _on_channel_open(self, client_callback, channel):
        try:
            client_callback(channel)
        except:
            LOGGER.warning(
                "Exception while calling client channel opened "
                "callback.  Closing channel.:\n{}".format(traceback.format_exc()))
            channel.close()
        else:
            self._channels.append(channel)
            channel.add_on_close_callback(self._on_channel_closed)

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
