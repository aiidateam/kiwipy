import logging
import uuid
from functools import partial
import kiwipy
import pika
from tornado.gen import coroutine
import yaml

from . import defaults
from . import messages
from . import pubsub
from . import utils

_LOGGER = logging.getLogger(__name__)

__all__ = ['RmqTaskSubscriber', 'RmqTaskPublisher']


class TaskMessage(messages.Message):
    def __init__(self, body, correlation_id=None):
        super(TaskMessage, self).__init__()
        self.correlation_id = correlation_id if correlation_id is not None else str(uuid.uuid4())
        self.body = body
        self._future = kiwipy.Future()

    @property
    def future(self):
        return self._future

    def send(self, publisher):
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())
        publisher.publish_msg(
            self.body,
            routing_key=None,  # Set by the publisher
            correlation_id=self.correlation_id,
            mandatory=True, )

        publisher.await_response(self.correlation_id, self.on_response)
        return self.future

    def on_response(self, done_future):
        kiwipy.copy_future(done_future, self.future)


class PendingTask(object):
    def __init__(self, subscriber, future, delivery_tag, correlation_id, reply_to):
        self._subscriber = subscriber
        self._delivery_tag = delivery_tag
        self._correlation_id = correlation_id
        self._reply_to = reply_to
        self._future = future
        self._future.add_done_callback(self._on_task_done)

    def ignore(self):
        self._future.remove_done_callback(self._on_task_done)
        self._future = None

    def _on_task_done(self, future):
        try:
            response = utils.result_response(future.result())
        except Exception as e:
            response = utils.exception_response(e)

        self._subscriber._task_finished(
            self._delivery_tag,
            self._correlation_id,
            self._reply_to,
            response)


class RmqTaskSubscriber(messages.BaseConnectionWithExchange):
    """
    Listens for tasks coming in on the RMQ task queue
    """

    def __init__(self, connector,
                 task_queue_name=defaults.TASK_QUEUE,
                 testing_mode=False,
                 decoder=yaml.load,
                 encoder=yaml.dump,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 ):
        """
        :param connector: An RMQ connector
        :type connector: :class:`pubsub.RmqConnector`
        :param task_queue_name: The name of the queue to use
        :param decoder: A message decoder
        :param encoder: A response encoder
        """
        super(RmqTaskSubscriber, self).__init__(
            connector,
            exchange_name=exchange_name,
            exchange_params=exchange_params
        )

        self._task_queue = task_queue_name
        self._testing_mode = testing_mode
        self._decode = decoder
        self._encode = encoder

        self._subscribers = []
        self._pending_tasks = []

    def add_task_subscriber(self, subscriber):
        self._subscribers.append(subscriber)

    def remove_task_subscriber(self, subscriber):
        self._subscribers.remove(subscriber)

    @coroutine
    def connect(self):
        if self.channel():
            # Already connected
            return

        yield super(RmqTaskSubscriber, self).connect()
        connector = self._connector
        self.channel().basic_qos(prefetch_count=1)

        # Set up task queue
        task_queue = self._task_queue
        yield connector.queue_declare(
            self._channel,
            queue=task_queue,
            durable=not self._testing_mode,
            auto_delete=self._testing_mode,
            arguments={"x-expires": 60000}
        )
        yield connector.queue_bind(
            self._channel,
            queue=task_queue,
            exchange=self._exchange_name,
            routing_key=task_queue)

        self._consumer_tag = self.channel().basic_consume(self._on_task, task_queue)

    def _on_task(self, ch, method, props, body):
        handled = False
        for subscriber in self._subscribers:
            try:
                task = self._decode(body)
                result = subscriber(task)
                if isinstance(result, kiwipy.Future):
                    pending = PendingTask(self, result, method.delivery_tag, props.correlation_id, props.reply_to)
                    self._pending_tasks.append(pending)
                else:
                    # Finished
                    self._task_finished(
                        method.delivery_tag,
                        props.correlation_id,
                        props.reply_to,
                        utils.result_response(result))
                handled = True
                break
            except kiwipy.TaskRejected:
                pass
            except KeyboardInterrupt:
                raise
            except Exception as e:
                import traceback
                response = '{}\n{}'.format(e, traceback.format_exc())
                self._task_finished(
                    method.delivery_tag,
                    props.correlation_id,
                    props.reply_to,
                    utils.exception_response(response))
                handled = True

        if not handled:
            self._channel.basic_reject(delivery_tag=method.delivery_tag)

    def _task_finished(self, delivery_tag, correlation_id, reply_to, response):
        """
        Send an acknowledgement of the task being actioned and a response to the
        initiator.

        :param props: The message properties
        :param method: The message method
        :param response: The response to send to the initiator
        """
        self._channel.basic_ack(delivery_tag=delivery_tag)
        self._send_response(correlation_id, reply_to, response)

    def _send_response(self, correlation_id, reply_to, response):
        # Build full response
        response[utils.HOST_KEY] = utils.get_host_info()
        self.channel().basic_publish(
            exchange='',
            routing_key=reply_to,
            body=self._encode(response),
            properties=pika.BasicProperties(correlation_id=correlation_id))


class RmqTaskPublisher(messages.BasePublisherWithReplyQueue):
    """
    Publishes messages to the RMQ task queue and gets the response
    """

    def __init__(self, connector,
                 task_queue_name=defaults.TASK_QUEUE,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 encoder=yaml.dump,
                 decoder=yaml.load,
                 confirm_deliveries=True,
                 testing_mode=False):
        super(RmqTaskPublisher, self).__init__(
            connector,
            exchange_name=exchange_name,
            exchange_params=exchange_params,
            encoder=encoder,
            decoder=decoder,
            confirm_deliveries=confirm_deliveries,
            testing_mode=testing_mode)
        self._task_queue = task_queue_name

    def publish_msg(self, task, routing_key, correlation_id=None, mandatory=False, ttl=None):
        if routing_key is not None:
            _LOGGER.warn(
                "Routing key '{}' passed but is ignored for all tasks".format(routing_key))
        # pika (and AMQP) expects the ttl to be a string
        if ttl is not None and not isinstance(ttl, basestring):
            ttl = str(ttl)

        return self.do_publish(
            correlation_id,
            exchange=self.get_exchange_name(),
            routing_key=self._task_queue,
            properties=pika.BasicProperties(
                reply_to=self.get_reply_queue_name(),
                delivery_mode=2,  # Persistent
                correlation_id=correlation_id
            ),
            body=self._encode(task),
            mandatory=mandatory,
        )

    def task_send(self, msg):
        message = TaskMessage(msg)
        self.action_message(message)
        return message.future
