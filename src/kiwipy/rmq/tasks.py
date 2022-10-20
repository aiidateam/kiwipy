# -*- coding: utf-8 -*-
import asyncio
import collections
from contextlib import asynccontextmanager
import logging
from typing import Generator, Optional
import uuid
import weakref

import aio_pika
import shortuuid

import kiwipy

from . import defaults, messages, utils

_LOGGER = logging.getLogger(__name__)

__all__ = 'RmqTaskSubscriber', 'RmqTaskPublisher', 'RmqTaskQueue', 'RmqIncomingTask'

TaskInfo = collections.namedtuple('TaskBody', ('task', 'no_reply'))


class RmqTaskSubscriber(messages.BaseConnectionWithExchange):
    """
    Listens for tasks coming in on the RMQ task queue
    """
    TASK_QUEUE_ARGUMENTS = {'x-message-ttl': defaults.TASK_MESSAGE_TTL}

    def __init__(
        self,
        connection: aio_pika.Connection,
        exchange_name: str = defaults.MESSAGE_EXCHANGE,
        queue_name: str = defaults.TASK_QUEUE,
        testing_mode=False,
        decoder=defaults.DECODER,
        encoder=defaults.ENCODER,
        exchange_params=None,
        prefetch_size=defaults.TASK_PREFETCH_SIZE,
        prefetch_count=defaults.TASK_PREFETCH_COUNT
    ):
        # pylint: disable=too-many-arguments
        """
        :param connection: An RMQ connection
        :param exchange_name: the name of the exchange to use
        :param queue_name: the name of the task queue to use
        :param decoder: A message decoder
        :param encoder: A response encoder
        """
        super().__init__(
            connection, exchange_name=exchange_name, exchange_params=exchange_params, testing_mode=testing_mode
        )

        self._task_queue_name = queue_name
        self._testing_mode = testing_mode
        self._decode = decoder
        self._encode = encoder
        self._prefetch_size = prefetch_size
        self._prefetch_count = prefetch_count
        self._consumer_tag = None

        self._task_queue = None  # type: Optional[aio_pika.Queue]
        self._subscribers = {}
        self._pending_tasks = []

    async def add_task_subscriber(self, subscriber, identifier=None):
        identifier = identifier or shortuuid.uuid()
        if identifier in self._subscribers:
            raise kiwipy.DuplicateSubscriberIdentifier(f"Task identifier '{identifier}'")

        self._subscribers[identifier] = subscriber
        if self._consumer_tag is None:
            self._consumer_tag = await self._task_queue.consume(self._on_task)

        return identifier

    async def remove_task_subscriber(self, identifier):
        try:
            self._subscribers.pop(identifier)
        except KeyError as exception:
            raise ValueError(f"Unknown task subscriber '{identifier}'") from exception
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

    async def __aiter__(self):
        tasks = []
        try:
            while True:
                task = RmqIncomingTask(self, await self._task_queue.get(timeout=1.))
                tasks.append(task)
                yield task
        except aio_pika.exceptions.QueueEmpty:
            return
        finally:
            # Put back any tasks that are still pending (i.e. not processed or to be processed)
            for task in tasks:
                if task.state == TASK_PENDING:
                    await task.requeue()

    @asynccontextmanager
    async def next_task(self,
                        no_ack=False,
                        fail=True,
                        timeout=defaults.TASK_FETCH_TIMEOUT) -> Generator['RmqIncomingTask', None, None]:
        """
        Get the next task from the queue.

        raises:
            kiwipy.exceptions.QueueEmpty: When the queue has no tasks within the timeout
        """
        try:
            message = await self._task_queue.get(no_ack=no_ack, fail=fail, timeout=timeout)
        except aio_pika.exceptions.QueueEmpty as exc:
            raise kiwipy.exceptions.QueueEmpty(str(exc))
        else:
            task = RmqIncomingTask(self, message)
            try:
                yield task
            finally:
                if task.state == TASK_PENDING:
                    await task.requeue()

    async def _create_task_queue(self):
        """Create and bind the task queue"""
        arguments = dict(self.TASK_QUEUE_ARGUMENTS)
        if self._testing_mode:
            arguments['x-expires'] = defaults.TEST_QUEUE_EXPIRES

        # x-expires means how long does the queue stay alive after no clients
        # x-message-ttl means what is the default ttl for a message arriving in the queue
        self._task_queue = await self._channel.declare_queue(
            name=self._task_queue_name, durable=not self._testing_mode, arguments=arguments
        )
        await self._task_queue.bind(self._exchange, routing_key=self._task_queue.name)

    async def _on_task(self, message: aio_pika.IncomingMessage):
        """
        :param message: The aio_pika RMQ message
        """
        # Decode the message tuple into a task body for easier use
        rmq_task = RmqIncomingTask(self, message)
        async with rmq_task.processing() as outcome:
            for subscriber in self._subscribers.values():
                try:
                    subscriber = utils.ensure_coroutine(subscriber)
                    result = await subscriber(self, rmq_task.body)

                    # If a task returns a future it is not considered done until the chain of
                    # futures (i.e. if the first future resolves to a future and so on) finishes
                    # and produces a concrete result
                    while asyncio.isfuture(result):
                        if not rmq_task.no_reply:
                            await self._send_response(utils.pending_response(), message)
                        result = await result
                except kiwipy.TaskRejected:
                    # Task was rejected by this subscriber, keep trying
                    continue
                except kiwipy.CancelledError:
                    # The subscriber has cancelled their processing of the task
                    outcome.cancel()
                except Exception as exc:  # pylint: disable=broad-except
                    # There was an exception during the processing of this task
                    outcome.set_exception(exc)
                    _LOGGER.exception('Exception occurred while processing task.')
                else:
                    # All good
                    outcome.set_result(result)
                    break  # Got handled

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


TASK_PENDING = 'pending'
TASK_FINISHED = 'finished'
TASK_PROCESSING = 'processing'
TASK_REQUEUED = 'requeued'


class RmqIncomingTask:

    def __init__(self, subscriber: RmqTaskSubscriber, message: aio_pika.IncomingMessage):
        self._subscriber = subscriber
        self._message = message
        self._task_info = TaskInfo(*subscriber._decode(message.body))
        self._state = TASK_PENDING
        self._outcome_ref = None  # type: Optional[weakref.ReferenceType]
        self._loop = self._subscriber.loop()

    @property
    def body(self) -> str:
        return self._task_info.task

    @property
    def no_reply(self) -> bool:
        return self._task_info.no_reply

    @property
    def state(self) -> str:
        return self._state

    def process(self) -> asyncio.Future:
        if self._state != TASK_PENDING:
            raise asyncio.InvalidStateError(f'The task is {self._state}')

        self._state = TASK_PROCESSING
        outcome = self._loop.create_future()
        # Rely on the done callback to signal the end of processing
        outcome.add_done_callback(self._on_task_done)
        # Or the user lets the future get destroyed
        self._outcome_ref = weakref.ref(outcome, self._outcome_destroyed)

        return outcome

    async def requeue(self):
        if self._state not in [TASK_PENDING, TASK_PROCESSING]:
            raise asyncio.InvalidStateError(f'The task is {self._state}')

        self._state = TASK_REQUEUED
        await self._message.nack(requeue=True)
        self._finalise()

    @asynccontextmanager
    async def processing(self) -> Generator[asyncio.Future, None, None]:
        """Processing context.  The task should be done at the end otherwise it's assumed the
        caller doesn't want to process it, and it's sent back to the queue"""

        if self._state != TASK_PENDING:
            raise asyncio.InvalidStateError(f'The task is {self._state}')

        self._state = TASK_PROCESSING
        outcome = self._loop.create_future()
        try:
            yield outcome
        except KeyboardInterrupt:  # pylint: disable=try-except-raise
            raise
        except Exception as exc:
            # Set the exception on the task and re-raise so the client also sees it
            outcome.set_exception(exc)
            raise
        finally:
            if outcome.done():
                await self._task_done(outcome)
            else:
                await self.requeue()

    def _on_task_done(self, outcome):
        """Schedule a task to call ``_task_done`` when the outcome is done."""
        self._loop.create_task(self._task_done(outcome))

    async def _task_done(self, outcome: asyncio.Future):
        assert outcome.done()
        self._outcome_ref = None

        if outcome.cancelled():
            # Whoever took the task decided not to process it
            self._state = TASK_PENDING
        else:
            # Task is done or excepted
            # Permanently store the outcome
            self._state = TASK_FINISHED
            await self._message.ack()

            # We have to get the result from the future here (even if not replying), otherwise
            # python complains that it was never retrieved in case of exception
            try:
                reply_body = utils.result_response(outcome.result())
            except Exception as exc:  # pylint: disable=broad-except
                reply_body = utils.exception_response(exc)

            if not self.no_reply:
                # Schedule a task to send the appropriate response
                # pylint: disable=protected-access
                await self._subscriber._send_response(reply_body, self._message)

        # Clean up
        self._finalise()

    def _outcome_destroyed(self, outcome_ref):
        # This only happens if someone called self.process() and then let the future
        # get destroyed without setting an outcome
        assert outcome_ref is self._outcome_ref
        # This task will not be processed
        self._outcome_ref = None
        asyncio.run_coroutine_threadsafe(self.requeue(), loop=self._loop)

    def _finalise(self):
        self._outcome_ref = None
        self._subscriber = None
        self._message = None


class RmqTaskPublisher(messages.BasePublisherWithReplyQueue):
    """
    Publishes messages to the RMQ task queue and gets the response
    """

    def __init__(
        self,
        connection,
        queue_name=defaults.TASK_QUEUE,
        exchange_name=defaults.MESSAGE_EXCHANGE,
        exchange_params=None,
        encoder=defaults.ENCODER,
        decoder=defaults.DECODER,
        confirm_deliveries=True,
        testing_mode=False
    ):
        # pylint: disable=too-many-arguments
        super().__init__(
            connection,
            exchange_name=exchange_name,
            exchange_params=exchange_params,
            encoder=encoder,
            decoder=decoder,
            confirm_deliveries=confirm_deliveries,
            testing_mode=testing_mode
        )
        self._task_queue_name = queue_name

    async def task_send(self, task, no_reply: bool = False) -> asyncio.Future:
        """Send a task for processing by a task subscriber.

        All task messages will be set to be persistent by setting `delivery_mode=2`.

        :param task: The task payload
        :param no_reply: Don't send a reply containing the result of the task
        :return: A future representing the result of the task
        """
        _LOGGER.debug(
            'Sending task with routing key %r to RMQ queue %r (reply=%r): %r',
            self._task_queue_name,
            self._reply_queue.name,
            not no_reply,
            task,
        )
        # Build the full message body and encode as a tuple
        body = self._encode((task, no_reply))
        # Now build up the full aio_pika message
        task_msg = aio_pika.Message(
            body=body,
            correlation_id=str(uuid.uuid4()),
            reply_to=self._reply_queue.name,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT  # Task messages need to be persistent
        )

        result_future = None
        if no_reply:
            published = await self.publish(task_msg, routing_key=self._task_queue_name, mandatory=True)
        else:
            published, result_future = await self.publish_expect_response(
                task_msg, routing_key=self._task_queue_name, mandatory=True
            )

        assert published, 'The task was not published to the exchange'
        return result_future


class RmqTaskQueue:
    """Combines a task publisher and subscriber to create a work queue where you can do both"""

    def __init__(
        self,
        connection,
        exchange_name=defaults.MESSAGE_EXCHANGE,
        queue_name=defaults.TASK_QUEUE,
        decoder=defaults.DECODER,
        encoder=defaults.ENCODER,
        exchange_params=None,
        prefetch_size=defaults.TASK_PREFETCH_SIZE,
        prefetch_count=defaults.TASK_PREFETCH_COUNT,
        testing_mode=False
    ):
        # pylint: disable=too-many-arguments
        self._publisher = RmqTaskPublisher(
            connection,
            exchange_name=exchange_name,
            exchange_params=exchange_params,
            queue_name=queue_name,
            decoder=decoder,
            encoder=encoder,
            testing_mode=testing_mode
        )
        self._subscriber = RmqTaskSubscriber(
            connection,
            exchange_name=exchange_name,
            exchange_params=exchange_params,
            queue_name=queue_name,
            decoder=decoder,
            encoder=encoder,
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
            testing_mode=testing_mode
        )

    async def __aiter__(self):
        # Have to do it this way rather than the more convenient yield from style because
        # python doesn't support it for coroutines.  See:
        # https://stackoverflow.com/questions/47376408/why-cant-i-yield-from-inside-an-async-function
        async for task in self._subscriber:
            yield task

    async def task_send(self, task, no_reply: bool = False):
        """Send a task to the queue"""
        return await self._publisher.task_send(task, no_reply)

    async def add_task_subscriber(self, subscriber, identifier=None):
        return await self._subscriber.add_task_subscriber(subscriber, identifier)

    async def remove_task_subscriber(self, identifier):
        return await self._subscriber.remove_task_subscriber(identifier)

    @asynccontextmanager
    async def next_task(self, no_ack=False, fail=True, timeout=defaults.TASK_FETCH_TIMEOUT):
        async with self._subscriber.next_task(no_ack=no_ack, fail=fail, timeout=timeout) as task:  # pylint: disable=not-async-context-manager
            yield task

    async def connect(self):
        await self._subscriber.connect()
        await self._publisher.connect()

    async def disconnect(self):
        await self._subscriber.disconnect()
        await self._publisher.disconnect()
