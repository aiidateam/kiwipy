# -*- coding: utf-8 -*-
import asyncio
import collections
from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
from typing import Generator, Optional, Union
import uuid
import weakref

import aio_pika
import shortuuid

import kiwipy

from . import defaults, messages, utils

_LOGGER = logging.getLogger(__name__)

__all__ = 'RmqTaskSubscriber', 'RmqTaskPublisher', 'RmqTaskQueue', 'RmqIncomingTask', 'TaskResult'

TaskInfo = collections.namedtuple('TaskBody', ('task', 'no_reply', 'nowait'))


@dataclass
class TaskResult:
    """Result from a task subscriber with immediate ID and deferred result.

    This is used when a subscriber wants to:
    1. Return an immediate identifier (task_id) that can be sent to the client right away
    2. Provide a Future that kiwipy will wait on before acknowledging the message

    The reply behavior depends on the `nowait` flag in the task message:
    - nowait=True: Send {task_id, result: None} immediately, ack when Future done
    - nowait=False: Wait for Future, send {task_id, result: <resolved>}, then ack
    """

    task_id: Union[int, str, uuid.UUID]
    result: kiwipy.Future  # Future resolves to Any, used for reply content


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
        # relinquish so that if there is requeue coroutines, they are run first and task queue get updated
        await asyncio.sleep(0)

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
        outcome = rmq_task.process()  # Returns Future with done callback for ack

        for subscriber in self._subscribers.values():
            try:
                subscriber = utils.ensure_coroutine(subscriber)
                # Call subscriber with just (comm, task) - kiwipy handles TaskResult internally
                result = await subscriber(self, rmq_task.body)

                # Handle TaskResult: subscriber returns TaskResult(task_id, result=Future)
                if isinstance(result, TaskResult):
                    # If nowait, send task_id immediately as reply
                    if rmq_task.nowait and not rmq_task.no_reply:
                        # Send just the task_id for nowait mode
                        reply_body = utils.result_response(result.task_id)
                        await self._send_response(reply_body, message)
                        rmq_task._early_reply_sent = True

                    # Attach callback to wait for result Future and ack when done
                    self._attach_task_result_callback(result, outcome, rmq_task)
                    return

                # If a task returns a future, attach a done callback instead of awaiting.
                # This keeps the task slot blocked (message unacked) until the future resolves.
                if asyncio.isfuture(result):
                    if not rmq_task.no_reply:
                        await self._send_response(utils.pending_response(), message)
                    self._attach_outcome_callback(result, outcome, rmq_task)
                    return  # Don't block - ack happens when result future resolves

                # Non-future result: complete immediately
                outcome.set_result(result)
                return  # Got handled

            except kiwipy.TaskRejected:
                # Task was rejected by this subscriber, keep trying
                continue
            except kiwipy.CancelledError:
                # The subscriber has cancelled their processing of the task
                outcome.cancel()
                return
            except Exception as exc:  # pylint: disable=broad-except
                # There was an exception during the processing of this task
                outcome.set_exception(exc)
                _LOGGER.exception('Exception occurred while processing task.')
                return

    def _attach_outcome_callback(
        self, result_future: asyncio.Future, outcome: asyncio.Future, rmq_task: 'RmqIncomingTask' = None
    ):
        """Attach a callback to resolve the outcome when the result future completes.

        This keeps the task slot blocked (message unacked) until the future resolves.
        Handles chained futures (futures that resolve to futures) using a while loop.
        Also handles TaskResult by extracting the result Future and waiting for it.
        """

        def resolve_chain(fut: asyncio.Future):
            if fut.cancelled():
                outcome.cancel()
                return
            if fut.exception():
                outcome.set_exception(fut.exception())
                return

            result = fut.result()

            # Follow chain of futures with a while loop
            while asyncio.isfuture(result):
                if result.done():
                    # Already resolved - get result and continue loop
                    if result.cancelled():
                        outcome.cancel()
                        return
                    if result.exception():
                        outcome.set_exception(result.exception())
                        return
                    result = result.result()
                else:
                    # Not done yet - attach callback and return
                    result.add_done_callback(resolve_chain)
                    return

            # Check if the result is a TaskResult - need to handle it specially
            if isinstance(result, TaskResult):
                # If nowait, send task_id immediately as early reply (if not already sent)
                if rmq_task and rmq_task.nowait and not rmq_task.no_reply and not rmq_task.early_reply_sent:
                    # Need to send early reply - schedule it in the event loop
                    async def send_early_reply():
                        reply_body = utils.result_response(result.task_id)
                        await self._send_response(reply_body, rmq_task._message)
                        rmq_task._early_reply_sent = True
                    self.loop().create_task(send_early_reply())
                self._attach_task_result_callback(result, outcome, rmq_task)
                return

            outcome.set_result(result)

        result_future.add_done_callback(resolve_chain)

    def _attach_task_result_callback(
        self, task_result: TaskResult, outcome: asyncio.Future, rmq_task: 'RmqIncomingTask'
    ):
        """Attach a callback to handle TaskResult when the result Future completes.

        This keeps the task slot blocked (message unacked) until the Future resolves.
        - If nowait=True: Early reply with task_id was already sent, just ack when done
        - If nowait=False: Send the full result when done

        Note: task_result.result is a kiwipy.Future (concurrent.futures.Future) which may be
        completed from a different thread. We use call_soon_threadsafe to safely update the
        asyncio.Future outcome from the callback.
        """
        loop = self.loop()
        nowait = rmq_task.nowait if rmq_task else False

        def on_result_done(fut):
            # This callback may be called from a different thread (plumpy's event loop)
            # Use call_soon_threadsafe to safely update the asyncio outcome Future
            if fut.cancelled():
                loop.call_soon_threadsafe(outcome.cancel)
                return

            try:
                exc = fut.exception()
            except Exception:
                exc = None
            if exc is not None:
                loop.call_soon_threadsafe(outcome.set_exception, exc)
                return

            resolved_result = fut.result()

            if nowait:
                # Early reply with task_id was already sent, just ack (outcome not used for reply)
                loop.call_soon_threadsafe(outcome.set_result, resolved_result)
            else:
                # Send the full result - for backward compatibility, just the resolved result
                loop.call_soon_threadsafe(outcome.set_result, resolved_result)

        task_result.result.add_done_callback(on_result_done)

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
        self._early_reply_sent = False

    @property
    def body(self) -> str:
        return self._task_info.task

    @property
    def no_reply(self) -> bool:
        return self._task_info.no_reply

    @property
    def nowait(self) -> bool:
        return self._task_info.nowait

    @property
    def state(self) -> str:
        return self._state

    @property
    def early_reply_sent(self) -> bool:
        """Return True if an early reply has already been sent."""
        return self._early_reply_sent

    async def send_early_response(self, result) -> bool:
        """Send a response without acknowledging the message.

        This allows confirming receipt/progress while keeping the task slot blocked.
        The message will be acknowledged when the task handler completes.

        :param result: The result to send as the response
        :return: True if response was sent, False if no_reply is set or already sent
        """
        if self.no_reply:
            return False
        if self._early_reply_sent:
            return False

        reply_body = utils.result_response(result)
        await self._subscriber._send_response(reply_body, self._message)
        self._early_reply_sent = True
        return True

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

            if not self.no_reply and not self._early_reply_sent:
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

    async def task_send(self, task, no_reply: bool = False, nowait: bool = False) -> asyncio.Future:
        """Send a task for processing by a task subscriber.

        All task messages will be set to be persistent by setting `delivery_mode=2`.

        :param task: The task payload
        :param no_reply: Don't send a reply containing the result of the task
        :param nowait: If True, send task_id reply immediately instead of waiting for result
        :return: A future representing the result of the task
        """
        _LOGGER.debug(
            'Sending task with routing key %r to RMQ queue %r (reply=%r, nowait=%r): %r',
            self._task_queue_name,
            self._reply_queue.name,
            not no_reply,
            nowait,
            task,
        )
        # Build the full message body and encode as a tuple
        body = self._encode((task, no_reply, nowait))
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

    async def task_send(self, task, no_reply: bool = False, nowait: bool = False):
        """Send a task to the queue"""
        return await self._publisher.task_send(task, no_reply, nowait)

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
