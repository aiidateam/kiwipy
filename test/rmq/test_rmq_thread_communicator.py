# -*- coding: utf-8 -*-
# pylint: disable=invalid-name, redefined-outer-name
import concurrent.futures
import pathlib
import sys
import unittest

import shortuuid
import pytest

import kiwipy
from kiwipy import rmq

from ..utils import CommunicatorTester
from . import utils

WAIT_TIMEOUT = 5.


@pytest.fixture
def thread_communicator():
    message_exchange = '{}.{}'.format(__file__, shortuuid.uuid())
    task_exchange = '{}.{}'.format(__file__, shortuuid.uuid())
    task_queue = '{}.{}'.format(__file__, shortuuid.uuid())

    communicator = rmq.RmqThreadCommunicator.connect(
        connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
        message_exchange=message_exchange,
        task_exchange=task_exchange,
        task_queue=task_queue,
        testing_mode=True
    )

    yield communicator

    communicator.close()


@pytest.fixture
def thread_task_queue(thread_communicator: rmq.RmqThreadCommunicator):
    task_queue_name = '{}.{}'.format(__file__, shortuuid.uuid())

    task_queue = thread_communicator.task_queue(task_queue_name)

    yield task_queue


class TestRmqThreadCommunicator(CommunicatorTester, unittest.TestCase):
    """Use the standard tests cases to check the RMQ thread communicator"""

    def create_communicator(self):
        message_exchange = '{}.message_exchange.{}'.format(self.__class__.__name__, shortuuid.uuid())
        task_exchange = '{}.task_exchange.{}'.format(self.__class__.__name__, shortuuid.uuid())
        task_queue = '{}.task_queue.{}'.format(self.__class__.__name__, shortuuid.uuid())

        return rmq.RmqThreadCommunicator.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            testing_mode=True
        )

    def destroy_communicator(self, communicator):
        communicator.close()

    def test_context_manager(self):
        MESSAGE = 'get this yo'

        rpc_future = kiwipy.Future()

        def rpc_get(_comm, msg):
            rpc_future.set_result(msg)

        self.communicator.add_rpc_subscriber(rpc_get, 'test_context_manager')
        # Check the context manager of the communicator works
        with self.communicator as comm:
            comm.rpc_send('test_context_manager', MESSAGE)

        message = rpc_future.result(self.WAIT_TIMEOUT)
        self.assertEqual(MESSAGE, message)

    def test_custom_task_queue(self):
        """Test creating a custom task queue"""
        TASK = 'The meaning?'
        RESULT = 42
        result_future = kiwipy.Future()

        tasks = []

        def on_task(_comm, task):
            tasks.append(task)
            return result_future

        task_queue = self.communicator.task_queue('test-queue-{}'.format(utils.rand_string(5)))

        task_queue.add_task_subscriber(on_task)
        task_future = task_queue.task_send(TASK).result(timeout=self.WAIT_TIMEOUT)

        result_future.set_result(42)

        result = task_future.result(timeout=self.WAIT_TIMEOUT)

        self.assertEqual(TASK, tasks[0])
        self.assertEqual(RESULT, result)

    def test_task_queue_next(self):
        """Test creating a custom task queue"""
        TASK = 'The meaning?'
        RESULT = 42

        # Create a new queue and sent the task
        task_queue = self.communicator.task_queue('test-queue-{}'.format(utils.rand_string(5)))
        task_future = task_queue.task_send(TASK)

        # Get the task and carry it out
        with task_queue.next_task() as task:
            task.process().set_result(RESULT)

        # Now wait for the result
        result = task_future.result(timeout=self.WAIT_TIMEOUT)
        self.assertEqual(RESULT, result)


def test_queue_get_next(thread_task_queue: rmq.RmqThreadTaskQueue):
    """Test getting the next task from the queue"""
    result = thread_task_queue.task_send('Hello!')
    with thread_task_queue.next_task(timeout=1.) as task:
        with task.processing() as outcome:
            assert task.body == 'Hello!'
            outcome.set_result('Goodbye')
    assert result.result() == 'Goodbye'


def test_queue_iter(thread_task_queue: rmq.RmqThreadTaskQueue):
    """Test iterating through a task queue"""
    results = []

    # Insert tasks
    for i in range(10):
        results.append(thread_task_queue.task_send(i))

    for task in thread_task_queue:
        with task.processing() as outcome:
            outcome.set_result(task.body * 10)

    concurrent.futures.wait(results)
    assert all(result.done() for result in results)

    # Make sure there are no more tasks in the queue
    for _ in thread_task_queue:
        assert False, "Shouldn't get here"


def test_queue_iter_not_process(thread_task_queue: rmq.RmqThreadTaskQueue):
    """Check what happens when we iterate a queue but don't process all tasks"""
    outcomes = []

    # Insert tasks
    for i in range(10):
        outcomes.append(thread_task_queue.task_send(i))

    # Now let's see what happens when we have tasks but don't process some of them
    for task in thread_task_queue:
        if task.body < 5:
            task.process().set_result(task.body * 10)

    concurrent.futures.wait(outcomes[:5])
    for i, outcome in enumerate(outcomes[:5]):
        assert outcome.result() == i * 10

    # Now, to through and process the rest
    for task in thread_task_queue:
        task.process().set_result(task.body * 10)

    concurrent.futures.wait(outcomes)
    for i, outcome in enumerate(outcomes):
        assert outcome.result() == i * 10


def test_queue_task_forget(thread_task_queue: rmq.RmqThreadTaskQueue):
    """
    Check what happens when we forget to process a task we said we would
    WARNING: This test mail fail when running with a debugger as it relies on the 'outcome'
    reference count dropping to zero but the debugger may be preventing this.
    """
    outcomes = list()

    outcomes.append(thread_task_queue.task_send(1))

    # Get the first task and say that we will process it
    outcome = None
    with thread_task_queue.next_task() as task:
        outcome = task.process()

    with pytest.raises(kiwipy.exceptions.QueueEmpty):
        with thread_task_queue.next_task():
            pass

    # Now let's 'forget' i.e. lose the outcome
    del outcome

    # Now the task should be back in the queue
    with thread_task_queue.next_task() as task:
        task.process().set_result(10)

    concurrent.futures.wait(outcomes)
    assert outcomes[0].result() == 10


def test_empty_queue(thread_task_queue: rmq.RmqThreadTaskQueue):
    with pytest.raises(kiwipy.exceptions.QueueEmpty):
        with thread_task_queue.next_task(timeout=5.):
            pass


def test_task_processing_exception(thread_task_queue: rmq.RmqThreadTaskQueue):
    """Check that if there is an exception processing a task that it is removed from the queue"""
    task_future = thread_task_queue.task_send('Do this')

    # The error should still get propageted in the 'worker'
    with pytest.raises(RuntimeError):
        with thread_task_queue.next_task(timeout=WAIT_TIMEOUT) as task:
            with task.processing():
                raise RuntimeError('Cannea do it captain!')

    # And the task sender should get a remote exception to inform them of the problem
    with pytest.raises(kiwipy.RemoteException):
        task_future.result(timeout=WAIT_TIMEOUT)

    # The queue should now be empty
    with pytest.raises(kiwipy.QueueEmpty):
        with thread_task_queue.next_task(timeout=1.):
            pass


@pytest.mark.skipif(sys.version_info < (3, 6), reason='`pytest-notebook` plugin requires Python >= 3.6')
def test_jupyter_notebook():
    """Test that the `RmqThreadCommunicator` can be used in a Jupyter notebook."""
    from pytest_notebook.nb_regression import NBRegressionFixture

    fixture = NBRegressionFixture(exec_timeout=50)
    fixture.diff_color_words = False
    fixture.diff_ignore = ('/metadata/language_info/version',)

    # Express the path in a way that will work no matter where the tests are being ran and convert
    # to str as py35 doesn't support Paths being passed to open()
    my_dir = pathlib.Path(__file__).parent
    with open(str(my_dir / pathlib.Path('notebooks/communicator.ipynb'))) as handle:
        fixture.check(handle)
