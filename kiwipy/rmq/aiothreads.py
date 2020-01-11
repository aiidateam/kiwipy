"""
A module to create interoperability between concurrent threads and asyncio.

An asyncio event loop can be running on a thread on which coroutines can be scheduled
from a different threads.  The result is returned as a concurrent future which can be
waited on.
"""
import asyncio
import concurrent.futures
from concurrent.futures import Future as ThreadFuture
from contextlib import contextmanager
from functools import partial
import logging
import sys
import threading
import typing

import kiwipy

__all__ = ('LoopScheduler',)

_LOGGER = logging.getLogger(__name__)


def aio_future_chain_thread(aio_future: asyncio.Future, future: ThreadFuture):
    """Chain an asyncio future to a thread future.
    If the result of the asyncio future is another aio future this will also
    be chained so the client only sees thread futures
    """

    def done(done_future: asyncio.Future):
        # Here we're on the aio thread
        # Copy over the future
        try:
            result = done_future.result()
            if asyncio.isfuture(result):
                # Change the aio future to a thread future
                fut = ThreadFuture()
                aio_future_chain_thread(result, fut)
                result = fut

            future.set_result(result)
        except asyncio.CancelledError:
            future.cancel()
        except Exception as exception:  # pylint: disable=broad-except
            future.set_exception(exception)

    aio_future.add_done_callback(done)
    return future


def thread_future_chain_aio(future: ThreadFuture, aio_future: asyncio.Future):
    """Chain a thread future to an asyncio future
    If the reulst of the thread future is another thread future this will also be
    chained so the client only sees aio futures"""
    loop = aio_future._loop  # pylint: disable=protected-access

    def done(done_future: ThreadFuture):
        try:
            result = done_future.result()
            if isinstance(result, ThreadFuture):
                # Change the thread future to an aio future
                fut = loop.create_future()
                thread_future_chain_aio(result, fut)
                result = fut

            loop.call_soon_threadsafe(aio_future.set_result, result)
        except concurrent.futures.CancelledError:
            loop.call_soon_threadsafe(aio_future.cancel)
        except Exception as exception:  # pylint: disable=broad-except
            loop.call_soon_threadsafe(aio_future.set_exception, exception)

    future.add_done_callback(done)
    return aio_future


def aio_future_to_thread(aio_future: asyncio.Future):
    """Convert an asyncio future to a thread future.  Mutations of the thread future will be propagated
    to the asyncio future but not the other way around."""
    future = ThreadFuture()
    thread_future_chain_aio(future, aio_future)
    return future


class LoopScheduler:
    TASK_TIMEOUT = 5.

    def __init__(self, loop: asyncio.AbstractEventLoop = None, name='AsyncioScheduler', timeout=TASK_TIMEOUT):
        self._loop = loop or asyncio.get_event_loop()
        self._name = name
        self._timeout = timeout
        self._asyncio_thread = None
        self._stop_signal = None
        self._closed = False

    def __enter__(self):
        self._ensure_running()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def loop(self):
        return self._loop

    def is_closed(self) -> bool:
        return self._closed

    def is_running(self):
        return self._asyncio_thread is not None

    def close(self):
        if self.is_closed():
            return
        self.stop()
        self._closed = True

    def start(self):
        assert self._asyncio_thread is None, "Already running"

        start_future = ThreadFuture()

        self._asyncio_thread = threading.Thread(target=self._run_loop,
                                                name=self._name,
                                                args=(start_future,),
                                                daemon=True)
        self._asyncio_thread.start()
        start_future.result()

    def stop(self):
        # Save the thread because it will be set to None when it does stop
        aio_thread = self._asyncio_thread
        if aio_thread is None:
            return

        stop_future = ThreadFuture()
        # Send the stop signal
        self._loop.call_soon_threadsafe(partial(self._stop_signal.set_result, stop_future))
        # Wait for the result in case there was an exception
        stop_future.result()
        aio_thread.join()

    def arun(self, coro, *args, **kwargs):
        """
        Run a coroutine on the event loop and return the result.  It may take a little time for the loop
        to get around to scheduling it so we use a timeout as set by the TASK_TIMEOUT class constant.

        :param coro: the coroutine to run
        :return: the result of running the coroutine
        """
        return self.asubmit(coro, *args, **kwargs).result(timeout=self.TASK_TIMEOUT)

    def asubmit(self, coro, *args, **kwargs) -> ThreadFuture:
        """
        Schedule a coroutine on the loop and return the corresponding future
        """
        self._ensure_running()

        future = ThreadFuture()

        def callback():
            # Here we're on the comms thread again
            async def proxy():
                if not future.cancelled():
                    return await coro(*args, **kwargs)

            coro_future = asyncio.ensure_future(proxy(), loop=self._loop)
            aio_future_chain_thread(coro_future, future)

        handle = self._loop.call_soon_threadsafe(callback)

        def handle_cancel(done_future: ThreadFuture):
            """Function to propagate a cancellation of the concurrent future up to the loop callback"""
            if done_future.cancelled():
                self._loop.call_soon_threadsafe(handle.cancel)

        future.add_done_callback(handle_cancel)

        return future

    def run(self, func, *args, **kwargs):
        """
        Run a function on the event loop and return the result.  It may take a little time for the loop
        to get around to scheduling it so we use a timeout as set by the TASK_TIMEOUT class constant.

        :param func: the coroutine to run
        :return: the result of running the coroutine
        """
        return self.submit(func, *args, **kwargs).result(timeout=self.TASK_TIMEOUT)

    def submit(self, func, *args, **kwargs) -> ThreadFuture:
        """
        Schedule a function on the loop and return the corresponding future
        """
        self._ensure_running()

        future = ThreadFuture()

        def callback():
            if not future.cancelled():
                with kiwipy.capture_exceptions(future):
                    result = func(*args, **kwargs)
                    if asyncio.isfuture(result):
                        result = aio_future_to_thread(result)

                    future.set_result(result)

        handle = self._loop.call_soon_threadsafe(callback)

        def handle_cancel(done_future: ThreadFuture):
            """Function to propagate a cancellation of the concurrent future up to the loop callback"""
            if done_future.cancelled():
                self._loop.call_soon_threadsafe(handle.cancel)

        future.add_done_callback(handle_cancel)

        return future

    @contextmanager
    def actx(self, ctx_manager: typing.AsyncContextManager):
        """Can be used to turn an async context manager into a synchronous one"""
        aexit = ctx_manager.__aexit__
        aenter = ctx_manager.__aenter__

        result = self.arun(aenter)
        try:
            yield result
        except Exception:  # pylint: disable=broad-except
            if not self.arun(aexit, *sys.exc_info()):
                raise
        else:
            self.arun(aexit, None, None, None)

    @contextmanager
    def ctx(self, ctx_manager: typing.ContextManager):
        """Can be used to enter a context on the event loop"""
        ctx_exit = ctx_manager.__exit__
        ctx_enter = ctx_manager.__enter__

        result = self.run(ctx_enter)
        try:
            yield result
        except Exception:  # pylint: disable=broad-except
            if not self.run(ctx_exit, *sys.exc_info()):
                raise
        else:
            self.run(ctx_exit, None, None, None)

    def aiter(self, aiterable: typing.AsyncIterable):
        """Iterate an async iterable from this thread"""
        iterator = aiterable.__aiter__()
        running = True
        while running:
            try:
                target = self.arun(iterator.__anext__)
            except StopAsyncIteration:
                running = False
            else:
                yield target

    def _ensure_running(self):
        if self._asyncio_thread is not None:
            return
        self.start()

    def _run_loop(self, start_future):
        """Here we are on the aio thread"""
        _LOGGER.debug('Starting event loop (id %s) on %s', id(self._loop), threading.current_thread())

        asyncio.set_event_loop(self._loop)
        try:
            self._stop_signal = self._loop.create_future()

            async def run_loop():
                start_future.set_result(True)

                # Wait to stop
                stop_future = await self._stop_signal
                stop_future.set_result(True)

            self._loop.run_until_complete(run_loop())

            # The loop is finished
            self._asyncio_thread = None

            _LOGGER.debug('Event loop stopped on %s', threading.current_thread())
        finally:
            asyncio.set_event_loop(None)
