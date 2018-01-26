from tornado import ioloop

import kiwipy

__all__ = ['new_event_loop', 'set_event_loop', 'get_event_loop', 'run_until_complete']

get_event_loop = ioloop.IOLoop.current
new_event_loop = ioloop.IOLoop


def set_event_loop(loop):
    if loop is None:
        ioloop.IOLoop.clear_instance()
    else:
        loop.make_current()


def run_until_complete(future, loop=None):
    if loop is None:
        loop = get_event_loop()

    def _stop(future):
        loop.stop()

    future.add_done_callback(_stop)
    loop.start()
    return future.result()


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
