from tornado import ioloop

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
