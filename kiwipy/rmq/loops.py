from tornado import ioloop

import kiwipy

__all__ = ['new_event_loop', 'set_event_loop', 'get_event_loop']

get_event_loop = ioloop.IOLoop.current
new_event_loop = ioloop.IOLoop


def set_event_loop(loop):
    if loop is None:
        ioloop.IOLoop.clear_instance()
    else:
        loop.make_current()
