import logging
import traceback

__all__ = ['EventHelper']

_LOGGER = logging.getLogger(__name__)


class EventHelper(object):
    def __init__(self, listener_type):
        assert listener_type is not None, "Must provide valid listener type"

        self._listener_type = listener_type
        self._listeners = set()

    def add_listener(self, listener):
        assert isinstance(listener, self._listener_type), "Listener is not of right type"
        self._listeners.add(listener)

    def remove_listener(self, listener):
        self._listeners.discard(listener)

    def remove_all_listeners(self):
        self._listeners.clear()

    @property
    def listeners(self):
        return self._listeners

    def fire_event(self, event_function, *args, **kwargs):
        if event_function is None:
            raise ValueError("Must provide valid event method")

        for l in self.listeners:
            try:
                getattr(l, event_function.__name__)(*args, **kwargs)
            except Exception :
                _LOGGER.error(
                    "Listener {} produced an exception:\n{}".format(l, traceback.format_exc()))


