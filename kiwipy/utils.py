from __future__ import absolute_import
import logging
import traceback

__all__ = ['EventHelper']

_LOGGER = logging.getLogger(__name__)


class EventHelper(object):  # pylint: disable=useless-object-inheritance

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

        for listener in self.listeners:
            try:
                getattr(listener, event_function.__name__)(*args, **kwargs)
            except Exception:  # pylint: disable=broad-except
                _LOGGER.error("Listener %s produced an exception:\n%s", listener, traceback.format_exc())
