# -*- coding: utf-8 -*-
import re
import typing

__all__ = ('BroadcastFilter',)


class BroadcastFilter:
    """A filter that can be used to limit the subjects and/or senders that will be received"""

    def __init__(self, subscriber, subject=None, sender=None):
        self._subscriber = subscriber
        self._subject_filters = []
        self._sender_filters = []
        if subject is not None:
            self.add_subject_filter(subject)
        if sender is not None:
            self.add_sender_filter(sender)

    @property
    def __name__(self):
        return 'BroadcastFilter'

    def __call__(self, communicator, body, sender=None, subject=None, correlation_id=None):
        if self.is_filtered(sender, subject):
            return None
        return self._subscriber(communicator, body, sender, subject, correlation_id)

    def is_filtered(self, sender, subject) -> bool:
        if subject is not None and self._subject_filters and \
                not any(check(subject) for check in self._subject_filters):
            return True

        if sender is not None and self._sender_filters and \
                not any(check(sender) for check in self._sender_filters):
            return True

        return False

    def add_subject_filter(self, subject_filter):
        self._subject_filters.append(self._ensure_filter(subject_filter))

    def add_sender_filter(self, sender_filter):
        self._sender_filters.append(self._ensure_filter(sender_filter))

    @classmethod
    def _ensure_filter(cls, filter_value):
        if isinstance(filter_value, str):
            return re.compile(filter_value.replace('.', '[.]').replace('*', '.*')).match
        if isinstance(filter_value, typing.Pattern):  # pylint: disable=isinstance-second-argument-not-valid-type
            return filter_value.match

        return lambda val: val == filter_value

    @classmethod
    def _make_regex(cls, filter_str):
        """
        :param filter_str: The filter string
        :type filter_str: str
        :return: The regular expression object
        """
        return re.compile(filter_str.replace('.', '[.]'))
