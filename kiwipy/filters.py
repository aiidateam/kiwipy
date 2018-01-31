from past.builtins import basestring
import re
import typing

__all__ = ['BroadcastFilter']


class BroadcastFilter(object):
    def __init__(self, subscriber, subject=None, sender=None):
        self._subscriber = subscriber
        self._subject_filters = []
        self._sender_filters = []
        if subject is not None:
            self.add_subject_filter(subject)
        if sender is not None:
            self.add_sender_filter(sender)

    def __call__(self, body, sender=None, subject=None, correlation_id=None):
        if subject is not None and self._subject_filters and \
                not any([check(subject) for check in self._subject_filters]):
            return

        if sender is not None and self._sender_filters and \
                not any([check(sender) for check in self._sender_filters]):
            return

        self._subscriber(body, sender, subject, correlation_id)

    def add_subject_filter(self, subject_filter):
        self._subject_filters.append(self._ensure_filter(subject_filter))

    def add_sender_filter(self, sender_filter):
        self._sender_filters.append(self._ensure_filter(sender_filter))

    def _ensure_filter(self, filter_value):
        if isinstance(filter_value, basestring):
            return re.compile(filter_value.replace('.', '\.').replace('*', '.*')).match
        elif isinstance(filter_value, typing.re.Pattern):
            return filter_value.match
        else:
            return lambda val: val == filter_value

    def _make_regex(self, filter_str):
        """
        :param filter_str: The filter string
        :type filter_str: basestring
        :return: The regular expression object
        """
        return re.compile(filter_str.replace('.', '\.'))
