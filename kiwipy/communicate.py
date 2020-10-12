# -*- coding: utf-8 -*-
__all__ = ('connect',)

DEFAULT_COMM_URI = 'amqp://guest:guest@127.0.0.1/'


def connect(uri: str = DEFAULT_COMM_URI, **kwargs):
    """Create a connection using a URI"""
    if uri.startswith('amqp'):
        from . import rmq  # Avoid circular ref: pylint: disable=import-outside-toplevel
        return rmq.connect(connection_params=uri, **kwargs)

    raise ValueError(f"Uknown communicator uri '{uri}'")
