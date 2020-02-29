__all__ = 'create_default_communicator', 'get_communicator', 'set_communicator'

# pylint: disable=global-statement,invalid-name

# The global communicator
_communicator = None


def create_default_communicator():
    from . import rmq
    return rmq.connect()


def set_communicator(communicator):
    global _communicator
    _communicator = communicator


def get_communicator():
    global _communicator
    if _communicator is None:
        _communicator = create_default_communicator()
    return _communicator
