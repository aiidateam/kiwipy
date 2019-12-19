import concurrent.futures

__all__ = [
    'RemoteException', 'DeliveryFailed', 'TaskRejected', 'UnroutableError', 'TimeoutError',
    'DuplicateSubscriberIdentifier', 'CommunicatorClosed'
]


class RemoteException(Exception):
    """An exception occurred at the remote end of the call """


class DeliveryFailed(Exception):
    """Failed to deliver a message """


class UnroutableError(DeliveryFailed):
    """The messages was unroutable """


class TaskRejected(Exception):
    """ A task was rejected at the remote end """


class DuplicateSubscriberIdentifier(Exception):
    """Failed to add a subscriber because the identifier supplied is already in use"""


class CommunicatorClosed(Exception):
    """Raised when an operation is attempted on a closed communicator"""


TimeoutError = concurrent.futures.TimeoutError  # pylint: disable=redefined-builtin
