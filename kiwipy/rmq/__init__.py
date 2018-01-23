try:
    import pika
except ImportError:
    pika = None

if pika:
    # We have to have pika, otherwise disable

    from .communicator import *
    from .tasks import *
    from .loops import *
    from .pubsub import *

    __all__ = (tasks.__all__ + loops.__all__ +
               pubsub.__all__ + communicator.__all__)
