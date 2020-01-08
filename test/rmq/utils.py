import shortuuid

import kiwipy
from kiwipy import rmq


async def new_communicator(connection, settings=None) -> kiwipy.rmq.RmqCommunicator:
    settings = settings or {}

    message_exchange = "{}.{}".format(__file__, shortuuid.uuid())
    task_exchange = "{}.{}".format(__file__, shortuuid.uuid())
    task_queue = "{}.{}".format(__file__, shortuuid.uuid())

    communicator = rmq.RmqCommunicator(connection,
                                       message_exchange=message_exchange,
                                       task_exchange=task_exchange,
                                       task_queue=task_queue,
                                       testing_mode=True,
                                       **settings)
    await communicator.connect()
    return communicator
