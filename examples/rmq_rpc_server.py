from __future__ import absolute_import
import threading

from kiwipy import rmq


def fib(comm, num):
    if num == 0:
        return 0
    if num == 1:
        return 1

    return fib(comm, num - 1) + fib(comm, num - 2)


communicator = rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://localhost'})  # pylint: disable=invalid-name

# Register an RPC subscriber with the name square
communicator.add_rpc_subscriber(fib, 'fib')
threading.Event().wait()
