from __future__ import absolute_import
import threading

from kiwipy import rmq


def fib(comm, num):
    if num == 0:
        return 0
    if num == 1:
        return 1
    return fib(comm, num - 1) + fib(comm, num - 2)

def fac(comm, n):
    z=1
    if n>1:
        z=n*fac(comm, n-1)
    return z

try:
    with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1'}) as communicator:
        # Register an RPC subscriber with the name 'fib'
        communicator.add_rpc_subscriber(fib, 'fib')
        # Register an RPC subscriber with the name 'fac'
        communicator.add_rpc_subscriber(fac, 'fac')
        # Now wait indefinitely for fibonacci calls
        threading.Event().wait()
except KeyboardInterrupt:
    pass
