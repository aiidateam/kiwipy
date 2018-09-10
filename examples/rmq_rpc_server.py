from kiwipy.rmq import *


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


communicator = RmqCommunicator.connect(connection_params={'url': 'amqp://localhost'})

# Register an RPC subscriber with the name square
communicator.add_rpc_subscriber(fib, 'fib')
communicator.await()
