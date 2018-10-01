from __future__ import absolute_import
from __future__ import print_function
from kiwipy import rmq

# pylint: disable=invalid-name

communicator = rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://localhost'})

# Send an RPC message
print(" [x] Requesting fib(30)")
response = communicator.rpc_send('fib', 30).result()
print((" [.] Got %r" % response))
