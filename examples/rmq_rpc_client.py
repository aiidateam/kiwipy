from kiwipy import rmq

# pylint: disable=invalid-name

comm = rmq.RmqThreadCommunicator.connect('amqp://127.0.0.1')

# Send an RPC message with identifier 'fib'
print(" [x] Requesting fib(30)")
response = comm.rpc_send('fib', 30).result()
print((" [.] Got %r" % response))

# Send an RPC message with identifier 'fac'
print(" [x] Requesting fac(3)")
response = comm.rpc_send('fac', 3).result()
print((" [.] Got %r" % response))
