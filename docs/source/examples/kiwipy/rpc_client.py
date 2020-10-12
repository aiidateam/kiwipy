import kiwipy

with kiwipy.connect('amqp://localhost') as comm:
    print(' [x] Requesting fib(30)')
    response = comm.rpc_send('rpc_queue', 30).result()
    print(f' [.] Got {response!r}')
