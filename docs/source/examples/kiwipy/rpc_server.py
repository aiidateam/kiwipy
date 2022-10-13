import threading

import kiwipy


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def on_request(_comm, body):
    n = int(body)

    print(f' [.] fib({n})')
    return fib(n)


with kiwipy.connect('amqp://localhost') as comm:
    comm.add_rpc_subscriber(on_request, identifier='rpc_queue')
    print(' [x] Awaiting RPC requests')
    threading.Event().wait()
