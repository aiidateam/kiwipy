# -*- coding: utf-8 -*-
import kiwipy

# pylint: disable=invalid-name

with kiwipy.connect('amqp://127.0.0.1') as comm:
    # Send an RPC message with identifier 'fib'
    print(' [x] Requesting fib(30)')
    response = comm.rpc_send('fib', 30).result()
    print(f' [.] Got {response!r}')

    # Send an RPC message with identifier 'fac'
    print(' [x] Requesting fac(3)')
    response = comm.rpc_send('fac', 3).result()
    print(f' [.] Got {response!r}')
