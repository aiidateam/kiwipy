# -*- coding: utf-8 -*-
import threading

import kiwipy


def fib(_comm, num):
    if num == 0:
        return 0
    if num == 1:
        return 1
    return fib(_comm, num - 1) + fib(_comm, num - 2)


def fac(_comm, num):
    result = 1
    if num > 1:
        result = num * fac(_comm, num - 1)
    return result


if __name__ == '__main__':
    try:
        with kiwipy.connect('amqp://127.0.0.1') as comm:
            # Register an RPC subscriber with the name 'fib'
            comm.add_rpc_subscriber(fib, 'fib')
            # Register an RPC subscriber with the name 'fac'
            comm.add_rpc_subscriber(fac, 'fac')
            # Now wait indefinitely for fibonacci calls
            threading.Event().wait()
    except KeyboardInterrupt:
        pass
