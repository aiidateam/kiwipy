

# kiwipy

| master | [![Build Status](https://travis-ci.org/muhrin/kiwipy.svg?branch=master)](https://travis-ci.org/muhrin/kiwipy)|
|--|--|
| dev |[![Build Status](https://travis-ci.org/muhrin/kiwipy.svg?branch=develop)](https://travis-ci.org/muhrin/kiwipy)|

kiwipy is a library that makes remote messaging using RabbitMQ (and any other protocol for which a backend is written) EASY.  I don't know about you but I find RabbitMQ HARD.  It's all too easy to make a configuration mistake which is then difficult to debug.  With kiwipy, there's none of this, just messaging, made simple, with all the nice properties and guarantees of AMQP.

Here's what you get:
* RPC
* Broadcast (with filters)
* Task queue messages

Let's dive in, with some examples taken from the [rmq tutorial](https://www.rabbitmq.com/getstarted.html).

## RPC

The client:
```python
from kiwipy.rmq import *

communicator = RmqCommunicator(RmqConnector('amqp://localhost'))

# Send an RPC message
print(" [x] Requesting fib(30)")
response = communicator.rpc_send_and_wait('fib', 30)
print(" [.] Got %r" % response)
```
[(rmq_rpc_client.py source)](https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_rpc_client.py)


The server:
```python
from kiwipy.rmq import *

def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)

communicator = RmqCommunicator(RmqConnector('amqp://localhost'))

# Register an RPC subscriber with the name square
communicator.add_rpc_subscriber(fib, 'fib')
communicator.await()
```
[(rmq_rpc_server.py source)](https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_rpc_server.py)


## Worker

Create a new task:
```python
from kiwipy.rmq import *
import sys

message = ' '.join(sys.argv[1:]) or "Hello World!"

communicator = RmqCommunicator(RmqConnector('amqp://localhost'))
communicator.task_send(message)
communicator.close()
```
[(rmq_new_task.py source)](https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_new_task.py)


And the worker:
```python
from kiwipy.rmq import *
import time

communicator = RmqCommunicator(RmqConnector('amqp://localhost'))

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(task):
    print(" [x] Received %r" % task)
    time.sleep(task.count(b'.'))
    print(" [x] Done")


communicator.add_task_subscriber(callback)
communicator.await()
```
[(rmq_worker.py source)](https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_worker.py)
