

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
from kiwipy import rmq

communicator = rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://localhost'})

# Send an RPC message
print(" [x] Requesting fib(30)")
response = communicator.rpc_send('fib', 30).result()
print((" [.] Got %r" % response))
```
[(rmq_rpc_client.py source)](https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_rpc_client.py)


The server:
```python
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
# Now wait indefinitely for fibonacci calls
threading.Event().wait()
```
[(rmq_rpc_server.py source)](https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_rpc_server.py)


## Worker

Create a new task:
```python
import sys

from kiwipy import rmq

message = ' '.join(sys.argv[1:]) or "Hello World!"

with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://localhost'}) as communicator:
    communicator.task_send(message)
```
[(rmq_new_task.py source)](https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_new_task.py)


And the worker:
```python
import time
import threading

from kiwipy import rmq

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(_comm, task):
    print((" [x] Received %r" % task))
    time.sleep(task.count(b'.'))
    print(" [x] Done")


try:
    with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://localhost'}) as communicator:
        communicator.add_task_subscriber(callback)
        threading.Event().wait()
except KeyboardInterrupt:
    pass
```
[(rmq_worker.py source)](https://raw.githubusercontent.com/muhrin/kiwipy/develop/examples/rmq_worker.py)
