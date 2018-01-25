# kiwipy


kiwipy is a library that makes remote messaging EASY.  So far, there is support for:

* RPC
* Broadcast
* Task queue messages

Let's dive in.

## RPC


```python
from kiwipy.rmq import *
communicator = RmqCommunicator(RmqConnector('amqp://localhost'))

def square(x):
    return x * x

# Register an RPC subscriber with the name square
communicator.add_rpc_subscriber(square, 'square')

# Send an RPC message
communicator.rpc_send_and_wait('square', 10)
>>> 100
```
