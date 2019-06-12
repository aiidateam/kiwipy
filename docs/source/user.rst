..  _user:

.. _examples:

***************
Usage examples
***************

.. Here the simple examples cover the basics of creating messaging applications using kiwipy over RabbitMQ.

RPC
---
(Some notes on the code)

The client:

.. literalinclude:: ../../examples/rmq_rpc_client.py

The server:

.. literalinclude:: ../../examples/rmq_rpc_server.py

Worker
------
(Some notes on the code)

.. literalinclude:: ../../examples/rmq_new_task.py

And the worker:

.. literalinclude:: ../../examples/rmq_worker.py

Broadcast
---------
(Some notes on the code)

The Client:

.. literalinclude:: ../../examples/rmq_broadcast_client.py

The Server:

.. literalinclude:: ../../examples/rmq_broadcast_server.py

*********************************
Communicator parameters
*********************************

.. The parameters section will focus on some of the important parameters that initialize the communicator and their meaning.

* `connection_params`
* `connection_factory`,
* `loop`,
* `message_exchange`
* `task_exchange`
* `task_queue`
* `task_prefetch_size`
* `task_prefetch_count`
* `encoder`
* `decoder`
* `testing_mode`
