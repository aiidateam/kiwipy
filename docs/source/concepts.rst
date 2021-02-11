.. _tasks example: examples/tasks.ipynb
.. _rpc example: examples/rpc.ipynb
.. _broadcast example: examples/broadcast.ipynb



Concepts
========

Here we introduce the meaning of task, rpc, and broadcast and their usage scenarios, since the meaning of a the task in
``kiwiPy`` differs slightly from the one in RabbitMQ's tutorial.

Task
----

Tasks are one to many messages.  This means that you sent out a task to a queue and there can be zero or more workers
attached one of which will process the task when it is ready.  The result of the task can optionally be delivered to the
sender.  See the `tasks example`_ to see this in action.

RPC
---

A remote procedure is one-to-one.  This is used when you want to call a particular remote function/method and (usually)
expect an immediate response. For example imagine asking a remote process to pause itself.  Here you would make a RPC
and wait to get confirmation that it has, indeed, paused.  See the `rpc example`_ to see this in action.


Broadcast
---------

One sender to zero or more consumers.  These are fire and forget calls that broadcast a message to anyone who is
listening.  Consumers may optionally apply a filter to only receive messages that match some criteria.
See the `broadcast example`_ to see this in action.


Coroutines vs Threads
=====================

KiwiPy's RabbitMQ backend has both coroutine and threaded version of the Communicator.  If you're in a coroutine
environment you may prefer to use the :class:`~kiwipy.rmq.communicator.RmqCommunicator` which uses coroutines and
:class:`asyncio.futures.Future` s while if you don't have an event loop just stick with the :class:`~kiwipy.rmq.threadcomms.RmqThreadCommunicator` which
runs an event loop in a separate thread to allow it to do asynchronous communication while your application logic gets on
with other stuff.

If all of this means nothing to you then stick with the :class:`~kiwipy.rmq.threadcomms.RmqThreadCommunicator`.
