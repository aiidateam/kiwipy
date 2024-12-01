
.. _Pika: https://pika.readthedocs.io/en/stable/
.. _RabbitMQ tutorials: https://www.rabbitmq.com/getstarted.html
.. _work queues: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
.. _topics: https://www.rabbitmq.com/tutorials/tutorial-five-python.html
.. _RPC: https://www.rabbitmq.com/tutorials/tutorial-six-python.html
.. _aio-pika: https://docs.aio-pika.com/index.html

Coming from Pika
================

KiwiPy comes natively with a RabbitMQ communicator (others can be added by extending the :py:class:`~kiwipy.Communicator` interface) and thus it may be useful to see how to achieve the same things in `Pika`_ (the standard library used in the `RabbitMQ tutorials`_) and kiwiPy.  This also shows some of the differences, particularly where kiwiPy is less verbose by setting sensible defaults for the user.


Work Queues
-----------

Let's start with RabbitMQ's `work queues`_ example.


Pika
++++

The code for sending a task:

.. literalinclude:: examples/pika/new_task.py
    :language: python


The code for running a worker:

.. literalinclude:: examples/pika/worker.py
    :language: python


KiwiPy
++++++

And now, in kiwiPy.  The code for sending the same task:

.. literalinclude:: examples/kiwipy/new_task.py
    :language: python

Here, compared to the Pika snippet, we see that we don't have to decide or know about an ``exchange``, ``routing_key``, ``properties``, ``channel`` or durability of the message.
Task queues in kiwiPy are always durable and the corresponding messages persistent.
The ``routing_key`` is simply the name of the queue that the user declared and an ``exchange`` is selected automatically for the user (set to ``kiwipy.tasks`` by default).
The ``exchange`` and some other defaults can be changed when constructing a :py:class:`~kiwipy.rmq.threadcomms.RmqThreadCommunicator`.

Here we have explicitly created a task queue called ``task_queue``, however, even this has a default version if the user doesn't need multiple queues.  In which case the code would simply be::

    with kiwipy.connect('amqp://localhost') as comm:
        comm.task_send(message)
        print(" [x] Sent %r" % message)

Now to run a worker it's just:

.. literalinclude:: examples/kiwipy/worker.py
    :language: python

This is fairly straightforward and the only decision the user has to make is about the prefetch count (the number of tasks and worker can be working on simultaneously).


Topics
------

Moving on to the `topics`_ example (known as broadcasts with filters in kiwiPy).


Pika
++++

The Pika code to emit a log topic is:

.. literalinclude:: examples/pika/emit_log_topic.py
    :language: python

And to receive, the code is:

.. literalinclude:: examples/pika/receive_logs_topic.py
    :language: python


KiwiPy
++++++

Emitting in kiwiPy:

.. literalinclude:: examples/kiwipy/emit_log_topic.py
    :language: python

As we've come to see in kiwiPy there's no need to worry about the channel, exchange, exchange type while the routing key is known as the subject of the message.

And to receive the log:

.. literalinclude:: examples/kiwipy/receive_logs_topic.py
    :language: python

Here we can see that the filtering is performed by using a :py:class:`~kiwipy.BroadcastFilter` which can match a string, that optionally includes wildcards ``*``.


RPC
---

Finally, let's end with the `RPC`_ example.


Pika
++++

The code to run an RPC server is:

.. literalinclude:: examples/pika/rpc_server.py
    :language: python


The client side is:

.. literalinclude:: examples/pika/rpc_client.py
    :language: python


KiwiPy
++++++

Now, in kiwiPy the server becomes:

.. literalinclude:: examples/kiwipy/rpc_server.py
    :language: python

As usual, the routing of messages is automatically set up by kiwiPy and the ``identifier`` of the RPC subscriber is the only thing needed to link the client and server.

The client code is:

.. literalinclude:: examples/kiwipy/rpc_client.py
    :language: python

Here, we send the RPC request to the identifier we used before.  The ``rpc_send`` call gives us back a future from which we can get the result once it has been received from the server.


Roundup
-------

As these examples demonstrate, kiwiPy tries to use sensible defaults and keep many of the routing details hidden from the user.
This has the advantage of bringing the power of a dedicated message broker such as RabbitMQ to less expert users.
Additionally, it allows messaging to be done using a simple interface which can be implemented by other messaging protocols, allowing clients to be protocol agnostic.  The trade-off, of course, is reduced control over the finer details of how the messaging is performed.
For users that need such fine-grained control, `pika`_ and the excellent `aio-pika`_ may be better alternatives.
