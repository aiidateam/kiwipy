.. _AiiDA: https://www.aiida.net
.. _rmq tutorial: https://www.rabbitmq.com/getstarted.html
.. _documentation: https://kiwipy.readthedocs.io/en/latest/index.html


kiwiPy
======

.. image:: docs/source/_static/logo.svg
   :height: 64px
   :width: 64px
   :alt: kiwiPy

.. image:: https://travis-ci.org/aiidateam/kiwipy.svg
    :target: https://travis-ci.org/aiidateam/kiwipy
    :alt: Travis CI

.. image:: https://readthedocs.org/projects/kiwipy/badge
    :target: http://kiwipy.readthedocs.io/
    :alt: Docs status


.. image:: https://img.shields.io/pypi/v/kiwipy.svg
    :target: https://pypi.python.org/pypi/kiwipy/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/kiwipy.svg
    :target: https://pypi.python.org/pypi/kiwipy/

.. image:: https://img.shields.io/pypi/pyversions/kiwipy.svg
    :target: https://pypi.python.org/pypi/kiwipy/

.. image:: https://img.shields.io/pypi/l/kiwipy.svg
    :target: https://pypi.python.org/pypi/kiwipy/


`kiwiPy`_ is a library that makes remote messaging using RabbitMQ (and possibly other message brokers) EASY.  It was
designed to support high-throughput workflows in big-data and computational science settings and is currently used
by `AiiDA`_ for computational materials research around the world.  That said, kiwiPy is entirely general and can
be used anywhere where high-throughput and robust messaging are needed.

Here's what you get:

* RPC
* Broadcast (with filters)
* Task queue messages

Let's dive in, with some examples taken from the `rmq tutorial`_.  To see more detail head over to the `documentation`_.

RPC
---

The client:

.. code-block:: python

    import kiwipy

    with kiwipy.connect('amqp://localhost') as comm:
        # Send an RPC message
        print(" [x] Requesting fib(30)")
        response = comm.rpc_send('fib', 30).result()
        print((" [.] Got %r" % response))

`(rmq_rpc_client.py source) <https://raw.githubusercontent.com/aiidateam/kiwipy/develop/examples/rmq_rpc_client.py>`_


The server:

.. code-block:: python

    import threading
    import kiwipy

    def fib(comm, num):
        if num == 0:
            return 0
        if num == 1:
            return 1

        return fib(comm, num - 1) + fib(comm, num - 2)

    with kiwipy.connect('amqp://127.0.0.1') as comm:
        # Register an RPC subscriber with the name 'fib'
        comm.add_rpc_subscriber(fib, 'fib')
        # Now wait indefinitely for fibonacci calls
        threading.Event().wait()

`(rmq_rpc_server.py source) <https://raw.githubusercontent.com/aiidateam/kiwipy/develop/examples/rmq_rpc_server.py>`_


Worker
------

Create a new task:

.. code-block:: python

    import sys
    import kiwipy

    message = ' '.join(sys.argv[1:]) or "Hello World!"

    with rmq.connect('amqp://localhost') as comm:
        comm.task_send(message)

`(rmq_new_task.py source) <https://raw.githubusercontent.com/aiidateam/kiwipy/develop/examples/rmq_new_task.py>`_


And the worker:

.. code-block:: python

    import time
    import threading
    import kiwipy

    print(' [*] Waiting for messages. To exit press CTRL+C')


    def callback(_comm, task):
        print((" [x] Received %r" % task))
        time.sleep(task.count(b'.'))
        print(" [x] Done")


    try:
        with kiwipy.connect('amqp://localhost') as comm:
            comm.add_task_subscriber(callback)
            threading.Event().wait()
    except KeyboardInterrupt:
        pass

`(rmq_worker.py source) <https://raw.githubusercontent.com/aiidateam/kiwipy/develop/examples/rmq_worker.py>`_


Versioning
==========

This software follows `Semantic Versioning`_



.. _Semantic Versioning: http://semver.org/

