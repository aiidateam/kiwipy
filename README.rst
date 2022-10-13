.. _AiiDA: https://www.aiida.net
.. _rmq tutorial: https://www.rabbitmq.com/getstarted.html
.. _documentation: https://kiwipy.readthedocs.io/en/latest/index.html


kiwiPy
======

.. image:: docs/source/_static/logo.svg
   :height: 64px
   :width: 64px
   :alt: kiwiPy

.. image:: https://codecov.io/gh/aiidateam/kiwipy/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/aiidateam/kiwipy
    :alt: Coveralls

.. image:: https://github.com/aiidateam/kiwipy/workflows/continuous-integration/badge.svg
    :target: https://github.com/aiidateam/kiwipy/actions?query=workflow%3Acontinuous-integration
    :alt: Github Actions

.. image:: https://img.shields.io/pypi/v/kiwipy.svg
    :target: https://pypi.python.org/pypi/kiwipy/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/kiwipy.svg
    :target: https://pypi.python.org/pypi/kiwipy/

.. image:: https://img.shields.io/pypi/l/kiwipy.svg
    :target: https://pypi.python.org/pypi/kiwipy/

.. image:: https://joss.theoj.org/papers/10.21105/joss.02351/status.svg
   :target: https://doi.org/10.21105/joss.02351



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

`(rmq_rpc_client.py source) <https://raw.githubusercontent.com/aiidateam/kiwipy/master/examples/rmq_rpc_client.py>`_


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

`(rmq_rpc_server.py source) <https://raw.githubusercontent.com/aiidateam/kiwipy/master/examples/rmq_rpc_server.py>`_


Worker
------

Create a new task:

.. code-block:: python

    import sys
    import kiwipy

    message = ' '.join(sys.argv[1:]) or "Hello World!"

    with rmq.connect('amqp://localhost') as comm:
        comm.task_send(message)

`(rmq_new_task.py source) <https://raw.githubusercontent.com/aiidateam/kiwipy/master/examples/rmq_new_task.py>`_


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

`(rmq_worker.py source) <https://raw.githubusercontent.com/aiidateam/kiwipy/master/examples/rmq_worker.py>`_

Citing
======

If you use kiwiPy directly or indirectly (e.g. by using `AiiDA`_) then please cite:

Uhrin, M., & Huber, S. P. (2020). kiwiPy : Robust , high-volume , messaging for big-data and computational science workflows, 5, 4–6. http://doi.org/10.21105/joss.02351

This helps us to keep making community software.

Versioning
==========

This software follows `Semantic Versioning`_

Contributing
============

Want a new feature? Found a bug? Want to contribute more documentation or a translation perhaps?

Help is always welcome, get started with the `contributing guide <https://github.com/aiidateam/kiwipy/wiki/Contributing>`__.

.. _Semantic Versioning: http://semver.org/

Development
===========

This package utilises `tox <https://tox.readthedocs.io>`__ for unit test automation, and `pre-commit <https://pre-commit.com>`__ for code style formatting and test automation.

To install these development dependencies:

.. code-block:: bash

    pip install tox pre-commit

To run the unit tests:

.. code-block:: bash

    tox

For the ``rmq`` tests you will require a running instance of RabbitMQ.
One way to achieve this is using Docker and launching ``test/rmq/docker-compose.yml``.

To run the pre-commit tests:

.. code-block:: bash

    pre-commit run --all

To build the documentation:

.. code-block:: bash

    tox -e docs-clean

Changes should be submitted as Pull Requests (PRs) to the ``develop`` branch.

Publishing Releases
===================

1. Create a release PR/commit to the ``develop`` branch, updating ``kiwipy/version.py`` and ``CHANGELOG.md``.
2. Fast-forward merge `develop` into the `master` branch
3. Create a release on GitHub (https://github.com/aiidateam/kiwipy/releases/new), pointing to the release commit on `master`, named ``v.X.Y.Z`` (identical to version in ``kiwipy/version.py``)
4. This will trigger the ``continuous-deployment`` GitHub workflow which, if all tests pass, will publish the package to PyPi. Check this has successfully completed in the GitHub Actions tab (https://github.com/aiidateam/kiwipy/actions).

(if the release fails, delete the release and tag)
