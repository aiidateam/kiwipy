
.. _examples: examples.rst


Installation
============

Python
------

KiwiPy supports Python versions 3.8 and above.

RabbitMQ
--------

KiwiPy depends on RabbitMQ as the message broker.
On Ubuntu this is as simple as:

.. code-block:: shell

    apt install rabbitmq

For other platforms refer to `Downloading and Installing RabbitMQ <https://www.rabbitmq.com/download.html>`_

Basic Installation
------------------

    $ pip install kiwipy[rmq]

Now you're ready to run `examples`_!

Building from Source
--------------------

In order to develop kiwipy it's best to install kiwipy in ``editable`` mode. This allows changes you
make to kiwipy to be reflected immediately in your runtime environment.

First, clone the source:

.. code-block:: shell

   $ git clone https://github.com/aiidateam/kiwipy.git

Then, create and activate a virtualenv:

.. code-block:: shell

    virtualenv venv
    . venv/bin/activate
    pip install -e "kiwipy[rmq,pre-commit,tests]"

To run the tests, make sure the RabbitMQ server is up and running (see the RabbitMQ documentation on how to accomplish and/or verify this) and type:

.. code-block:: shell

    pytest test
