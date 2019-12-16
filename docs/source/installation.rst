.. _installation:

************
Installation
************

RabbitMQ
-------------

kiwipy depends on RabbitMQ as the message broker.

To install RabbitMQ, refer to `Downloading and Installing RabbitMQ <https://www.rabbitmq.com/download.html>`_

Basic Installation
------------------

    $ pip install kiwipy[rmq]

Now you're ready to run :ref:`examples <examples>`!

Building from Source
--------------------

In order to develop kiwipy it's best to install kiwipy in ``editable`` mode. This allows changes you
make to kiwipy to be reflected immediately in your runtime environment.

First, clone the source::

   $ git clone https://github.com/aiidateam/kiwipy.git

Then, create and activate a virtualenv::

   $ virtualenv venv
   $ . venv/bin/activate
   $ pip install -e "kiwipy[rmq,dev,docs]"
