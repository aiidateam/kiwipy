.. _pika: https://pika.readthedocs.io/en/stable/
.. _aio-pika: https://docs.aio-pika.com/
.. _msgpack: https://github.com/msgpack/msgpack-python


Performance
===========

KiwiPy is designed primarily with ease-of-use and robustness in mind with less of a focus on performance.
Never the less, it is expected that depending on the configuration and network conditions kiwPy should be able to handle hundreds to thousands of messages per second.

The chart below shows benchmarks comparing kiwiPy to `pika`_ using the following configuration:

* Sending 100 task messages consisting of 32 to 2048 bytes in size (default 1024)
* Messages are sent and _then_ received (i.e. serialised) to be able to show split timings
* Everything running locally (server and client) on Dell XPS 15 (Core i9-9980H @ 2.3 x 16, 1TB SSD HDD), Ubuntu 20.04
* Using `msgpack`_ for encoding and decoding of messages
* Mandatory routing enabled meaning that the code will wait for messages to be routed by the broker (or raise an exception)
* Full code can be found in ``test/rmq/bench/test_benchmarks.py``

Tests are split into ``send``, ``get`` and ``send_get`` to show a breakdown of the full process of sending and receiving a message.

The ``send_get`` test is carried out for different message size (in bytes) as indicated in square brackets.


.. image:: /_static/bench.svg
  :width: 640
  :alt: Performance comparison of kiwiPy vs pika


As we can see, kiwiPy is faster in getting tasks (``test_kiwi_get``: 21.7 ms vs ``test_pika__get``: 50.6 ms) but significantly slower in sending them out (``test_kiwi_send``: 237.7 ms vs ``test_pika_send``: 31.5 ms) and this results in overall (send/get) times that are roughly 3.5x (or 200ms) slower than that of pika.
This may be due to the fact that kiwiPy is running a separate thread to perform all communications meaning that when a client makes a request to send a message (from the main thread) kiwiPy synchronises with the communications thread and waits until the message has been send before returning control the user.  This makes it easier to write correct code when using kiwiPy as any errors encountered in sending will be raised immediately as exceptions, as opposed to the user having to explicitly check a future.  Incoming messages, on the other hand, are delivered directly on the communications thread and it is the users responsibility to synchronise with their main thread if they need, hence we see no performance loss.

To run these benchmarks yourself, just clone the source, then install and run like so

.. code-block:: shell

    pip install -e .[rmq,tests]
    pytest test


At the end of the tests you should see a summary of all the timings.
