.. kiwipy documentation master file, created by
   sphinx-quickstart on Tue May 14 13:41:41 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _kiwiPy: https://github.com/aiidateam/kiwipy
.. _AiiDA: https://www.aiida.net
.. _Pika: https://pika.readthedocs.io/en/stable/
.. _examples: examples.rst
.. _concepts: concepts.rst
.. _coming from pika: pika-comparison.rst
.. _installation: installation.rst
.. _performance: performance.rst
.. _API documentation: apidoc.rst


Welcome to kiwiPy's documentation!
==================================

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


Features
++++++++

* Support for `1000s of messages per second <performance_>`__
* Highly robust - no loss of messages on connection interruptions, etc., as messages are automatically persisted to disk
* Generic communicator interface with native support for RabbitMQ
* Supports task queues, broadcasts and RPC
* Support for both thread and coroutine based communication
* Python 3.8+ compatible.


Getting Started
+++++++++++++++

* To install kiwiPy follow the instructions in the `installation`_ section
* After you have successfully installed kiwipy, give in to some of the `examples`_ to see what kiwiPy can do.
* The design concepts behind kiwiPy can be found in `concepts`_ section
* If you're already familiar with `Pika`_ you might find the `coming from pika`_ section useful
* Finally check out the complete `API documentation`_

.. admonition:: Development Contributions
   :class: note

   Want a new feature? Found a bug? Want to contribute more documentation or a translation perhaps?
   Help is always welcome, get started with the `contributing guide <https://github.com/aiidateam/kiwipy/wiki/Contributing>`__.



Table Of Contents
+++++++++++++++++

.. toctree::
   :maxdepth: 2

   installation
   concepts
   examples
   pika-comparison
   performance
   API Reference <apidoc/kiwipy>


Citing
++++++

If you use kiwiPy directly or indirectly (e.g. by using `AiiDA`_) then please cite:

Uhrin, M., & Huber, S. P. (2020). kiwiPy : Robust , high-volume , messaging for big-data and computational science workflows, 5, 4–6. http://doi.org/10.21105/joss.02351

This helps us to keep making community software.

Versioning
++++++++++

This software follows `Semantic Versioning`_

.. _Semantic Versioning: http://semver.org/
