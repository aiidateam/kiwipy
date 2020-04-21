.. kiwipy documentation master file, created by
   sphinx-quickstart on Tue May 14 13:41:41 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _kiwiPy: https://github.com/aiidateam/kiwipy
.. _AiiDA: https://www.aiida.net
.. _examples: examples.rst
.. _concepts: concepts.rst
.. _installation: installation.rst
.. _API documentation: apidoc.rst


Welcome to kiwiPy's documentation!
==================================

.. image:: https://codecov.io/gh/aiidateam/kiwipy/branch/develop/graph/badge.svg
    :target: https://codecov.io/gh/aiidateam/kiwipy
    :alt: Coveralls

.. image:: https://travis-ci.org/aiidateam/kiwipy.svg
    :target: https://travis-ci.org/aiidateam/kiwipy
    :alt: Travis CI

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


Features
++++++++

* Support for 1000s of messages per second
* Highly robust (no loss of messages on connection interruptions, etc)
* Generic communicator interface with native support for RabbitMQ
* Supports task queues, broadcasts and RPC
* Support for both thread and coroutine based communication
* Python 3.5+ compatible.


Getting Started
+++++++++++++++

* To install kiwiPy follow the instructions in the `installation`_ section
* After you have successfully installed kiwipy, give in to some of the `examples`_ to see what kiwiPy can do.
* The design concepts behind kiwiPy can be found in `concepts`_ section
* Finally check out the complete `API documentation`_


Table Of Contents
+++++++++++++++++

.. toctree::
   :maxdepth: 2

   installation
   concepts
   examples
   apidoc


Versioning
++++++++++

This software follows `Semantic Versioning`_

.. _Semantic Versioning: http://semver.org/
