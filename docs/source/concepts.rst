.. _concepts:

Here we introduce the meaning of task, rpc, and broadcast and their usage scenarios, since the meaning of a the task in ``kiwipy`` differs slightly from the one in RabbitMQ's tutorial.

Furthermore, we introduce the concepts of loop-dependent and thread-dependent communicators, which are objects that might be used directly by dependent libraries.

***********************
Task
***********************

The task in kiwipy is bidirectional and the return value can be obtained by the client, which is more like a simple rpc. Thus easy to be confused with rpc mode.

*********
Rpc
*********

One-to-one with name identifier

*********
Broadcast
*********

One sender to multiple subscriber.

****************
Communicator
****************

Loop communicator
==================

Thread communicator
====================
