.. _concepts:

Here I preparing to firstly introduce the differences between task, rpc, and broadcast and their usage scenario, because the task in kiwipy is slightly different from the task defined in RabbitMQ's tutorial. The task in kiwipy is bidirectional and the return value can be obtained in client, which is more like a simple rpc. Thus easy to be confused with rpc mode.

Secondly, I prepare to introduce the concepts of loop dependent communicator and thread dependent communicator, which are objects might be used by other library directly.

***********************
Task
***********************

One-to-one

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
