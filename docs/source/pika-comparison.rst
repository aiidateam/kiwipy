
.. _Pika: https://pika.readthedocs.io/en/stable/
.. _RabbitMQ tutorials: https://www.rabbitmq.com/getstarted.html
.. _work queues: https://www.rabbitmq.com/tutorials/tutorial-two-python.html
.. _topics: https://www.rabbitmq.com/tutorials/tutorial-five-python.html
.. _RPC: https://www.rabbitmq.com/tutorials/tutorial-six-python.html
.. _aio-pika: https://aio-pika.readthedocs.io/en/latest/index.html

Coming from Pika
================

KiwiPy comes natively with a RabbitMQ communicator (others can be added by extending the :py:class:`~kiwipy.Communicator` interface) and thus it may be useful to see how to achieve the same things in `Pika`_ (the standard library used in the `RabbitMQ tutorials`_) and kiwiPy.  This also shows some of the differences, particularly where kiwiPy is less verbose by setting sensible defaults for the user.


Work Queues
-----------

Let's start with RabbitMQ's `work queues`_ example.

Pika
++++

The code for sending a task::

    import pika
    import sys

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)

    message = ' '.join(sys.argv[1:]) or "Hello World!"
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    print(" [x] Sent %r" % message)
    connection.close()

The code for running a worker::

    import pika
    import time

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')


    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        time.sleep(body.count(b'.'))
        print(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)

    channel.start_consuming()


KiwiPy
++++++

And now, in kiwiPy.  The code for sending the same task:

.. literalinclude:: examples/pika/new_task.py

Here, compared to the Pika snippet, we see that we don't have to decide or know about an ``exchange``, ``routing_key``, ``properties``, ``channel`` or durability of the message.  Tasks queues in kiwiPy are always durable and the corresponding messages persistent.  The ``routing_key`` is simply the name of the queue that the user declared and an ``exchange`` is selected automatically for the user (set to ``kiwipy.tasks`` by default).

Here we have explicitly created a task queue called ``task_queue``, however, even this has a default version if the user doesn't need multiple queues.  In which case the code would simply be::

    with kiwipy.connect('amqp://localhost') as comm:
        comm.task_send(message)
        print(" [x] Sent %r" % message)

Now to run a worker it's just:

.. literalinclude:: examples/pika/worker.py

This is fairly straightforward and the only decision the user has to make is about the prefetch count (the number of tasks and worker can be working on simultaneously).


Topics
------

Moving on to the `topics`_ example (known as broadcasts with filters in kiwiPy).

Pika
++++

The Pika code to emit a log topic is::

    import pika
    import sys

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    routing_key = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
    message = ' '.join(sys.argv[2:]) or 'Hello World!'
    channel.basic_publish(
        exchange='topic_logs', routing_key=routing_key, body=message)
    print(" [x] Sent %r:%r" % (routing_key, message))
    connection.close()



And to receive, the code is::

    import pika
    import sys

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    binding_keys = sys.argv[1:]
    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        channel.queue_bind(
            exchange='topic_logs', queue=queue_name, routing_key=binding_key)

    print(' [*] Waiting for logs. To exit press CTRL+C')


    def callback(ch, method, properties, body):
        print(" [x] %r:%r" % (method.routing_key, body))


    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()

KiwiPy
++++++

Emitting in kiwiPy:

.. literalinclude:: examples/pika/emit_log_topic.py

As we've come to see in kiwiPy there's no need to worry about the channel, exchange, exchange type while the routing key is known as the subject of the message.

And to receive the log:

.. literalinclude:: examples/pika/receive_logs_topic.py

Here we can see that the filtering is performed by using a :py:class:`~kiwipy.BroadcastFilter` which can either match a string or a string with a wildcard ``*``.


RPC
---

Finally, let's end on the `RPC`_ example.

Pika
++++

The code to run an RPC server is::


    import pika

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()

    channel.queue_declare(queue='rpc_queue')

    def fib(n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return fib(n - 1) + fib(n - 2)

    def on_request(ch, method, props, body):
        n = int(body)

        print(" [.] fib(%s)" % n)
        response = fib(n)

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = \
                                                             props.correlation_id),
                         body=str(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

    print(" [x] Awaiting RPC requests")
    channel.start_consuming()


The client side is::

    import pika
    import uuid

    class FibonacciRpcClient(object):

        def __init__(self):
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='localhost'))

            self.channel = self.connection.channel()

            result = self.channel.queue_declare(queue='', exclusive=True)
            self.callback_queue = result.method.queue

            self.channel.basic_consume(
                queue=self.callback_queue,
                on_message_callback=self.on_response,
                auto_ack=True)

        def on_response(self, ch, method, props, body):
            if self.corr_id == props.correlation_id:
                self.response = body

        def call(self, n):
            self.response = None
            self.corr_id = str(uuid.uuid4())
            self.channel.basic_publish(
                exchange='',
                routing_key='rpc_queue',
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=self.corr_id,
                ),
                body=str(n))
            while self.response is None:
                self.connection.process_data_events()
            return int(self.response)


    fibonacci_rpc = FibonacciRpcClient()

    print(" [x] Requesting fib(30)")
    response = fibonacci_rpc.call(30)
    print(" [.] Got %r" % response)


KiwiPy
++++++

Now, in kiwiPy the server becomes:

.. literalinclude:: examples/pika/rpc_server.py


As usual, the routing of messages is automatically set up by kiwiPy and the ``identifier`` of the RPC subscriber is the only thing needed to link the client and server.

The client code is:

.. literalinclude:: examples/pika/rpc_client.py

Here, we send the RPC request to the identifier we used before.  The ``rpc_send`` call gives us back a future from which we can get the result once it has been received from the server.


Roundup
-------

As these examples demonstrate kiwiPy tries to use sensible defaults and keep many of the routing details hidden from the user.  This has the advantage of bringing the power of a dedicated message broker such as RabbitMQ to less expert users.  Additionally, it allows messaging to be done using a simple interface which can be implemented by other messaging protocols allowing clients to be protocol agnostic.  The trade-off, of course, is reduced control over the finer detail of how the messaging is performed.  For users that need such fine-grained control, `pika`_ and the excellent `aio-pika`_ may be better alternatives.
