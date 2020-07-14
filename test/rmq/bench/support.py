# -*- coding: utf-8 -*-
import threading

import pika

from kiwipy import rmq


def send_tasks(num_tasks, task_size, queue: rmq.RmqThreadTaskQueue):
    message = b'R' * task_size

    # Send the messages
    for _ in range(num_tasks):
        queue.task_send(message, no_reply=True)


def get_tasks(num_tasks, queue: rmq.RmqThreadTaskQueue):
    counter = [0]
    finished = threading.Event()

    def callback(_comm, _task):
        counter[0] += 1
        if counter[0] == num_tasks:
            finished.set()

    identifier = None
    try:
        identifier = queue.add_task_subscriber(callback)
        finished.wait()
    finally:
        if identifier is not None:
            queue.remove_task_subscriber(identifier)


def clear_all_tasks(queue: rmq.RmqThreadTaskQueue):
    """Just go through all tasks picking them up so the queue is cleared"""
    for task in queue:
        with task.processing() as outcome:
            outcome.set_result(True)


def pika_send_tasks(num_tasks, task_size, channel, queue_name):
    message = b'R' * task_size

    for _ in range(num_tasks):
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
            mandatory=True
        )


def pika_get_tasks(num_tasks, channel, queue_name):
    counter = [0]

    def callback(chan, method, _properties, _body):
        counter[0] += 1
        chan.basic_ack(delivery_tag=method.delivery_tag)
        if counter[0] == num_tasks:
            channel.stop_consuming()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    channel.start_consuming()
