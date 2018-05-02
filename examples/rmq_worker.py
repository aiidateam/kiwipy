from kiwipy.rmq import *
import time

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(task):
    print(" [x] Received %r" % task)
    time.sleep(task.count(b'.'))
    print(" [x] Done")


with RmqCommunicator(RmqConnector('amqp://localhost')) as communicator:
    communicator.add_task_subscriber(callback)
    communicator.await()
