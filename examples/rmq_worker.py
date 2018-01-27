from kiwipy.rmq import *
import time

communicator = RmqCommunicator(RmqConnector('amqp://localhost'))

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(task):
    print(" [x] Received %r" % task)
    time.sleep(task.count(b'.'))
    print(" [x] Done")


communicator.add_task_subscriber(callback)
communicator.await()
