from __future__ import absolute_import
from __future__ import print_function
from kiwipy import rmq
import time

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(_comm, task):
    print((" [x] Received %r" % task))
    time.sleep(task.count(b'.'))
    print(" [x] Done")


with rmq.RmqCommunicator.connect(connection_params={'url': 'amqp://localhost'}) as communicator:
    communicator.add_task_subscriber(callback)
    communicator.await()
