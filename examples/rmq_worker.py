from __future__ import absolute_import
from __future__ import print_function
import time
import threading

from kiwipy import rmq

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(_comm, task):
    print((" [x] Received %r" % task))
    time.sleep(task.count('.'))
    print(" [x] Done")
    return task


with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1/'}) as communicator:
    communicator.add_task_subscriber(callback)
    threading.Event().wait()
