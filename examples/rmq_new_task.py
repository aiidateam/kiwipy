from __future__ import absolute_import
import sys

from kiwipy import rmq

# pylint: disable=invalid-name

message = ' '.join(sys.argv[1:]) or "Hello World!"

with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://localhost'}) as communicator:
    communicator.task_send(message)
