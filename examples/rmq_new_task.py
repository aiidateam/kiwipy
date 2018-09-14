from __future__ import absolute_import
from kiwipy.rmq import *
import sys

message = ' '.join(sys.argv[1:]) or "Hello World!"

with RmqCommunicator.connect(connection_params={'url': 'amqp://localhost'}) as communicator:
    communicator.task_send(message)
