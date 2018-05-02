from kiwipy.rmq import *
import sys

message = ' '.join(sys.argv[1:]) or "Hello World!"

with RmqCommunicator(RmqConnector('amqp://localhost')) as communicator:
    communicator.task_send(message)
