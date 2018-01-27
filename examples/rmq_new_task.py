from kiwipy.rmq import *
import sys

message = ' '.join(sys.argv[1:]) or "Hello World!"

communicator = RmqCommunicator(RmqConnector('amqp://localhost'))
communicator.task_send(message)
communicator.close()
