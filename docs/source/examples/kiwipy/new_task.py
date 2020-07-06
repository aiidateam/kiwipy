import kiwipy
import sys

message = ' '.join(sys.argv[1:]) or 'Hello World!'

with kiwipy.connect('amqp://localhost') as comm:
    queue = comm.task_queue('task_queue')  # Durable by default
    queue.task_send(message)
    print(' [x] Sent %r' % message)
