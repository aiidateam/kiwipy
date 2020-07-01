import kiwipy
import sys

message = ' '.join(sys.argv[2:]) or 'Hello World!'

with kiwipy.connect('amqp://localhost') as comm:
    subject = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
    comm.broadcast_send(message, subject=subject)
    print(' [x] Sent %r:%r' % (subject, message))
