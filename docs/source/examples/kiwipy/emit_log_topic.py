import sys

import kiwipy

message = ' '.join(sys.argv[2:]) or 'Hello World!'

with kiwipy.connect('amqp://localhost') as comm:
    subject = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
    comm.broadcast_send(message, subject=subject)
    print(f' [x] Sent {subject!r}:{message!r}')
