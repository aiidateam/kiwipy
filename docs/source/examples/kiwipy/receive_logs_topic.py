import sys
import threading

import kiwipy


def callback(_comm, body, _sender, subject, _msg_id):
    print(f' [x] {subject!r}:{body!r}')


with kiwipy.connect('amqp://localhost') as comm:
    binding_keys = sys.argv[1:]
    if not binding_keys:
        sys.stderr.write(f'Usage: {sys.argv[0]} [binding_key]...\n')
        sys.exit(1)

    for binding_key in binding_keys:
        comm.add_broadcast_subscriber(kiwipy.BroadcastFilter(callback, binding_key))

    print(' [*] Waiting for logs. To exit press CTRL+C')
    threading.Event().wait()
