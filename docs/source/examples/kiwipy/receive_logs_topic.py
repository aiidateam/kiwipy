import kiwipy
import sys
import threading


def callback(_comm, body, _sender, subject, _msg_id):
    print(' [x] %r:%r' % (subject, body))


with kiwipy.connect('amqp://localhost') as comm:
    binding_keys = sys.argv[1:]
    if not binding_keys:
        sys.stderr.write('Usage: %s [binding_key]...\n' % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        comm.add_broadcast_subscriber(kiwipy.BroadcastFilter(callback, binding_key))

    print(' [*] Waiting for logs. To exit press CTRL+C')
    threading.Event().wait()
