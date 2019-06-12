from __future__ import absolute_import
from __future__ import print_function
import threading

import kiwipy
from kiwipy import rmq

def on_broadcast_send(comm, body, sender, subject, correlation_id):
    print(" [x] listening on_broadcast_send:")
    print(" body: {}, sender {}, subject {}\n".format(body, sender, subject))

def on_broadcast_filter(comm, body, sender=None, subject=None, correlation_id=None):
    print(" [x] listening on_broadcast_filter:")
    print(" body: {}, sender {}, subject {}\n".format(body, sender, subject))
filtered = kiwipy.BroadcastFilter(on_broadcast_filter)
filtered.add_subject_filter("purchase.*")

try:
    with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1'}) as communicator:
        # Register a broadcast subscriber
        communicator.add_broadcast_subscriber(on_broadcast_send)
        # Register a broadcast subscriber
        communicator.add_broadcast_subscriber(filtered)
        # Now wait indefinitely for fibonacci calls
        threading.Event().wait()
except KeyboardInterrupt:
    pass
