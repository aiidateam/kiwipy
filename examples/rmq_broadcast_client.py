import sys

from kiwipy import rmq

# pylint: disable=invalid-name

body = ' '.join(sys.argv[1:]) or "___"

with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1'}) as communicator:
    # send message with different sender and subject

    # listen by two subscriber
    sendr = 'bob.jones'
    subj = 'purchase.car'
    communicator.broadcast_send(body, sender=sendr, subject=subj)

    # Filtered by filter subscriber because subject not matched with "purchase.*" pattern
    # Therefore filterd.
    sendr = 'bob.jones'
    subj = 'sell.car'
    communicator.broadcast_send(body, sender=sendr, subject=subj)
