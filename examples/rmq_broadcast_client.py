# -*- coding: utf-8 -*-
import sys

import kiwipy

# pylint: disable=invalid-name

body = ' '.join(sys.argv[1:]) or '___'

with kiwipy.connect('amqp://127.0.0.1') as comm:
    # send message with different sender and subject

    # listen by two subscriber
    sendr = 'bob.jones'
    subj = 'purchase.car'
    comm.broadcast_send(body, sender=sendr, subject=subj)

    # Filtered by filter subscriber because subject not matched with "purchase.*" pattern
    # Therefore filterd.
    sendr = 'bob.jones'
    subj = 'sell.car'
    comm.broadcast_send(body, sender=sendr, subject=subj)
