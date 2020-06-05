# -*- coding: utf-8 -*-
import sys

import kiwipy

# pylint: disable=invalid-name

message = ' '.join(sys.argv[1:]) or 'Hello World!'

with kiwipy.connect('amqp://127.0.0.1') as comm:
    result = comm.task_send(message).result(timeout=5.0)
    print(result)
