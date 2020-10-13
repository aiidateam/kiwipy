# -*- coding: utf-8 -*-
import time
import threading

import kiwipy

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(_comm, task):
    print(f' [x] Received {task!r}')
    time.sleep(task.count('.'))
    print(' [x] Done')
    return task


with kiwipy.connect('amqp://127.0.0.1/') as communicator:
    communicator.add_task_subscriber(callback)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
