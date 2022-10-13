import threading
import time

import kiwipy


def callback(_comm, body):
    print(f' [x] Received {body!r}')
    time.sleep(body.count('.'))
    print(' [x] Done')
    return True


with kiwipy.connect('amqp://localhost') as comm:
    queue = comm.task_queue('task_queue', prefetch_count=1)
    queue.add_task_subscriber(callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    threading.Event().wait()  # Wait for incoming messages
