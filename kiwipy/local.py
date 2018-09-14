from __future__ import absolute_import
import threading
import time

from . import communications
from . import futures

__all__ = ['LocalCommunicator']


class LocalCommunicator(communications.CommunicatorHelper):

    def task_send(self, msg):
        return self.fire_task(msg)

    def rpc_send(self, recipient_id, msg):
        return self.fire_rpc(recipient_id, msg)

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        return self.fire_broadcast(body, sender=sender, subject=subject, correlation_id=correlation_id)

    def wait_for(self, future, timeout=None):
        event = threading.Event()
        future.add_done_callback(lambda _: event.set())

        if not event.wait(timeout):
            raise communications.TimeoutError("Timed out waiting for result")
