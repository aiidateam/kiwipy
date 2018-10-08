from __future__ import absolute_import

from . import communications

__all__ = ['LocalCommunicator']


class LocalCommunicator(communications.CommunicatorHelper):

    def task_send(self, task, no_reply=False):
        return self.fire_task(task, no_reply)

    def rpc_send(self, recipient_id, msg):
        return self.fire_rpc(recipient_id, msg)

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        return self.fire_broadcast(body, sender=sender, subject=subject, correlation_id=correlation_id)
