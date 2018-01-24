from . import communications

__all__ = ['LocalCommunicator']


class LocalCommunicator(communications.CommunicatorHelper):
    def task_send(self, msg):
        return self.fire_task(msg)

    def rpc_send(self, recipient_id, msg):
        return self.fire_rpc(recipient_id, msg)

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        return self.fire_broadcast(body, sender, subject, correlation_id)

    def await_response(self, future):
        return future.result()
