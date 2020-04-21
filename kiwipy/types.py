from typing import Callable, Any

import kiwipy  # pylint: disable=unused-import

# RPC subscriber params: communicator, msg
RpcSubscriber = Callable[['kiwipy.Communicator', Any], Any]
# Task subscriber params: communicator, task
TaskSubscriber = Callable[['kiwipy.Communicator', Any], Any]
# Broadcast subscribers params: communicator, body, sender, subject, correlation id
BroadcastSubscriber = Callable[['kiwipy.Communicator', Any, Any, Any, Any], Any]
