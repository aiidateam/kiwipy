from __future__ import absolute_import
from functools import partial
import yaml

# Times are in milliseconds

TASK_EXCHANGE = 'kiwipy.tasks'
TASK_QUEUE = 'kiwipy.tasks'
MESSAGE_EXCHANGE = 'kiwipy.messages'
BROADCAST_TOPIC = '[broadcast]'
RPC_TOPIC = '[rpc]'
MESSAGE_TTL = 60 * 1000  # One minute
TEST_QUEUE_EXPIRES = 10 * 1000
QUEUE_EXPIRES = 60 * 1000
REPLY_QUEUE_EXPIRES = 60 * 1000
TASK_MESSAGE_TTL = 60000 * 60 * 24 * 7  # One week
TASK_PREFETCH_SIZE = 0
TASK_PREFETCH_COUNT = 0

ENCODER = partial(yaml.dump, encoding='utf-8')
DECODER = yaml.load
