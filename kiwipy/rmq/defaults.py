from functools import partial
import yaml

# Times are in milliseconds
MILLI_TO_SECONDS = 1000

TASK_EXCHANGE = 'kiwipy.tasks'
TASK_QUEUE = 'kiwipy.tasks'
MESSAGE_EXCHANGE = 'kiwipy.messages'
BROADCAST_TOPIC = '[broadcast]'
RPC_TOPIC = '[rpc]'
# Have to set MESSAGE_TTL to > 65535 because of a bug in aio-pika which fails when using RabbitMQ
# 3.5 (as present in Ubuntu 16.04) and likely earlier.  If you have a newer version of RabbitMQ
# it's fine to set this lower.  See:
# https://github.com/mosquito/aio-pika/issues/165
MESSAGE_TTL = 66 * MILLI_TO_SECONDS
TEST_QUEUE_EXPIRES = 10 * MILLI_TO_SECONDS
QUEUE_EXPIRES = 60 * MILLI_TO_SECONDS
REPLY_QUEUE_EXPIRES = 60 * MILLI_TO_SECONDS
TASK_MESSAGE_TTL = 60 * MILLI_TO_SECONDS * 60 * 24 * 7  # One week
TASK_PREFETCH_SIZE = 0
TASK_PREFETCH_COUNT = 0
TASK_FETCH_TIMEOUT = 5.

ENCODER = partial(yaml.dump, encoding='utf-8')
DECODER = partial(yaml.load, Loader=yaml.FullLoader)
