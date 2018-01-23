import unittest

try:
    import pika
    from kiwipy import rmq
except ImportError:
    pika = None


class TestCaseWithLoop(unittest.TestCase):
    def setUp(self):
        super(TestCaseWithLoop, self).setUp()
        self.loop = rmq.new_event_loop()
        rmq.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        self.loop = None
        rmq.set_event_loop(None)
