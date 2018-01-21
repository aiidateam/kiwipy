import kiwi
import unittest

from .utils import CommunicatorTester


class TestLocalCommunicator(CommunicatorTester, unittest.TestCase):
    def create_communicator(self):
        return kiwi.LocalCommunicator()
