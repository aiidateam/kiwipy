import kiwipy
import unittest

from .utils import CommunicatorTester


class TestLocalCommunicator(CommunicatorTester, unittest.TestCase):
    def create_communicator(self):
        return kiwipy.LocalCommunicator()
