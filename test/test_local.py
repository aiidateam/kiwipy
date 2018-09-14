from __future__ import absolute_import
import kiwipy
import unittest

from .utils import CommunicatorTester


class TestLocalCommunicator(CommunicatorTester, unittest.TestCase):

    def create_communicator(self):
        return kiwipy.LocalCommunicator()
