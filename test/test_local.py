# -*- coding: utf-8 -*-
import unittest

import kiwipy

from .utils import CommunicatorTester


class TestLocalCommunicator(CommunicatorTester, unittest.TestCase):

    def create_communicator(self):
        return kiwipy.LocalCommunicator()
