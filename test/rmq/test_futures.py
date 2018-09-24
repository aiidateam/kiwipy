from __future__ import absolute_import
import unittest

import kiwipy


class TestUtilities(unittest.TestCase):

    def test_capture_exceptions(self):
        comm = kiwipy.LocalCommunicator()
        future = comm.create_future()

        exception = RuntimeError()
        with kiwipy.capture_exceptions(future):
            raise exception

        self.assertIs(exception, future.exception())
