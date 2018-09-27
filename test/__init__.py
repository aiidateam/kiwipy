from __future__ import absolute_import
from __future__ import print_function
import logging
import tempfile
import os
from . import utils

TEST_LOG = os.path.join(tempfile.gettempdir(), 'kiwipy_unittest.log')
try:
    os.remove(TEST_LOG)
except OSError:
    pass
print(("Logging test to '{}'".format(TEST_LOG)))
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s()] %(message)s"
logging.basicConfig(filename=TEST_LOG, level=logging.INFO, format=FORMAT)
