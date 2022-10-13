# -*- coding: utf-8 -*-
import logging
import os
import tempfile

from . import utils

TEST_LOG = os.path.join(tempfile.gettempdir(), 'kiwipy_unittest.log')
try:
    os.remove(TEST_LOG)
except OSError:
    pass
FORMAT = '[%(levelname)s][%(filename)s:%(lineno)s - %(funcName)s()] %(message)s'
logging.basicConfig(filename=TEST_LOG, level=logging.DEBUG, format=FORMAT)
