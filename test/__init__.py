import logging
import tempfile
import os

testfile = os.path.join(tempfile.gettempdir(), 'kiwipy_unittest.log')
try:
    os.remove(testfile)
except OSError:
    pass
print("Logging test to '{}'".format(testfile))
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s()] %(message)s"
logging.basicConfig(filename=testfile, level=logging.INFO, format=FORMAT)
