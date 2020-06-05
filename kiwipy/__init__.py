# -*- coding: utf-8 -*-
import logging

# pylint: disable=redefined-builtin, undefined-variable
from .communications import *
from .communicate import *
from .exceptions import *
from .filters import *
from .futures import *
from .local import *
from .utils import *
from .version import *

__all__ = (
    exceptions.__all__ + futures.__all__ + communications.__all__ + version.__all__ + utils.__all__ + local.__all__ +
    filters.__all__ + communicate.__all__
)


# Do this se we don't get the "No handlers could be found..." warnings that will be produced
# if a user of this library doesn't set any handlers. See
# https://docs.python.org/3.1/library/logging.html#library-config
# for more details
class NullHandler(logging.Handler):

    def emit(self, record):
        pass


logging.getLogger('kiwipy').addHandler(NullHandler())
