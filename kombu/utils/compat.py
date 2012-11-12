"""
kombu.utils.compat
==================

Helps compatibility with older Python versions.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

############## collections.OrderedDict #######################################
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # noqa
