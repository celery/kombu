"""
kombu.utils.compat
==================

Helps compatibility with older Python versions.

"""
from __future__ import absolute_import

############## collections.OrderedDict #######################################
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # noqa
