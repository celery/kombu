# -*- coding: utf-8 -*-
"""Amazon boto interface."""
from __future__ import absolute_import, unicode_literals

try:
    import boto
except ImportError:  # pragma: no cover
    boto = get_regions = ResultSet = RegionInfo = XmlHandler = None

    class _void(object):
        pass
    AWSAuthConnection = AWSQueryConnection = _void  # noqa

    class BotoError(Exception):
        pass
    exception = _void()
    exception.SQSError = BotoError
    exception.SQSDecodeError = BotoError
else:
    from boto import exception
    from boto.connection import AWSAuthConnection, AWSQueryConnection
    from boto.handler import XmlHandler
    from boto.resultset import ResultSet
    from boto.regioninfo import RegionInfo, get_regions

__all__ = [
    'exception', 'AWSAuthConnection', 'AWSQueryConnection',
    'XmlHandler', 'ResultSet', 'RegionInfo', 'get_regions',
]
