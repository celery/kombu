# -*- coding: utf-8 -*-
"""Amazon boto3 interface."""
from __future__ import absolute_import, unicode_literals

try:
    import boto
except ImportError:  # pragma: no cover
    boto = ResultSet = RegionInfo = XmlHandler = None

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
    from boto.regioninfo import RegionInfo


try:
    import boto3
    from botocore import exceptions
    from boto3 import session
except ImportError:
    boto3 = session = None

    class _void(object):
        pass

    class BotoCoreError(Exception):
        pass
    exceptions = _void()
    exceptions.BotoCoreError = BotoCoreError


__all__ = [
    'exception', 'exceptions', 'AWSAuthConnection', 'AWSQueryConnection',
    'XmlHandler', 'ResultSet', 'RegionInfo',
]
