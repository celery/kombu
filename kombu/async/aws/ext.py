# -*- coding: utf-8 -*-
"""Amazon boto3 interface."""
from __future__ import absolute_import, unicode_literals

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
    'exceptions'
]
