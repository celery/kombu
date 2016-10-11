# -*- coding: utf-8 -*-
"""Amazon SQS boto interface."""

from __future__ import absolute_import, unicode_literals

try:
    import boto
except ImportError:  # pragma: no cover
    boto = Attributes = BatchResults = None  # noqa

    class _void(object):
        pass
    regions = SQSConnection = Queue = _void

    RawMessage = Message = MHMessage = \
        EncodedMHMessage = JSONMessage = _void
else:
    from boto.sqs.attributes import Attributes
    from boto.sqs.batchresults import BatchResults
    from boto.sqs.message import (
        EncodedMHMessage, Message, MHMessage, RawMessage,
    )
    from boto.sqs import regions
    from boto.sqs.jsonmessage import JSONMessage
    from boto.sqs.connection import SQSConnection
    from boto.sqs.queue import Queue

__all__ = [
    'Attributes', 'BatchResults', 'EncodedMHMessage', 'MHMessage',
    'Message', 'RawMessage', 'JSONMessage', 'SQSConnection',
    'Queue', 'regions',
]
