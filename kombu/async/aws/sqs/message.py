# -*- coding: utf-8 -*-
"""Amazon SQS message implementation."""
from __future__ import absolute_import, unicode_literals

from .ext import (
    RawMessage, Message, MHMessage, EncodedMHMessage, JSONMessage,
)

__all__ = [
    'BaseAsyncMessage', 'AsyncRawMessage', 'AsyncMessage',
    'AsyncMHMessage', 'AsyncEncodedMHMessage', 'AsyncJSONMessage',
]


class BaseAsyncMessage(object):
    """Base class for messages received on async client."""


class AsyncRawMessage(BaseAsyncMessage, RawMessage):
    """Raw Message."""


class AsyncMessage(BaseAsyncMessage, Message):
    """Serialized message."""

    def __getitem__(self, item):
        """
        Support Boto3-style access on a message.
        """
        if item == 'ReceiptHandle':
            return self.receipt_handle
        elif item == 'Body':
            return self.get_body()
        elif item == 'queue':
            return self.queue
        else:
            raise KeyError(item)


class AsyncMHMessage(BaseAsyncMessage, MHMessage):
    """MHM Message (uhm, look that up later)."""


class AsyncEncodedMHMessage(BaseAsyncMessage, EncodedMHMessage):
    """Encoded MH Message."""


class AsyncJSONMessage(BaseAsyncMessage, JSONMessage):
    """Json serialized message."""
