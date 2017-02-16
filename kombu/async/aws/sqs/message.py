# -*- coding: utf-8 -*-
"""Amazon SQS message implementation."""
from __future__ import absolute_import, unicode_literals

from kombu.message import Message


class BaseAsyncMessage(Message):
    """Base class for messages received on async client."""


class AsyncRawMessage(BaseAsyncMessage):
    """Raw Message."""


class AsyncMessage(BaseAsyncMessage):
    """Serialized message."""
    
    def encode(self, value):
        return value
    
    def decode(self, value):
        return value
    
    def __getitem__(self, item):
        """Support Boto3-style access on a message."""
        if item == 'ReceiptHandle':
            return self.receipt_handle
        elif item == 'Body':
            return self.get_body()
        elif item == 'queue':
            return self.queue
        else:
            raise KeyError(item)
