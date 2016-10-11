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

    def delete(self, callback=None):
        if self.queue:
            return self.queue.delete_message(self, callback)

    def change_visibility(self, visibility_timeout, callback=None):
        if self.queue:
            return self.queue.connection.change_message_visibility(
                self.queue, self.receipt_handle, visibility_timeout, callback,
            )


class AsyncRawMessage(BaseAsyncMessage, RawMessage):
    """Raw Message."""


class AsyncMessage(BaseAsyncMessage, Message):
    """Serialized message."""


class AsyncMHMessage(BaseAsyncMessage, MHMessage):
    """MHM Message (uhm, look that up later)."""


class AsyncEncodedMHMessage(BaseAsyncMessage, EncodedMHMessage):
    """Encoded MH Message."""


class AsyncJSONMessage(BaseAsyncMessage, JSONMessage):
    """Json serialized message."""
