# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from .ext import (
    RawMessage, Message, MHMessage, EncodedMHMessage, JSONMessage,
)

__all__ = [
    'BaseAsyncMessage', 'AsyncRawMessage', 'AsyncMessage',
    'AsyncMHMessage', 'AsyncEncodedMHMessage', 'AsyncJSONMessage',
]


class BaseAsyncMessage(object):

    def delete(self, callback=None):
        if self.queue:
            return self.queue.delete_message(self, callback)

    def change_visibility(self, visibility_timeout, callback=None):
        if self.queue:
            return self.queue.connection.change_message_visibility(
                self.queue, self.receipt_handle, visibility_timeout, callback,
            )


class AsyncRawMessage(BaseAsyncMessage, RawMessage):
    pass


class AsyncMessage(BaseAsyncMessage, Message):
    pass


class AsyncMHMessage(BaseAsyncMessage, MHMessage):
    pass


class AsyncEncodedMHMessage(BaseAsyncMessage, EncodedMHMessage):
    pass


class AsyncJSONMessage(BaseAsyncMessage, JSONMessage):
    pass
