# -*- coding: utf-8 -*-
from __future__ import absolute_import

from boto.sqs import message as _message

__all__ = ['BaseAsyncMessage', 'AsyncRawMessage', 'AsyncMessage',
           'AsyncMHMessage', 'AsyncEncodedMHMessage']


class BaseAsyncMessage(object):

    def delete(self, callback=None):
        if self.queue:
            return self.queue.delete_message(self, callback)

    def change_visibility(self, visibility_timeout, callback=None):
        if self.queue:
            return self.queue.connection.change_message_visibility(
                self.queue, self.receipt_handle, visibility_timeout, callback,
            )


class AsyncRawMessage(BaseAsyncMessage, _message.RawMessage):
    pass


class AsyncMessage(BaseAsyncMessage, _message.Message):
    pass


class AsyncMHMessage(BaseAsyncMessage, _message.MHMessage):
    pass


class AsyncEncodedMHMessage(BaseAsyncMessage, _message.EncodedMHMessage):
    pass
