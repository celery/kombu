# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from kombu.async.aws.sqs.message import AsyncMessage

from kombu.tests.async.aws.case import AWSCase
from kombu.tests.case import PromiseMock, Mock
from kombu.utils import uuid


class test_AsyncMessage(AWSCase):

    def setup(self):
        self.queue = Mock(name='queue')
        self.callback = PromiseMock(name='callback')
        self.x = AsyncMessage(self.queue, 'body')
        self.x.receipt_handle = uuid()

    def test_delete(self):
        self.assertTrue(self.x.delete(callback=self.callback))
        self.x.queue.delete_message.assert_called_with(
            self.x, self.callback,
        )

        self.x.queue = None
        self.assertIsNone(self.x.delete(callback=self.callback))

    def test_change_visibility(self):
        self.assertTrue(self.x.change_visibility(303, callback=self.callback))
        self.x.queue.connection.change_message_visibility.assert_called_with(
            self.x.queue, self.x.receipt_handle, 303, self.callback,
        )
        self.x.queue = None
        self.assertIsNone(self.x.change_visibility(
            303, callback=self.callback,
        ))
