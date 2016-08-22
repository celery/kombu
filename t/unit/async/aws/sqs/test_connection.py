# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock

from kombu.async.aws.sqs.connection import (
    AsyncSQSConnection, Attributes, BatchResults,
)
from kombu.async.aws.sqs.message import AsyncMessage
from kombu.async.aws.sqs.queue import AsyncQueue
from kombu.utils.uuid import uuid

from t.mocks import PromiseMock

from ..case import AWSCase


class test_AsyncSQSConnection(AWSCase):

    def setup(self):
        self.x = AsyncSQSConnection('ak', 'sk', http_client=Mock())
        self.x.get_object = Mock(name='X.get_object')
        self.x.get_status = Mock(name='X.get_status')
        self.x.get_list = Mock(nanme='X.get_list')
        self.callback = PromiseMock(name='callback')

    def test_without_boto(self):
        from kombu.async.aws.sqs import connection
        prev, connection.boto = connection.boto, None
        try:
            with pytest.raises(ImportError):
                AsyncSQSConnection('ak', 'sk', http_client=Mock())
        finally:
            connection.boto = prev

    def test_default_region(self):
        assert self.x.region
        assert issubclass(self.x.region.connection_cls, AsyncSQSConnection)

    def test_create_queue(self):
        self.x.create_queue('foo', callback=self.callback)
        self.x.get_object.assert_called_with(
            'CreateQueue', {'QueueName': 'foo'}, AsyncQueue,
            callback=self.callback,
        )

    def test_create_queue__with_visibility_timeout(self):
        self.x.create_queue(
            'foo', visibility_timeout=33, callback=self.callback,
        )
        self.x.get_object.assert_called_with(
            'CreateQueue', {
                'QueueName': 'foo',
                'DefaultVisibilityTimeout': '33'
            },
            AsyncQueue, callback=self.callback
        )

    def test_delete_queue(self):
        queue = Mock(name='queue')
        self.x.delete_queue(queue, callback=self.callback)
        self.x.get_status.assert_called_with(
            'DeleteQueue', None, queue.id, callback=self.callback,
        )

    def test_get_queue_attributes(self):
        queue = Mock(name='queue')
        self.x.get_queue_attributes(
            queue, attribute='QueueSize', callback=self.callback,
        )
        self.x.get_object.assert_called_with(
            'GetQueueAttributes', {'AttributeName': 'QueueSize'},
            Attributes, queue.id, callback=self.callback,
        )

    def test_set_queue_attribute(self):
        queue = Mock(name='queue')
        self.x.set_queue_attribute(
            queue, 'Expires', '3600', callback=self.callback,
        )
        self.x.get_status.assert_called_with(
            'SetQueueAttribute', {
                'Attribute.Name': 'Expires',
                'Attribute.Value': '3600',
            },
            queue.id, callback=self.callback,
        )

    def test_receive_message(self):
        queue = Mock(name='queue')
        self.x.receive_message(queue, 4, callback=self.callback)
        self.x.get_list.assert_called_with(
            'ReceiveMessage', {'MaxNumberOfMessages': 4},
            [('Message', queue.message_class)],
            queue.id, callback=self.callback,
        )

    def test_receive_message__with_visibility_timeout(self):
        queue = Mock(name='queue')
        self.x.receive_message(queue, 4, 3666, callback=self.callback)
        self.x.get_list.assert_called_with(
            'ReceiveMessage', {
                'MaxNumberOfMessages': 4,
                'VisibilityTimeout': 3666,
            },
            [('Message', queue.message_class)],
            queue.id, callback=self.callback,
        )

    def test_receive_message__with_wait_time_seconds(self):
        queue = Mock(name='queue')
        self.x.receive_message(
            queue, 4, wait_time_seconds=303, callback=self.callback,
        )
        self.x.get_list.assert_called_with(
            'ReceiveMessage', {
                'MaxNumberOfMessages': 4,
                'WaitTimeSeconds': 303,
            },
            [('Message', queue.message_class)],
            queue.id, callback=self.callback,
        )

    def test_receive_message__with_attributes(self):
        queue = Mock(name='queue')
        self.x.receive_message(
            queue, 4, attributes=['foo', 'bar'], callback=self.callback,
        )
        self.x.get_list.assert_called_with(
            'ReceiveMessage', {
                'AttributeName.1': 'foo',
                'AttributeName.2': 'bar',
                'MaxNumberOfMessages': 4,
            },
            [('Message', queue.message_class)],
            queue.id, callback=self.callback,
        )

    def MockMessage(self, id=None, receipt_handle=None, body=None):
        m = Mock(name='message')
        m.id = id or uuid()
        m.receipt_handle = receipt_handle or uuid()
        m._body = body

        def _get_body():
            return m._body
        m.get_body.side_effect = _get_body

        def _set_body(value):
            m._body = value
        m.set_body.side_effect = _set_body

        return m

    def test_delete_message(self):
        queue = Mock(name='queue')
        message = self.MockMessage()
        self.x.delete_message(queue, message, callback=self.callback)
        self.x.get_status.assert_called_with(
            'DeleteMessage', {'ReceiptHandle': message.receipt_handle},
            queue.id, callback=self.callback,
        )

    def test_delete_message_batch(self):
        queue = Mock(name='queue')
        messages = [self.MockMessage('1', 'r1'),
                    self.MockMessage('2', 'r2')]
        self.x.delete_message_batch(queue, messages, callback=self.callback)
        self.x.get_object.assert_called_with(
            'DeleteMessageBatch', {
                'DeleteMessageBatchRequestEntry.1.Id': '1',
                'DeleteMessageBatchRequestEntry.1.ReceiptHandle': 'r1',
                'DeleteMessageBatchRequestEntry.2.Id': '2',
                'DeleteMessageBatchRequestEntry.2.ReceiptHandle': 'r2',
            },
            BatchResults, queue.id, verb='POST', callback=self.callback,
        )

    def test_send_message(self):
        queue = Mock(name='queue')
        self.x.send_message(queue, 'hello', callback=self.callback)
        self.x.get_object.assert_called_with(
            'SendMessage', {'MessageBody': 'hello'},
            AsyncMessage, queue.id, verb='POST', callback=self.callback,
        )

    def test_send_message__with_delay_seconds(self):
        queue = Mock(name='queue')
        self.x.send_message(
            queue, 'hello', delay_seconds='303', callback=self.callback,
        )
        self.x.get_object.assert_called_with(
            'SendMessage', {'MessageBody': 'hello', 'DelaySeconds': 303},
            AsyncMessage, queue.id, verb='POST', callback=self.callback,
        )

    def test_send_message_batch(self):
        queue = Mock(name='queue')
        messages = [self.MockMessage('1', 'r1', 'A'),
                    self.MockMessage('2', 'r2', 'B')]
        self.x.send_message_batch(
            queue, [(m.id, m.get_body(), 303) for m in messages],
            callback=self.callback
        )
        self.x.get_object.assert_called_with(
            'SendMessageBatch', {
                'SendMessageBatchRequestEntry.1.Id': '1',
                'SendMessageBatchRequestEntry.1.MessageBody': 'A',
                'SendMessageBatchRequestEntry.1.DelaySeconds': 303,
                'SendMessageBatchRequestEntry.2.Id': '2',
                'SendMessageBatchRequestEntry.2.MessageBody': 'B',
                'SendMessageBatchRequestEntry.2.DelaySeconds': 303,
            },
            BatchResults, queue.id, verb='POST', callback=self.callback,
        )

    def test_change_message_visibility(self):
        queue = Mock(name='queue')
        self.x.change_message_visibility(
            queue, 'rcpt', 33, callback=self.callback,
        )
        self.x.get_status.assert_called_with(
            'ChangeMessageVisibility', {
                'ReceiptHandle': 'rcpt',
                'VisibilityTimeout': 33,
            },
            queue.id, callback=self.callback,
        )

    def test_change_message_visibility_batch(self):
        queue = Mock(name='queue')
        messages = [
            (self.MockMessage('1', 'r1'), 303),
            (self.MockMessage('2', 'r2'), 909),
        ]
        self.x.change_message_visibility_batch(
            queue, messages, callback=self.callback,
        )

        def preamble(n):
            return '.'.join(['ChangeMessageVisibilityBatchRequestEntry', n])

        self.x.get_object.assert_called_with(
            'ChangeMessageVisibilityBatch', {
                preamble('1.Id'): '1',
                preamble('1.ReceiptHandle'): 'r1',
                preamble('1.VisibilityTimeout'): 303,
                preamble('2.Id'): '2',
                preamble('2.ReceiptHandle'): 'r2',
                preamble('2.VisibilityTimeout'): 909,
            },
            BatchResults, queue.id, verb='POST', callback=self.callback,
        )

    def test_get_all_queues(self):
        self.x.get_all_queues(callback=self.callback)
        self.x.get_list.assert_called_with(
            'ListQueues', {}, [('QueueUrl', AsyncQueue)],
            callback=self.callback,
        )

    def test_get_all_queues__with_prefix(self):
        self.x.get_all_queues(prefix='kombu.', callback=self.callback)
        self.x.get_list.assert_called_with(
            'ListQueues', {'QueueNamePrefix': 'kombu.'},
            [('QueueUrl', AsyncQueue)],
            callback=self.callback,
        )

    def MockQueue(self, url):
        q = Mock(name='Queue')
        q.url = url
        return q

    def test_get_queue(self):
        self.x.get_queue('foo', callback=self.callback)
        self.x.get_list.assert_called()
        on_ready = self.x.get_list.call_args[1]['callback']
        queues = [
            self.MockQueue('/queues/bar'),
            self.MockQueue('/queues/baz'),
            self.MockQueue('/queues/foo'),
        ]
        on_ready(queues)
        self.callback.assert_called_with(queues[-1])

        self.x.get_list.assert_called_with(
            'ListQueues', {'QueueNamePrefix': 'foo'},
            [('QueueUrl', AsyncQueue)],
            callback=on_ready,
        )

    def test_get_dead_letter_source_queues(self):
        queue = Mock(name='queue')
        self.x.get_dead_letter_source_queues(queue, callback=self.callback)
        self.x.get_list.assert_called_with(
            'ListDeadLetterSourceQueues', {'QueueUrl': queue.url},
            [('QueueUrl', AsyncQueue)], callback=self.callback,
        )

    def test_add_permission(self):
        queue = Mock(name='queue')
        self.x.add_permission(
            queue, 'label', 'accid', 'action', callback=self.callback,
        )
        self.x.get_status.assert_called_with(
            'AddPermission', {
                'Label': 'label',
                'AWSAccountId': 'accid',
                'ActionName': 'action',
            },
            queue.id, callback=self.callback,
        )

    def test_remove_permission(self):
        queue = Mock(name='queue')
        self.x.remove_permission(queue, 'label', callback=self.callback)
        self.x.get_status.assert_called_with(
            'RemovePermission', {'Label': 'label'}, queue.id,
            callback=self.callback,
        )
