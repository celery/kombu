from __future__ import annotations

from unittest.mock import Mock

from kombu.asynchronous.aws.ext import boto3
from kombu.asynchronous.aws.sqs.connection import AsyncSQSConnection
from kombu.asynchronous.hub import Hub
from kombu.utils.uuid import uuid
from t.mocks import PromiseMock

from ..case import AWSCase


class test_AsyncSQSConnection(AWSCase):

    def setup(self):
        session = boto3.session.Session(
            aws_access_key_id='AAA',
            aws_secret_access_key='AAAA',
            region_name='us-west-2',
        )
        sqs_client = session.client('sqs')
        self.sqs = AsyncSQSConnection(sqs_client,
                                      hub=Mock(spec=Hub),
                                      http_client=Mock())
        self.sqs._async_sqs_request = Mock(name='sqs._async_sqs_request')
        self.callback = PromiseMock(name='callback')
        self.queue_url = 'http://aws.com'

    def test_receive_message(self):
        self.sqs.receive_message(self.queue_url, 4, callback=self.callback)
        self.sqs._async_sqs_request.assert_called_with(
            'receive_message',
            self.callback,
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=4,
            MessageAttributeNames=('ApproximateReceiveCount',),
            WaitTimeSeconds=None,
        )

    def test_receive_message__with_visibility_timeout(self):
        self.sqs.receive_message(
            self.queue_url,
            4,
            3666,
            callback=self.callback,
        )
        self.sqs._async_sqs_request.assert_called_with(
            'receive_message',
            self.callback,
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=4,
            MessageAttributeNames=('ApproximateReceiveCount',),
            WaitTimeSeconds=None,
            VisibilityTimeout=3666
        )

    def test_receive_message__with_wait_time_seconds(self):
        self.sqs.receive_message(
            self.queue_url,
            4,
            wait_time_seconds=303,
            callback=self.callback,
        )
        self.sqs._async_sqs_request.assert_called_with(
            'receive_message',
            self.callback,
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=4,
            MessageAttributeNames=('ApproximateReceiveCount',),
            WaitTimeSeconds=303,
        )

    def test_receive_message__with_attributes(self):
        self.sqs.receive_message(
            self.queue_url,
            4,
            attributes=('foo', 'bar'),
            callback=self.callback,
        )
        self.sqs._async_sqs_request.assert_called_with(
            'receive_message',
            self.callback,
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=4,
            MessageAttributeNames=('foo', 'bar'),
            WaitTimeSeconds=None,
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
        message = self.MockMessage()
        self.sqs.delete_message(self.queue_url, message.receipt_handle,
                                callback=self.callback)
        self.sqs._async_sqs_request.assert_called_with(
            'delete_message',
            self.callback,
            QueueUrl=self.queue_url,
            ReceiptHandle=message.receipt_handle,
        )
