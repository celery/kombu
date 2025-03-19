from __future__ import annotations

import json
from unittest import mock
from unittest.mock import MagicMock, Mock

from kombu.asynchronous.aws.ext import AWSRequest, boto3
from kombu.asynchronous.aws.sqs.connection import (AsyncSQSConnection,
                                                   _query_object_encode)
from kombu.asynchronous.aws.sqs.message import AsyncMessage
from kombu.asynchronous.aws.sqs.queue import AsyncQueue
from kombu.utils.uuid import uuid
from t.mocks import PromiseMock

from ..case import AWSCase

SQS_URL = 'https://sqs.us-west-2.amazonaws.com/'


class test_AsyncSQSConnection(AWSCase):

    def setup_method(self):
        session = boto3.session.Session(
            aws_access_key_id='AAA',
            aws_secret_access_key='AAAA',
            region_name='us-west-2',
        )
        self.sqs_client = session.client('sqs')
        self.x = AsyncSQSConnection(self.sqs_client, 'ak', 'sk', http_client=Mock())
        self.x.get_object = Mock(name='X.get_object')
        self.x.get_status = Mock(name='X.get_status')
        self.x.get_list = Mock(name='X.get_list')
        self.callback = PromiseMock(name='callback')

        self.sqs_client.get_queue_url = MagicMock(return_value={
            'QueueUrl': 'http://aws.com'
        })

    def MockRequest(self):
        return AWSRequest(
            method='POST',
            url='https://aws.com',
        )

    def MockOperationModel(self, operation_name, method):
        mock = MagicMock()
        mock.configure_mock(
            http=MagicMock(
                get=MagicMock(
                    return_value=method,
                )
            ),
            name=operation_name,
            metadata={
                'jsonVersion': '1.0',
                'targetPrefix': 'sqs',
            }
        )
        return mock

    def MockServiceModel(self, operation_name, method):
        service_model = MagicMock()
        service_model.protocol = 'json',
        service_model.operation_model = MagicMock(
            return_value=self.MockOperationModel(operation_name, method)
        )
        return service_model

    def assert_requests_equal(self, req1, req2):
        assert req1.url == req2.url
        assert req1.method == req2.method
        assert req1.data == req2.data
        assert req1.params == req2.params
        assert dict(req1.headers) == dict(req2.headers)

    def test_fetch_attributes_on_construction(self):
        """Verify default fetch_message_attributes can be set at construction."""
        x = AsyncSQSConnection(
            self.sqs_client, 'ak', 'sk', http_client=Mock(),
            fetch_message_attributes=["AttributeOne"],
        )
        assert x.fetch_message_attributes == ["AttributeOne"]

        # Default value for backwards compatibility
        assert self.x.fetch_message_attributes == ["ApproximateReceiveCount"]

    def test_create_query_request_get(self):
        # Query Protocol GET call per
        # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-making-api-requests-xml.html
        operation_name = 'CreateQueue'
        params = {
            'DefaultVisibilityTimeout': 40,
            'QueueName': 'celery-test',
            'Version': '2012-11-05',
        }
        verb = 'GET'
        req = self.x._create_query_request(operation_name, params, SQS_URL, verb)
        self.assert_requests_equal(req, AWSRequest(
            url=SQS_URL,
            method=verb,
            data=None,
            params={
                'Action': operation_name,
                **params
            },
            headers={},
        ))

        prepared = req.prepare()  # without signing for test

        assert prepared.method == 'GET'
        assert prepared.url ==  (
            'https://sqs.us-west-2.amazonaws.com/?'
            'DefaultVisibilityTimeout=40'
            '&QueueName=celery-test'
            '&Version=2012-11-05'
            '&Action=CreateQueue'
        )
        assert prepared.headers == {}
        assert prepared.body is None

    def test_create_query_request(self):
        operation_name = 'ReceiveMessage'
        params = {
            'MaxNumberOfMessages': 10,
            'AttributeName.1': 'ApproximateReceiveCount',
            'WaitTimeSeconds': 20
        }
        queue_url = f'{SQS_URL}123456789012/celery-test'
        verb = 'POST'
        req = self.x._create_query_request(operation_name, params, queue_url,
                                           verb)
        self.assert_requests_equal(req, AWSRequest(
            url=queue_url,
            method=verb,
            data={
                'Action': operation_name,
                **params
            },
            headers={
                'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
            },
        ))

        prepared = req.prepare()  # without signing for test

        assert prepared.method == 'POST'
        assert prepared.url == queue_url
        assert prepared.headers == {
            'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
            'Content-Length': mock.ANY,
        }
        assert prepared.body == (
            'MaxNumberOfMessages=10'
            '&AttributeName.1=ApproximateReceiveCount'
            '&WaitTimeSeconds=20'
            '&Action=ReceiveMessage'
        )

    def test_create_json_request(self):
        operation_name = 'ReceiveMessage'
        method = 'POST'
        params = {
            'MaxNumberOfMessages': 10,
            'AttributeNames': ['ApproximateReceiveCount'],
            'WaitTimeSeconds': 20
        }
        queue_url = f'{SQS_URL}123456789012/celery-test'

        self.x.sqs_connection = Mock()
        self.x.sqs_connection._request_signer = Mock()
        self.x.sqs_connection._endpoint.host = SQS_URL
        self.x.sqs_connection.meta.service_model = Mock()
        self.x.sqs_connection.meta.service_model.protocol = 'json',
        self.x.sqs_connection.meta.service_model.operation_model = MagicMock(
            return_value=self.MockOperationModel(operation_name, method)
        )

        req = self.x._create_json_request(operation_name, params, queue_url)
        self.assert_requests_equal(req, AWSRequest(
            url=SQS_URL,
            method=method,
            data=json.dumps({
                **params,
                "QueueUrl": queue_url
            }),
            headers={
                'Content-Type': 'application/x-amz-json-1.0',
                'X-Amz-Target': f'sqs.{operation_name}'
            },
        ))

        prepared = req.prepare()  # without signing for test
        assert prepared.method == 'POST'
        assert prepared.url == SQS_URL
        assert prepared.headers == {
            'Content-Type': 'application/x-amz-json-1.0',
            'X-Amz-Target': 'sqs.ReceiveMessage',
            'Content-Length': mock.ANY,
        }
        assert json.loads(prepared.body) == {
            'MaxNumberOfMessages': 10,
            'AttributeNames': ['ApproximateReceiveCount'],
            'WaitTimeSeconds': 20,
            'QueueUrl': queue_url,
        }

    def test_make_request__with_query_protocol(self):
        # Do the necessary mocking.
        self.x.sqs_connection = Mock()
        self.x.sqs_connection._request_signer = Mock()
        self.x.sqs_connection.meta.service_model.protocol = 'query'
        self.x._create_query_request = Mock(return_value=self.MockRequest())

        # Execute the make_request called and confirm we are creating a
        # query request.
        operation = 'ReceiveMessage',
        params = {
            'MaxNumberOfMessages': 10,
            'WaitTimeSeconds': 20
        }
        pparams = {
            'json': {'AttributeNames': ['ApproximateReceiveCount']},
            'query': {'AttributeName.1': 'ApproximateReceiveCount'},
        }
        queue_url = f'{SQS_URL}123456789012/celery-test'
        verb = 'POST'

        expect_params = {**params, 'AttributeName.1': 'ApproximateReceiveCount'}

        self.x.make_request(operation, params, queue_url, verb, protocol_params=pparams)
        self.x._create_query_request.assert_called_with(
            operation, expect_params, queue_url, verb
        )

    def test_make_request__with_json_protocol(self):
        # Do the necessary mocking.
        self.x.sqs_connection = Mock()
        self.x.sqs_connection._request_signer = Mock()
        self.x.sqs_connection.meta.service_model.protocol = 'json'
        self.x._create_json_request = Mock(return_value=self.MockRequest())

        # Execute the make_request called and confirm we are creating a
        # query request.
        operation = 'ReceiveMessage',
        params = {
            'MaxNumberOfMessages': 10,
            'WaitTimeSeconds': 20
        }
        pparams = {
            'json': {'AttributeNames': ['ApproximateReceiveCount']},
            'query': {'AttributeName.1': 'ApproximateReceiveCount'},
        }

        queue_url = f'{SQS_URL}123456789012/celery-test'
        verb = 'POST'
        expect_params = {**params, 'AttributeNames': ['ApproximateReceiveCount']}

        self.x.make_request(operation, params, queue_url, verb, protocol_params=pparams)
        self.x._create_json_request.assert_called_with(
            operation, expect_params, queue_url
        )

    def test_create_queue(self):
        self.x.create_queue('foo', callback=self.callback)
        self.x.get_object.assert_called_with(
            'CreateQueue', {'QueueName': 'foo'},
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
            callback=self.callback
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
            queue.id, callback=self.callback,
        )

    def test_set_queue_attribute(self):
        queue = Mock(name='queue')
        self.x.set_queue_attribute(
            queue, 'Expires', '3600', callback=self.callback,
        )
        self.x.get_status.assert_called_with(
            'SetQueueAttribute',
            {}, queue.id, callback=self.callback,
            protocol_params={
                'json': {'Attributes': {'Expires': '3600'}},
                'query': {'Attribute.Name': 'Expires', 'Attribute.Value': '3600'},
            },
        )

    def test_receive_message(self):
        queue = Mock(name='queue')
        self.x.receive_message(
            queue,
            self.x.get_queue_url('queue'),
            4,
            callback=self.callback,
        )
        self.x.get_list.assert_called_with(
            'ReceiveMessage', {
                'MaxNumberOfMessages': 4,
            },
            [('Message', AsyncMessage)],
            'http://aws.com', callback=self.callback,
            parent=queue,
            protocol_params={
                'json': {'AttributeNames': ['ApproximateReceiveCount']},
                'query': {'AttributeName.1': 'ApproximateReceiveCount'},
            },
        )

    def test_receive_message__with_visibility_timeout(self):
        queue = Mock(name='queue')
        self.x.receive_message(
            queue,
            self.x.get_queue_url('queue'),
            4,
            3666,
            callback=self.callback,
        )
        self.x.get_list.assert_called_with(
            'ReceiveMessage', {
                'MaxNumberOfMessages': 4,
                'VisibilityTimeout': 3666,
            },
            [('Message', AsyncMessage)],
            'http://aws.com', callback=self.callback,
            parent=queue,
            protocol_params={
                'json': {'AttributeNames': ['ApproximateReceiveCount']},
                'query': {'AttributeName.1': 'ApproximateReceiveCount'},
            },
        )

    def test_receive_message__with_wait_time_seconds(self):
        queue = Mock(name='queue')
        self.x.receive_message(
            queue,
            self.x.get_queue_url('queue'),
            4,
            wait_time_seconds=303,
            callback=self.callback,
        )
        self.x.get_list.assert_called_with(
            'ReceiveMessage', {
                'MaxNumberOfMessages': 4,
                'WaitTimeSeconds': 303,
            },
            [('Message', AsyncMessage)],
            'http://aws.com', callback=self.callback,
            parent=queue,
            protocol_params={
                'json': {'AttributeNames': ['ApproximateReceiveCount']},
                'query': {'AttributeName.1': 'ApproximateReceiveCount'},
            },
        )

    def test_receive_message__with_attributes(self):
        queue = Mock(name='queue')
        self.x.receive_message(
            queue,
            self.x.get_queue_url('queue'),
            4,
            attributes=['foo', 'bar'],
            callback=self.callback,
        )
        self.x.get_list.assert_called_with(
            'ReceiveMessage', {
                'MaxNumberOfMessages': 4,
            },
            [('Message', AsyncMessage)],
            'http://aws.com', callback=self.callback,
            parent=queue,
            protocol_params={
                'json': {'AttributeNames': ['foo', 'bar']},
                'query': {'AttributeName.1': 'foo', 'AttributeName.2': 'bar'},
            },
        )

    def test_receive_message__with_fetch_attributes(self):
        queue = Mock(name='queue')
        self.x.fetch_message_attributes = ["DifferentAttribute1", "Another2"]
        self.x.receive_message(
            queue,
            self.x.get_queue_url('queue'),
            4,
            callback=self.callback,
        )
        self.x.get_list.assert_called_with(
            'ReceiveMessage', {
                'MaxNumberOfMessages': 4,
            },
            [('Message', AsyncMessage)],
            'http://aws.com', callback=self.callback,
            parent=queue,
            protocol_params={
                'json': {'AttributeNames': ['DifferentAttribute1', 'Another2']},
                'query': {'AttributeName.1': 'DifferentAttribute1', 'AttributeName.2': 'Another2'},
            },
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
        self.x.delete_message(queue, message.receipt_handle,
                              callback=self.callback)
        self.x.get_status.assert_called_with(
            'DeleteMessage', {'ReceiptHandle': message.receipt_handle},
            queue, callback=self.callback,
        )

    def test_delete_message_batch(self):
        queue = Mock(name='queue')
        messages = [self.MockMessage('1', 'r1'),
                    self.MockMessage('2', 'r2')]
        self.x.delete_message_batch(queue, messages, callback=self.callback)
        self.x.get_object.assert_called_with(
            'DeleteMessageBatch', {},
            queue.id, verb='POST', callback=self.callback,
            protocol_params={
                'json': {'Entries': [{'Id': '1', 'ReceiptHandle': 'r1'}, {'Id': '2', 'ReceiptHandle': 'r2'}]},
                'query': {
                    'DeleteMessageBatchRequestEntry.1.Id': '1',
                    'DeleteMessageBatchRequestEntry.1.ReceiptHandle': 'r1',
                    'DeleteMessageBatchRequestEntry.2.Id': '2',
                    'DeleteMessageBatchRequestEntry.2.ReceiptHandle': 'r2',
                },
            },
        )

    def test_send_message(self):
        queue = Mock(name='queue')
        self.x.send_message(queue, 'hello', callback=self.callback)
        self.x.get_object.assert_called_with(
            'SendMessage', {'MessageBody': 'hello'},
            queue.id, verb='POST', callback=self.callback,
        )

    def test_send_message__with_delay_seconds(self):
        queue = Mock(name='queue')
        self.x.send_message(
            queue, 'hello', delay_seconds='303', callback=self.callback,
        )
        self.x.get_object.assert_called_with(
            'SendMessage', {'MessageBody': 'hello', 'DelaySeconds': 303},
            queue.id, verb='POST', callback=self.callback,
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
            queue.id, verb='POST', callback=self.callback,
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

        self.x.get_object.assert_called_once_with(
            'ChangeMessageVisibilityBatch', {},
            queue.id, verb='POST', callback=self.callback,
            protocol_params={
                'json': {
                    'Entries': [
                        {'Id': '1', 'ReceiptHandle': 'r1', 'VisibilityTimeout': 303},
                        {'Id': '2', 'ReceiptHandle': 'r2', 'VisibilityTimeout': 909},
                    ],
                },
                'query': {
                    'ChangeMessageVisibilityBatchRequestEntry.1.Id': '1',
                    'ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle': 'r1',
                    'ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout': '303',
                    'ChangeMessageVisibilityBatchRequestEntry.2.Id': '2',
                    'ChangeMessageVisibilityBatchRequestEntry.2.ReceiptHandle': 'r2',
                    'ChangeMessageVisibilityBatchRequestEntry.2.VisibilityTimeout': '909',
                },
            },
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

    def test_query_protocol_encoding(self):
        assert _query_object_encode({}) == {}

        assert _query_object_encode({'Simple': 'String'}) == {'Simple': 'String'}

        assert _query_object_encode({'NumbersToString': 123}) == {'NumbersToString': '123'}

        assert _query_object_encode({'AttributeName': ['A', 'B']}) == {
            'AttributeName.1': 'A',
            'AttributeName.2': 'B',
        }

        assert _query_object_encode({'Grandparent': [{'Parent': {'Child': '1', 'Sibling': 2}}]}) == {
            'Grandparent.1.Parent.Child': '1',
            'Grandparent.1.Parent.Sibling': '2',
        }
