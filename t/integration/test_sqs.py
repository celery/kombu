from __future__ import annotations

import os
import uuid
from unittest.mock import patch

import pytest

import kombu
import kombu.asynchronous

from .common import BaseExchangeTypes, BaseMessage, BasicFunctionality


def get_connection(hostname: str = "localhost", port: int = 4100, queue_prefix: str = "") -> kombu.Connection:
    return kombu.Connection(
        f'sqs://{hostname}:{port}',
        userid="TestUsername",  # This can be anything
        password="TestPassword",  # This can be anything
        transport_options={
            "supports_fanout": True,
            "is_secure": False,
            "client-config": {
                "region_name": "us-east-1",
            },
            "queue_name_prefix": queue_prefix,
            "wait_time_seconds": 0,  # Set to 0 to ensure requeue testing works
        },
    )


@pytest.fixture()
def hub():
    """Provide a Kombu hub (event loop) for async I/O and callbacks."""
    previous_hub = kombu.asynchronous.get_event_loop()
    h = kombu.asynchronous.Hub()
    kombu.asynchronous.set_event_loop(h)
    yield h
    h.close()
    kombu.asynchronous.set_event_loop(previous_hub)


@pytest.fixture
def test_queue_prefix() -> str:
    """Generate a unique prefix for the test queues to avoid conflicts between test runs."""
    return str(uuid.uuid4())[:8] + "_"


@pytest.fixture()
def invalid_connection(test_queue_prefix):
    return kombu.Connection(
        'sqs://localhost:12345',
        userid="TestUsername",
        password="TestPassword",
        transport_options={
            "supports_fanout": True,
            "is_secure": False,
            "client-config": {
                "region_name": "us-east-1",
            },
            "queue_name_prefix": test_queue_prefix,
        })


@pytest.fixture()
def connection(hub, test_queue_prefix):
    conn = get_connection(
        hostname=os.environ.get('SQS_HOST', 'localhost'),
        port=os.environ.get('SQS_PORT', '4100'),
        queue_prefix=test_queue_prefix,
    )
    conn.transport_options['hub'] = hub
    return conn


@pytest.fixture()
def backoff_queue(connection, test_queue_prefix):
    queue_name = test_queue_prefix + 'ack_backoff'
    with connection as setup:
        client = setup.default_channel.sqs()
        queue_url = client.create_queue(QueueName=queue_name)['QueueUrl']
        try:
            with setup.clone(transport_options={
                **setup.transport_options,
                'queue_name_prefix': '',
                'predefined_queues': {
                    queue_name: {
                        'url': queue_url,
                        'backoff_tasks': ['tasks.example'],
                        'backoff_policy': {1: 10},
                    },
                },
            }) as conn:
                yield conn, kombu.Queue(queue_name, routing_key=queue_name)
        finally:
            client.delete_queue(QueueUrl=queue_url)


@pytest.fixture(autouse=True)
def mock_set_policy():
    """Mock the _set_policy_on_sqs_queue method as this is not supported by GoAws."""
    with patch("kombu.transport.SQS.SNS._SnsSubscription._set_policy_on_sqs_queue") as mock:
        yield mock


@pytest.mark.env('sqs')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_SQSBasicFunctionality(BasicFunctionality):
    pass


@pytest.mark.env('sqs')
@pytest.mark.flaky(reruns=5, reruns_delay=5)
class test_SQSBaseExchangeTypes(BaseExchangeTypes):
    def test_fanout(self, connection):
        ex = kombu.Exchange('test_fanout', type='fanout')
        test_queue1 = kombu.Queue('fanout1', exchange=ex)
        consumer1 = self._create_consumer(connection, test_queue1)
        test_queue2 = kombu.Queue('fanout2', exchange=ex)
        consumer2 = self._create_consumer(connection, test_queue2)

        with (
            connection as conn,
            conn.channel() as channel,
            consumer1, consumer2
        ):
            self._publish(channel, ex, [test_queue1, test_queue2])
            conn.drain_events(timeout=1)
            conn.drain_events(timeout=1)


@pytest.mark.env('sqs')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_SQSMessage(BaseMessage):
    pass


@pytest.mark.env('sqs')
@pytest.mark.parametrize('error_code', [
    'InvalidParameterValue', 'ReceiptHandleIsInvalid',
])
def test_ack_invalid_receipt_with_backoff(backoff_queue, error_code):
    connection, queue = backoff_queue
    messages = []

    def on_message(body, message):
        messages.append(message)

    with connection.channel() as channel, kombu.Consumer(
        channel, [queue], callbacks=[on_message],
        accept=['json'], prefetch_count=1,
    ):
        producer = kombu.Producer(channel)
        producer.publish(
            'first', routing_key=queue.name, serializer='json',
            headers={'task': 'tasks.example'},
        )
        connection.drain_events(timeout=2)
        message = messages.pop()
        assert message.payload == 'first'

        client = channel.sqs(queue.name)
        queue_url = message.delivery_info['sqs_queue']
        client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message.delivery_info['sqs_message']['ReceiptHandle'],
        )
        delete_responses = []

        def invalid_receipt_response(http_response, parsed, **kwargs):
            # GoAws does not return AWS's invalid-receipt error codes.
            delete_responses.append(http_response.status_code)
            parsed['Error']['Code'] = error_code

        event = 'after-call.sqs.DeleteMessage'
        client.meta.events.register(event, invalid_receipt_response)
        try:
            message.ack()
        finally:
            client.meta.events.unregister(event, invalid_receipt_response)
        assert delete_responses == [404]
        assert message.acknowledged

        producer.publish(
            'second', routing_key=queue.name, serializer='json',
            headers={'task': 'tasks.example'},
        )
        connection.drain_events(timeout=2)
        next_message = messages.pop()
        assert next_message.payload == 'second'
        next_message.ack()
