from __future__ import annotations

import os
import time
import uuid
from unittest.mock import patch

import boto3
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
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_SQSPerConnectionState:
    """Queue URLs are cached per Connection, not process-wide."""

    def test_same_queue_name_routes_to_each_connections_own_queue(self, hub, test_queue_prefix):
        # Two Connections whose predefined_queues map `orders` to different
        # SQS queues: opening a channel on the second must not repoint the
        # first one's `orders` at the second one's queue.
        host = os.environ.get('SQS_HOST', 'localhost')
        port = os.environ.get('SQS_PORT', '4100')
        admin = boto3.client(
            'sqs',
            region_name='us-east-1',
            endpoint_url=f'http://{host}:{port}',
            aws_access_key_id='TestUsername',
            aws_secret_access_key='TestPassword',
        )
        url_a = admin.create_queue(QueueName=f'{test_queue_prefix}orders_a')['QueueUrl']
        url_b = admin.create_queue(QueueName=f'{test_queue_prefix}orders_b')['QueueUrl']

        def connection_for(url):
            conn = get_connection(hostname=host, port=port)
            conn.transport_options['predefined_queues'] = {
                'orders': {
                    'url': url,
                    'access_key_id': 'TestUsername',
                    'secret_access_key': 'TestPassword',
                },
            }
            conn.transport_options['hub'] = hub
            return conn

        with connection_for(url_a) as conn_a, connection_for(url_b) as conn_b:
            channel_a = conn_a.channel()
            conn_b.channel()
            kombu.Producer(channel_a).publish({'sent_by': 'a'}, routing_key='orders')

        received_a = admin.receive_message(QueueUrl=url_a, WaitTimeSeconds=1).get('Messages', [])
        received_b = admin.receive_message(QueueUrl=url_b, WaitTimeSeconds=1).get('Messages', [])
        assert len(received_a) == 1
        assert received_b == []


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


@pytest.mark.env('sqs')
class test_SQSDelaySeconds:
    """Per-message delay reaches SQS from both spellings, coerced and clamped."""

    @pytest.fixture
    def delay_setup(self, connection, test_queue_prefix):
        """One connection for the whole class, plus the params put on the wire.

        The delay is only observable where it is actually consumed, so these
        assertions read the request the transport built rather than the
        properties the caller passed in.

        The recorded entry is the parameter dict handed to botocore, not its
        serialised body: under the ``json`` protocol the body is JSON, but
        under ``query`` it is a Python-repr dict, so parsing the body would
        make the test depend on the wire protocol.
        """
        # The connection applies ``queue_name_prefix`` to whatever routing key
        # it is given, so the queue is created under the prefixed name while
        # publishes address it by the bare name.
        routing_key = 'delayq'
        queue_name = f'{test_queue_prefix}{routing_key}'
        recorded = []

        def record(params, model, **kwargs):
            recorded.append(dict(params))

        with connection as conn:
            channel = conn.default_channel
            client = channel.sqs()
            queue_url = client.create_queue(QueueName=queue_name)['QueueUrl']
            event = 'before-call.sqs.SendMessage'
            client.meta.events.register(event, record)
            try:
                yield routing_key, queue_url, channel, client, recorded
            finally:
                client.meta.events.unregister(event, record)
                client.delete_queue(QueueUrl=queue_url)

    @pytest.mark.parametrize('spelling', ['delay_seconds', 'DelaySeconds'])
    def test_both_spellings_reach_sqs(self, delay_setup, spelling):
        """``delay_seconds`` and ``DelaySeconds`` mean the same thing."""
        routing_key, _url, channel, _client, recorded = delay_setup
        kombu.Producer(channel).publish(
            'delayed', routing_key=routing_key, serializer='json',
            **{spelling: 30},
        )
        assert recorded[-1].get('DelaySeconds') == 30

    def test_numeric_string_is_coerced(self, delay_setup):
        """A quoted number is still a delay, not a publish-time type error."""
        routing_key, _url, channel, _client, recorded = delay_setup
        kombu.Producer(channel).publish(
            'delayed', routing_key=routing_key, serializer='json',
            delay_seconds='30',
        )
        assert recorded[-1].get('DelaySeconds') == 30

    def test_explicit_zero_is_sent(self, delay_setup):
        """SQS accepts a zero delay, so the transport forwards it."""
        routing_key, _url, channel, _client, recorded = delay_setup
        kombu.Producer(channel).publish(
            'delayed', routing_key=routing_key, serializer='json',
            delay_seconds=0,
        )
        assert recorded[-1].get('DelaySeconds') == 0

    @pytest.mark.parametrize('spelling', ['delay_seconds', 'DelaySeconds'])
    def test_out_of_range_is_clamped(self, delay_setup, spelling):
        """An out-of-range delay must not fail the publish with an SQS error."""
        routing_key, _url, channel, _client, recorded = delay_setup
        kombu.Producer(channel).publish(
            'delayed', routing_key=routing_key, serializer='json',
            **{spelling: 100000},
        )
        assert recorded[-1].get('DelaySeconds') == 900

    @pytest.mark.parametrize('value', ['soon', None, -5])
    def test_unusable_values_publish_without_a_delay(self, delay_setup, value):
        """A typo'd delay costs the delay, not the message."""
        routing_key, _url, channel, _client, recorded = delay_setup
        kombu.Producer(channel).publish(
            'immediate', routing_key=routing_key, serializer='json',
            delay_seconds=value,
        )
        assert 'DelaySeconds' not in recorded[-1]

    def test_fifo_queue_sends_no_delay(self, connection, test_queue_prefix):
        """SQS rejects DelaySeconds on a FIFO queue, so none is sent."""
        queue_name = f'{test_queue_prefix}delay.fifo'
        with connection as conn:
            channel = conn.default_channel
            client = channel.sqs()
            queue_url = client.create_queue(
                QueueName=queue_name,
                Attributes={'FifoQueue': 'true'},
            )['QueueUrl']
            recorded = []

            def record(params, model, **kwargs):
                recorded.append(dict(params))

            event = 'before-call.sqs.SendMessage'
            client.meta.events.register(event, record)
            try:
                kombu.Producer(channel).publish(
                    'delayed', routing_key='delay.fifo', serializer='json',
                    delay_seconds=10,
                )
                assert 'DelaySeconds' not in recorded[-1]
            finally:
                client.meta.events.unregister(event, record)
                client.delete_queue(QueueUrl=queue_url)

    def test_no_delay_by_default(self, delay_setup):
        """A publish that asks for nothing must not send a delay."""
        routing_key, _url, channel, _client, recorded = delay_setup
        kombu.Producer(channel).publish(
            'immediate', routing_key=routing_key, serializer='json',
        )
        assert 'DelaySeconds' not in recorded[-1]

    @pytest.mark.flaky(reruns=3, reruns_delay=2)
    def test_delayed_message_is_withheld_until_the_delay_elapses(self, delay_setup):
        """The delay is honoured by the broker, not just sent on the wire."""
        routing_key, queue_url, channel, client, _recorded = delay_setup
        kombu.Producer(channel).publish(
            'delayed', routing_key=routing_key, serializer='json',
            delay_seconds=2,
        )

        def receive():
            return client.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10,
                VisibilityTimeout=1, WaitTimeSeconds=0,
            ).get('Messages', [])

        withheld = [m for m in receive() if m['Body']]
        assert not withheld, 'message became visible before its delay elapsed'

        # Poll rather than sleep a fixed amount: the delay is broker-side, so
        # how long it takes is the broker's business, not this test's.
        deadline = time.monotonic() + 30
        delivered = withheld
        while not delivered and time.monotonic() < deadline:
            time.sleep(0.5)
            delivered = [m for m in receive() if m['Body']]
        assert delivered, 'delayed message never became visible'
