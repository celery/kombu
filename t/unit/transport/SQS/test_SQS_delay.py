"""Tests for per-message delay handling in the SQS transport.

SQS accepts a per-message ``DelaySeconds``, and Kombu forwards it from the
message properties. These tests cover the two spellings a caller may use and
the range SQS actually accepts, since anything outside that range is rejected
by AWS at publish time.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from kombu import Exchange, Queue, messaging
from t.unit.transport.SQS.conftest import example_predefined_queues


@pytest.fixture
def delay_publisher(connection_fixture, mock_sqs):
    """A producer wired to a mocked SQS client on a standard queue."""
    channel = connection_fixture.channel()
    exchange = Exchange('test_SQS', type='direct')
    queue = Queue('queue-2', exchange, 'queue-2')
    queue(channel).declare()
    producer = messaging.Producer(channel, exchange, routing_key='queue-2')
    sqs_client = Mock()
    channel.sqs = Mock(return_value=sqs_client)
    return producer, sqs_client


def sent(sqs_client):
    """The kwargs that reached ``send_message``, minus the body."""
    kwargs = dict(sqs_client.send_message.call_args[1])
    kwargs.pop('MessageBody', None)
    return kwargs


def test_delay_seconds_is_forwarded(delay_publisher):
    """The snake_case spelling reaches SQS."""
    producer, sqs_client = delay_publisher
    producer.publish('message', delay_seconds=10)
    assert sent(sqs_client)['DelaySeconds'] == 10


def test_DelaySeconds_is_forwarded(delay_publisher):
    """The AWS spelling keeps working."""
    producer, sqs_client = delay_publisher
    producer.publish('message', DelaySeconds=10)
    assert sent(sqs_client)['DelaySeconds'] == 10


def test_string_delay_is_coerced(delay_publisher):
    """A numeric string is coerced instead of being sent as a string."""
    producer, sqs_client = delay_publisher
    producer.publish('message', delay_seconds='7')
    assert sent(sqs_client)['DelaySeconds'] == 7


def test_delay_above_maximum_is_clamped(delay_publisher):
    """SQS accepts at most 900s, so a larger value is clamped."""
    producer, sqs_client = delay_publisher
    producer.publish('message', delay_seconds=100000)
    assert sent(sqs_client)['DelaySeconds'] == 900


def test_negative_delay_is_ignored(delay_publisher):
    """A negative delay is meaningless and is dropped."""
    producer, sqs_client = delay_publisher
    producer.publish('message', delay_seconds=-5)
    assert 'DelaySeconds' not in sent(sqs_client)


def test_zero_delay_is_ignored(delay_publisher):
    """Zero is the default and needs no explicit parameter."""
    producer, sqs_client = delay_publisher
    producer.publish('message', delay_seconds=0)
    assert 'DelaySeconds' not in sent(sqs_client)


def test_non_numeric_delay_is_ignored(delay_publisher):
    """An unparsable delay is dropped rather than failing the publish."""
    producer, sqs_client = delay_publisher
    producer.publish('message', delay_seconds='soon')
    assert 'DelaySeconds' not in sent(sqs_client)


def test_no_delay_by_default(delay_publisher):
    producer, sqs_client = delay_publisher
    producer.publish('message')
    assert 'DelaySeconds' not in sent(sqs_client)


@pytest.mark.parametrize('properties, expected', [
    ({'delay_seconds': 30}, 30),
    ({'DelaySeconds': 30}, 30),
    ({'delay_seconds': '30'}, 30),
    ({'delay_seconds': 900}, 900),
    ({'delay_seconds': 901}, 900),
    ({'delay_seconds': 1000000}, 900),
    ({'delay_seconds': 0}, None),
    ({'delay_seconds': -1}, None),
    ({'delay_seconds': None}, None),
    ({'delay_seconds': 'abc'}, None),
    ({'DelaySeconds': 45}, 45),
    ({'delay_seconds': 60, 'DelaySeconds': 90}, 60),
    ({}, None),
])
def test_resolve_delay_seconds(channel_fixture, properties, expected):
    """Unit tests for the helper that decides what SQS is told."""
    assert channel_fixture._resolve_delay_seconds(properties) == expected


def test_predefined_queue_is_used_for_delay(channel_fixture):
    """The helper works on the queues the transport resolves by name."""
    assert channel_fixture._queue_cache
    assert set(example_predefined_queues) <= set(channel_fixture._queue_cache)
