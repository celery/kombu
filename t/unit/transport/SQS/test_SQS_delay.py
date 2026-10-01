"""Tests for per-message delay handling in the SQS transport.

SQS accepts a per-message ``DelaySeconds``, which Kombu forwards from the
message properties. These tests cover the spellings a caller may use and the
range SQS accepts, since anything outside it is rejected at publish time.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from kombu import Exchange, Queue, messaging


@pytest.fixture
def make_publisher(connection_fixture, mock_sqs):
    """Build a publisher on a predefined queue, reporting what SQS was told."""
    def _make(queue_name='queue-2'):
        channel = connection_fixture.channel()
        exchange = Exchange('test_SQS', type='direct')
        Queue(queue_name, exchange, queue_name)(channel).declare()
        client = Mock()
        channel.sqs = Mock(return_value=client)
        producer = messaging.Producer(
            channel, exchange, routing_key=queue_name,
        )

        def publish(**kwargs):
            producer.publish('message', **kwargs)
            sent = dict(client.send_message.call_args[1])
            sent.pop('MessageBody', None)
            return sent

        return publish

    return _make


@pytest.mark.parametrize('delay, expected', [
    (10, 10),
    ('10', 10),
    (900, 900),
    (100000, 900),
    (0, None),
    (-5, None),
    (None, None),
    ('soon', None),
])
def test_delay_seconds_is_forwarded(make_publisher, delay, expected):
    """A delay is sent on as ``DelaySeconds`` when it can be honoured."""
    sent = make_publisher()(delay_seconds=delay)
    if expected is None:
        assert 'DelaySeconds' not in sent
    else:
        assert sent['DelaySeconds'] == expected


def test_aws_spelling_still_works(make_publisher):
    """The delay can also be given under the name the SQS API uses."""
    assert make_publisher()(DelaySeconds=10)['DelaySeconds'] == 10


def test_no_delay_by_default(make_publisher):
    """A message published without a delay carries no delay."""
    assert 'DelaySeconds' not in make_publisher()()


def test_fifo_queue_is_not_delayed(make_publisher):
    """FIFO queues take no per-message delay, and SQS would reject one."""
    assert 'DelaySeconds' not in make_publisher('queue-3.fifo')(delay_seconds=10)


@pytest.mark.parametrize('properties, expected', [
    ({'delay_seconds': 30}, 30),
    ({'DelaySeconds': 30}, 30),
    ({'delay_seconds': '30'}, 30),
    ({'delay_seconds': 900}, 900),
    ({'delay_seconds': 901}, 900),
    ({'delay_seconds': 0}, None),
    ({'delay_seconds': None}, None),
    ({'delay_seconds': 'abc'}, None),
    ({'delay_seconds': 60, 'DelaySeconds': 90}, 60),
    ({}, None),
])
def test_resolve_delay_seconds(channel_fixture, properties, expected):
    """The helper decides what SQS is told, for either spelling."""
    assert channel_fixture._resolve_delay_seconds(properties) == expected
