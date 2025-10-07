"""Testing module for the kombu.transport.SQS package.

NOTE: The SQSQueueMock and SQSConnectionMock classes originally come from
http://github.com/pcsforeducation/sqs-mock-python. They have been patched
slightly.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from kombu import Connection
from kombu.transport.SQS.SNS import SNS, _SnsSubscription

boto3 = pytest.importorskip('boto3')

from botocore.exceptions import ClientError  # noqa

from kombu.transport import SQS  # noqa

SQS_Channel_sqs = SQS.Channel.sqs

example_predefined_queues = {
    'queue-1':      {
        'url':               'https://sqs.us-east-1.amazonaws.com/xxx/queue-1',
        'access_key_id':     'a',
        'secret_access_key': 'b',
        'backoff_tasks':     ['svc.tasks.tasks.task1'],
        'backoff_policy':    {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640}
    },
    'queue-2':      {
        'url':               'https://sqs.us-east-1.amazonaws.com/xxx/queue-2',
        'access_key_id':     'c',
        'secret_access_key': 'd',
    },
    "queue-3.fifo": {
        "url":               "https://sqs.us-east-1.amazonaws.com/xxx/queue-3.fifo",
        "access_key_id":     "e",
        "secret_access_key": "f",
    },
}

example_predefined_exchanges = {
    "exchange-1":      {
        "arn":               "arn:aws:sns:us-east-1:xxx:exchange-1",
        "access_key_id":     "a",
        "secret_access_key": "b",
    },
    "exchange-2.fifo": {
        "arn":               "arn:aws:sns:us-east-1:xxx:exchange-2",
        "access_key_id":     "a",
        "secret_access_key": "b",
    },
}


@pytest.fixture
def connection_fixture():
    return Connection(
        transport=SQS.Transport,
        transport_options={
            "predefined_queues": example_predefined_queues,
        },
    )


@pytest.fixture
def channel_fixture(connection_fixture) -> SQS.Channel:
    chan = connection_fixture.channel()
    chan.region = "some-aws-region"
    return chan


@pytest.fixture
def mock_sqs():
    with patch("kombu.transport.SQS.Channel.sqs") as mock:
        mock.name = "Sqs client mock"
        yield mock


@pytest.fixture
def mock_fanout():
    with patch("kombu.transport.SQS.Channel.fanout") as mock:
        yield mock


@pytest.fixture
def mock_new_sqs_client():
    with patch("kombu.transport.SQS.Channel.new_sqs_client") as mock:
        yield mock


@pytest.fixture
def sns_fanout(channel_fixture):
    inst = SNS(channel_fixture)

    # Clear previous class vars
    inst._predefined_clients = {}
    inst._topic_arn_cache = {}
    inst._exchange_topic_cache = {}

    return inst


@pytest.fixture
def sns_subscription(sns_fanout):
    inst = _SnsSubscription(sns_fanout)

    # Clear previous class vars
    inst._queue_arn_cache = {}
    inst._subscription_arn_cache = {}

    return inst
