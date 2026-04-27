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
