from __future__ import annotations

import pytest

import kombu

from .common import (BaseExchangeTypes, BaseFailover, BaseMessage,
                     BasicFunctionality)


def get_connection(hostname, port):
    return kombu.Connection(
        f'confluentkafka://{hostname}:{port}',
    )


def get_failover_connection(hostname, port):
    return kombu.Connection(
        f'confluentkafka://localhost:12345;confluentkafka://{hostname}:{port}',
        connect_timeout=10,
    )


@pytest.fixture()
def invalid_connection():
    return kombu.Connection('confluentkafka://localhost:12345')


@pytest.fixture()
def connection():
    return get_connection(
        hostname='localhost',
        port='9092'
    )


@pytest.fixture()
def failover_connection():
    return get_failover_connection(
        hostname='localhost',
        port='9092'
    )


@pytest.mark.env('kafka')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_KafkaBasicFunctionality(BasicFunctionality):
    pass


@pytest.mark.env('kafka')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_KafkaBaseExchangeTypes(BaseExchangeTypes):

    @pytest.mark.skip('fanout is not implemented')
    def test_fanout(self, connection):
        pass


@pytest.mark.env('kafka')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_KafkaFailover(BaseFailover):
    pass


@pytest.mark.env('kafka')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_KafkaMessage(BaseMessage):
    pass
