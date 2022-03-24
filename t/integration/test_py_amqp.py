import os

import pytest

import kombu

from .common import (BaseExchangeTypes, BaseFailover, BaseMessage,
                     BasePriority, BaseTimeToLive, BasicFunctionality)


def get_connection(hostname, port, vhost):
    return kombu.Connection(f'pyamqp://{hostname}:{port}')


def get_failover_connection(hostname, port, vhost):
    return kombu.Connection(
        f'pyamqp://localhost:12345;pyamqp://{hostname}:{port}'
    )


@pytest.fixture()
def invalid_connection():
    return kombu.Connection('pyamqp://localhost:12345')


@pytest.fixture()
def connection(request):
    return get_connection(
        hostname=os.environ.get('RABBITMQ_HOST', 'localhost'),
        port=os.environ.get('RABBITMQ_5672_TCP', '5672'),
        vhost=getattr(
            request.config, "slaveinput", {}
        ).get("slaveid", None),
    )


@pytest.fixture()
def failover_connection(request):
    return get_failover_connection(
        hostname=os.environ.get('RABBITMQ_HOST', 'localhost'),
        port=os.environ.get('RABBITMQ_5672_TCP', '5672'),
        vhost=getattr(
            request.config, "slaveinput", {}
        ).get("slaveid", None),
    )


@pytest.mark.env('py-amqp')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_PyAMQPBasicFunctionality(BasicFunctionality):
    pass


@pytest.mark.env('py-amqp')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_PyAMQPBaseExchangeTypes(BaseExchangeTypes):
    pass


@pytest.mark.env('py-amqp')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_PyAMQPTimeToLive(BaseTimeToLive):
    pass


@pytest.mark.env('py-amqp')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_PyAMQPPriority(BasePriority):
    pass


@pytest.mark.env('py-amqp')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_PyAMQPFailover(BaseFailover):
    pass


@pytest.mark.env('py-amqp')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_PyAMQPMessage(BaseMessage):
    pass
