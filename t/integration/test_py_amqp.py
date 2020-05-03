from __future__ import absolute_import, unicode_literals

import os

import pytest
import kombu

from .common import BasicFunctionality


def get_connection(
        hostname, port, vhost):
    return kombu.Connection('amqp://{}:{}'.format(hostname, port))


@pytest.fixture()
def connection(request):
    # this fixture yields plain connections to broker and TLS encrypted
    return get_connection(
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
