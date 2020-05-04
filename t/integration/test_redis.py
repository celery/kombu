from __future__ import absolute_import, unicode_literals

import os

import pytest
import kombu

from .common import BasicFunctionality, BaseExchangeTypes


def get_connection(
        hostname, port, vhost):
    return kombu.Connection('redis://{}:{}'.format(hostname, port))


@pytest.fixture()
def connection(request):
    # this fixture yields plain connections to broker and TLS encrypted
    return get_connection(
        hostname=os.environ.get('REDIS_HOST', 'localhost'),
        port=os.environ.get('REDIS_6379_TCP', '6379'),
        vhost=getattr(
            request.config, "slaveinput", {}
        ).get("slaveid", None),
    )


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisBasicFunctionality(BasicFunctionality):
    pass


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisBaseExchangeTypes(BaseExchangeTypes):
    pass
