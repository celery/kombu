import os
from time import sleep

import pytest
import redis

import kombu
from kombu.transport.redis import Transport

from .common import (BaseExchangeTypes, BaseMessage, BasePriority,
                     BasicFunctionality)


def get_connection(
        hostname, port, vhost, user_name=None, password=None,
        transport_options=None):

    credentials = f'{user_name}:{password}@' if user_name else ''

    return kombu.Connection(
        f'redis://{credentials}{hostname}:{port}',
        transport_options=transport_options
    )


@pytest.fixture(params=[None, {'global_keyprefix': '_prefixed_'}])
def connection(request):
    # this fixture yields plain connections to broker and TLS encrypted
    return get_connection(
        hostname=os.environ.get('REDIS_HOST', 'localhost'),
        port=os.environ.get('REDIS_6379_TCP', '6379'),
        vhost=getattr(
            request.config, "slaveinput", {}
        ).get("slaveid", None),
        transport_options=request.param
    )


@pytest.fixture()
def invalid_connection():
    return kombu.Connection('redis://localhost:12345')


@pytest.mark.env('redis')
def test_failed_credentials():
    """Tests denied connection when wrong credentials were provided"""
    with pytest.raises(redis.exceptions.ResponseError):
        get_connection(
            hostname=os.environ.get('REDIS_HOST', 'localhost'),
            port=os.environ.get('REDIS_6379_TCP', '6379'),
            vhost=None,
            user_name='wrong_redis_user',
            password='wrong_redis_password'
        ).connect()


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisBasicFunctionality(BasicFunctionality):
    def test_failed_connection__ConnectionError(self, invalid_connection):
        # method raises transport exception
        with pytest.raises(redis.exceptions.ConnectionError) as ex:
            invalid_connection.connection
        assert ex.type in Transport.connection_errors


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisBaseExchangeTypes(BaseExchangeTypes):
    pass


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisPriority(BasePriority):

    # Comparing to py-amqp transport has Redis transport several
    # differences:
    # 1. Order of priorities is reversed
    # 2. drain_events() consumes only single value

    # redis transport has lower numbers higher priority
    PRIORITY_ORDER = 'desc'

    def test_publish_consume(self, connection):
        test_queue = kombu.Queue(
            'priority_test', routing_key='priority_test', max_priority=10
        )

        received_messages = []

        def callback(body, message):
            received_messages.append(body)
            message.ack()

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                for msg, prio in [
                    [{'msg': 'first'}, 6],
                    [{'msg': 'second'}, 3],
                    [{'msg': 'third'}, 6],
                ]:
                    producer.publish(
                        msg,
                        retry=True,
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='pickle',
                        priority=prio
                    )
                # Sleep to make sure that queue sorted based on priority
                sleep(0.5)
                consumer = kombu.Consumer(
                    conn, [test_queue], accept=['pickle']
                )
                consumer.register_callback(callback)
                with consumer:
                    # drain_events() returns just on number in
                    # Virtual transports
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                # Second message must be received first
                assert received_messages[0] == {'msg': 'second'}
                assert received_messages[1] == {'msg': 'first'}
                assert received_messages[2] == {'msg': 'third'}


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisMessage(BaseMessage):
    pass
