from __future__ import absolute_import, unicode_literals

from contextlib import closing
import os

import pytest
import kombu


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
def test_connect(connection):
    connection.connect()
    connection.close()


@pytest.mark.env('py-amqp')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
def test_publish_consume(connection):
    test_queue = kombu.Queue('test', routing_key='test')

    def callback(body, message):
        assert body == {'hello': 'world'}
        assert message.content_type == 'application/x-python-serialize'
        assert message.delivery_info['routing_key'] == 'test'
        assert message.delivery_info['exchange'] == ''
        assert message.headers == {'k1': 'v1'}
        assert message.payload == body

        message.ack()

    with connection as conn:
        with conn.channel() as channel:
            producer = kombu.Producer(channel)
            producer.publish(
                {'hello': 'world'},
                retry=True,
                headers={'k1': 'v1'},
                exchange=test_queue.exchange,
                routing_key=test_queue.routing_key,
                declare=[test_queue],
                serializer='pickle'
            )

            consumer = kombu.Consumer(conn, [test_queue], accept=['pickle'])
            consumer.register_callback(callback)
            with consumer:
                conn.drain_events(timeout=1)


@pytest.mark.env('py-amqp')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
def test_simple_publish_consume(connection):
    with connection as conn:
        with closing(conn.SimpleQueue('simple_test')) as queue:
            queue.put({'Hello': 'World'}, headers={'k1': 'v1'})
            message = queue.get(timeout=1)
            assert message.payload == {'Hello': 'World'}
            assert message.content_type == 'application/json'
            assert message.content_encoding == 'utf-8'
            assert message.headers == {'k1': 'v1'}
            message.ack()
