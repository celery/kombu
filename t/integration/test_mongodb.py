from __future__ import annotations

import os

import pytest

import kombu

from .common import (BaseExchangeTypes, BaseMessage, BasePriority,
                     BasicFunctionality)


def get_connection(hostname, port, vhost):
    return kombu.Connection(
        f'mongodb://{hostname}:{port}/{vhost}',
        transport_options={'ttl': True},
    )


@pytest.fixture()
def invalid_connection():
    return kombu.Connection('mongodb://localhost:12345?connectTimeoutMS=1')


@pytest.fixture()
def connection(request):
    return get_connection(
        hostname=os.environ.get('MONGODB_HOST', 'localhost'),
        port=os.environ.get('MONGODB_27017_TCP', '27017'),
        vhost=getattr(
            request.config, "slaveinput", {}
        ).get("slaveid", 'tests'),
    )


@pytest.mark.env('mongodb')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_MongoDBBasicFunctionality(BasicFunctionality):
    pass


@pytest.mark.env('mongodb')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_MongoDBBaseExchangeTypes(BaseExchangeTypes):

    # MongoDB consumer skips old messages upon initialization.
    # Ensure that it's created before test messages are published.

    def test_fanout(self, connection):
        ex = kombu.Exchange('test_fanout', type='fanout')
        test_queue1 = kombu.Queue('fanout1', exchange=ex)
        consumer1 = self._create_consumer(connection, test_queue1)
        test_queue2 = kombu.Queue('fanout2', exchange=ex)
        consumer2 = self._create_consumer(connection, test_queue2)

        with connection as conn:
            with conn.channel() as channel:
                self._publish(channel, ex, [test_queue1, test_queue2])

                self._consume_from(conn, consumer1)
                self._consume_from(conn, consumer2)


@pytest.mark.env('mongodb')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_MongoDBPriority(BasePriority):

    # drain_events() consumes only one value unlike in py-amqp.

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
                    [{'msg': 'first'}, 3],
                    [{'msg': 'second'}, 6],
                    [{'msg': 'third'}, 3],
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
                consumer = kombu.Consumer(
                    conn, [test_queue], accept=['pickle']
                )
                consumer.register_callback(callback)
                with consumer:
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                # Second message must be received first
                assert received_messages[0] == {'msg': 'second'}
                assert received_messages[1] == {'msg': 'first'}
                assert received_messages[2] == {'msg': 'third'}

    def test_publish_requeue_consume(self, connection):
        test_queue = kombu.Queue(
            'priority_requeue_test',
            routing_key='priority_requeue_test', max_priority=10
        )

        received_messages = []
        received_message_bodies = []

        def callback(body, message):
            received_messages.append(message)
            received_message_bodies.append(body)
            # don't ack the message so it can be requeued

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                for msg, prio in [
                    [{'msg': 'first'}, 3],
                    [{'msg': 'second'}, 6],
                    [{'msg': 'third'}, 3],
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

                consumer = kombu.Consumer(
                    conn, [test_queue], accept=['pickle']
                )
                consumer.register_callback(callback)
                with consumer:
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)

                # requeue the messages
                for msg in received_messages:
                    msg.requeue()
                received_messages.clear()
                received_message_bodies.clear()

                # add a fourth higher priority message
                producer.publish(
                    {'msg': 'fourth'},
                    retry=True,
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='pickle',
                    priority=9  # highest priority
                )

                with consumer:
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)

                # Fourth message must be received first
                assert received_message_bodies[0] == {'msg': 'fourth'}
                assert received_message_bodies[1] == {'msg': 'second'}
                assert received_message_bodies[2] == {'msg': 'first'}
                assert received_message_bodies[3] == {'msg': 'third'}


@pytest.mark.env('mongodb')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_MongoDBMessage(BaseMessage):
    pass
