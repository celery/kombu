from __future__ import annotations

import os
import socket
from contextlib import closing
from time import sleep, time

import pytest

import kombu
from kombu.exceptions import OperationalError

from .common import BaseExchangeTypes, BaseMessage, BasicFunctionality


def get_connection(hostname, port, database, username='postgres',
                   password='postgres', transport_options=None):
    credentials = f'{username}:{password}@'
    return kombu.Connection(
        f'pgmq://{credentials}{hostname}:{port}/{database}',
        transport_options=transport_options or {
            'wait_time_seconds': 0,
            'polling_interval': 0.1,
            'visibility_timeout': 30,
        },
    )


@pytest.fixture()
def connection(request):
    return get_connection(
        hostname=os.environ.get('PGMQ_HOST', 'localhost'),
        port=os.environ.get('PGMQ_PORT', '5433'),
        database=os.environ.get('PGMQ_DATABASE', 'postgres'),
        username=os.environ.get('PGMQ_USERNAME', 'postgres'),
        password=os.environ.get('PGMQ_PASSWORD', 'postgres'),
    )


@pytest.fixture()
def invalid_connection():
    return kombu.Connection(
        'pgmq://postgres:postgres@127.0.0.1:1/postgres',
        transport_options={
            'max_retries': 1,
            'conn_string': (
                'postgresql://postgres:postgres@127.0.0.1:1/postgres'
                '?connect_timeout=1'
            ),
        },
    )


@pytest.mark.env('pgmq')
@pytest.mark.flaky(reruns=3, reruns_delay=2)
class test_PGMQBasicFunctionality(BasicFunctionality):

    def test_failed_connect(self, invalid_connection):
        with pytest.raises(OperationalError):
            invalid_connection.connect()

    def test_failed_connection(self, invalid_connection):
        with pytest.raises(OperationalError):
            invalid_connection.connection

    def test_failed_channel(self, invalid_connection):
        with pytest.raises(OperationalError):
            invalid_connection.channel()

    def test_failed_default_channel(self, invalid_connection):
        invalid_connection.transport_options = {'max_retries': 1}
        with pytest.raises(OperationalError):
            invalid_connection.default_channel


@pytest.mark.env('pgmq')
@pytest.mark.flaky(reruns=3, reruns_delay=2)
class test_PGMQBaseExchangeTypes(BaseExchangeTypes):
    pass


@pytest.mark.env('pgmq')
@pytest.mark.flaky(reruns=3, reruns_delay=2)
class test_PGMQBaseMessage(BaseMessage):
    pass


@pytest.mark.env('pgmq')
@pytest.mark.flaky(reruns=3, reruns_delay=2)
class test_PGMQTransportSpecific:

    def test_visibility_timeout_redelivery(self, connection):
        """Unacked messages become visible again after visibility timeout."""
        queue_name = 'pgmq_vt_redelivery'
        with connection as conn:
            with closing(conn.SimpleQueue(queue_name)) as queue:
                queue.put({'task': 'retry-me'})

                message = queue.get(timeout=5)
                assert message.payload == {'task': 'retry-me'}
                # Do not ack — let visibility timeout expire.

                deadline = time() + 35
                redelivered = None
                while time() < deadline:
                    try:
                        redelivered = queue.get(timeout=2)
                        break
                    except queue.Empty:
                        sleep(0.5)

                assert redelivered is not None
                assert redelivered.payload == {'task': 'retry-me'}
                redelivered.ack()

    def test_bulk_prefetch_consume(self, connection):
        """Prefetch bulk reads deliver multiple messages per poll."""
        queue_name = 'pgmq_bulk_prefetch'
        test_queue = kombu.Queue(queue_name, routing_key=queue_name)
        received = []

        def callback(body, message):
            received.append(body)
            message.ack()

        with connection as conn:
            conn.transport_options['wait_time_seconds'] = 0
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                for i in range(3):
                    producer.publish(
                        {'n': i},
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='json',
                    )

                consumer = kombu.Consumer(
                    conn, [test_queue], accept=['json'], prefetch_count=3,
                )
                consumer.register_callback(callback)
                with consumer:
                    conn.drain_events(timeout=10)

        assert sorted(received, key=lambda x: x['n']) == [
            {'n': 0}, {'n': 1}, {'n': 2},
        ]

    def test_reject_requeue_immediate(self, connection):
        with connection as conn:
            with closing(conn.SimpleQueue('pgmq_reject_requeue')) as queue:
                queue.put({'hello': 'again'})
                message = queue.get(timeout=5)
                message.reject(requeue=True)

                redelivered = queue.get(timeout=5)
                assert redelivered.payload == {'hello': 'again'}
                redelivered.ack()

    def test_consume_empty_queue(self, connection):
        def callback(body, message):
            assert False, 'Callback should not be called'

        test_queue = kombu.Queue('pgmq_empty', routing_key='pgmq_empty')
        with connection as conn:
            conn.transport_options['wait_time_seconds'] = 0
            with conn.channel():
                consumer = kombu.Consumer(conn, [test_queue], accept=['json'])
                consumer.register_callback(callback)
                with consumer:
                    with pytest.raises(socket.timeout):
                        conn.drain_events(timeout=1)

    def test_delayed_delivery(self, connection):
        """Messages published with expiration are delayed in PGMQ."""
        queue_name = 'pgmq_delayed'
        test_queue = kombu.Queue(queue_name, routing_key=queue_name)

        with connection as conn:
            conn.transport_options['wait_time_seconds'] = 0
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                producer.publish(
                    {'task': 'later'},
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='json',
                    expiration=2,
                )

            with closing(conn.SimpleQueue(queue_name)) as queue:
                with pytest.raises(queue.Empty):
                    queue.get(timeout=0.5)

                message = queue.get(timeout=5)
                assert message.payload == {'task': 'later'}
                message.ack()

    def test_fifo_grouped_reads(self, connection):
        """FIFO grouped reads preserve per-group ordering."""
        queue_name = 'pgmq_fifo_grouped'
        test_queue = kombu.Queue(
            queue_name,
            routing_key=queue_name,
            queue_arguments={'x-pgmq-fifo-index': True},
        )

        with connection as conn:
            conn.transport_options.update({
                'wait_time_seconds': 0,
                'fifo_mode': 'grouped',
            })
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                producer.publish(
                    {'group': 'a', 'n': 1},
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='json',
                    MessageGroupId='a',
                )
                producer.publish(
                    {'group': 'a', 'n': 2},
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    serializer='json',
                    MessageGroupId='a',
                )
                producer.publish(
                    {'group': 'b', 'n': 1},
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    serializer='json',
                    MessageGroupId='b',
                )

            received = []
            with closing(conn.SimpleQueue(queue_name)) as queue:
                for _ in range(3):
                    message = queue.get(timeout=5)
                    received.append(message.payload)
                    message.ack()

            assert received == [
                {'group': 'a', 'n': 1},
                {'group': 'a', 'n': 2},
                {'group': 'b', 'n': 1},
            ]

    def test_notify_wakeup(self, connection):
        """NOTIFY/LISTEN wakes drain_events without a full polling sleep."""
        queue_name = 'pgmq_notify'
        test_queue = kombu.Queue(queue_name, routing_key=queue_name)
        received = []

        def callback(body, message):
            received.append(body)
            message.ack()

        with connection as conn:
            conn.transport_options.update({
                'wait_time_seconds': 0,
                'use_notify': True,
                'notify_throttle_interval_ms': 0,
                'polling_interval': 5,
            })
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                consumer = kombu.Consumer(
                    conn, [test_queue], accept=['json'],
                )
                consumer.register_callback(callback)
                with consumer:
                    producer.publish(
                        {'hello': 'notify'},
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='json',
                    )
                    conn.drain_events(timeout=5)

        assert received == [{'hello': 'notify'}]

    def test_partitioned_queue(self, connection):
        """Partitioned queues require pg_partman in the PGMQ image."""
        queue_name = 'pgmq_partitioned'
        test_queue = kombu.Queue(
            queue_name,
            routing_key=queue_name,
            queue_arguments={
                'x-pgmq-partitioned': True,
                'x-pgmq-partition-interval': 100,
                'x-pgmq-retention-interval': 1000,
            },
        )

        try:
            with connection as conn:
                conn.transport_options['wait_time_seconds'] = 0
                with conn.channel() as channel:
                    producer = kombu.Producer(channel)
                    producer.publish(
                        {'partitioned': True},
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='json',
                    )

                with closing(conn.SimpleQueue(queue_name)) as queue:
                    message = queue.get(timeout=5)
                    assert message.payload == {'partitioned': True}
                    message.ack()
        except Exception as exc:
            if 'pg_partman' in str(exc):
                pytest.skip('pg_partman is not available in this PGMQ image')
            raise