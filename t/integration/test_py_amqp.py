from __future__ import annotations

import os
import socket
import uuid

import pytest
from amqp.exceptions import NotFound

import kombu
from kombu.connection import ConnectionPool

from .common import (BaseExchangeTypes, BaseFailover, BaseMessage,
                     BasePriority, BaseTimeToLive, BasicFunctionality)


def get_connection(hostname, port, vhost):
    return kombu.Connection(f'pyamqp://{hostname}:{port}')


def get_failover_connection(hostname, port, vhost):
    return kombu.Connection(
        f'pyamqp://localhost:12345;pyamqp://{hostname}:{port}'
    )


def get_confirm_connection(hostname, port):
    return kombu.Connection(
        f"pyamqp://{hostname}:{port}", transport_options={"confirm_publish": True}
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


@pytest.fixture()
def confirm_publish_connection():
    return get_confirm_connection(
        hostname=os.environ.get("RABBITMQ_HOST", "localhost"),
        port=os.environ.get("RABBITMQ_5672_TCP", "5672"),
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


@pytest.mark.env("py-amqp")
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_PyAMQPConnectionPool:
    def test_publish_confirm_does_not_block(self, confirm_publish_connection):
        """Tests that the connection pool closes connections in case of an exception.

        In case an exception occurs while the connection is in use, the pool should
        close the exception. In case the connection is not closed before releasing it
        back to the pool, the connection would remain in an unusable state, causing
        causing the next publish call to time out or block forever in case no
        timeout is specified.
        """
        pool = ConnectionPool(connection=confirm_publish_connection, limit=1)

        try:
            with pool.acquire(block=True) as connection:
                producer = kombu.Producer(connection)
                queue = kombu.Queue(
                    f"test-queue-{uuid.uuid4()}", channel=connection
                )
                queue.declare()
                producer.publish(
                    {"foo": "bar"}, routing_key=str(uuid.uuid4()), retry=False
                )
                assert connection.connected
                queue.delete()
                try:
                    queue.get()
                except NotFound:
                    raise
        except NotFound:
            pass

        with pool.acquire(block=True) as connection:
            assert not connection.connected
            producer = kombu.Producer(connection)
            queue = kombu.Queue(
                f"test-queue-{uuid.uuid4()}", channel=connection
            )
            queue.declare()
            # In case the connection is broken, we should get a Timeout here
            producer.publish(
                {"foo": "bar"}, routing_key=str(uuid.uuid4()), retry=False, timeout=3
            )


@pytest.mark.env('py-amqp')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_PyAMQPConsumerRevive:
    """Regression tests for #814."""

    def test_revive_resumes_active_queue_only(self, connection):
        active_name = f'revive_active_{uuid.uuid4()}'
        inactive_name = f'revive_inactive_{uuid.uuid4()}'
        # default exchange requires routing_key == queue name
        active_queue = kombu.Queue(active_name, routing_key=active_name)
        inactive_queue = kombu.Queue(inactive_name, routing_key=inactive_name)

        received = []

        def callback(body, message):
            received.append(body)
            message.ack()

        def _drain(conn, timeout):
            try:
                conn.drain_events(timeout=timeout)
            except socket.timeout:
                pass

        with connection as conn:
            with conn.channel():
                consumer = kombu.Consumer(
                    conn, [active_queue], accept=['pickle']
                )
                consumer.register_callback(callback)

                with consumer:
                    # registered but never consumed from
                    consumer.add_queue(inactive_queue)
                    assert inactive_queue.name not in consumer._active_tags

                    # kill the socket to simulate the broker vanishing
                    sock = conn.connection.transport.sock
                    sock.shutdown(socket.SHUT_RDWR)
                    sock.close()

                    safe_drain = conn.ensure(consumer, _drain, max_retries=3)
                    safe_drain(conn, 1)

                    assert active_queue.name in consumer._active_tags
                    assert inactive_queue.name not in consumer._active_tags

                    # confirm messages are actually delivered post-recovery
                    producer = kombu.Producer(conn)
                    producer.publish(
                        {'hello': 'world'},
                        retry=True,
                        exchange=active_queue.exchange,
                        routing_key=active_queue.routing_key,
                        declare=[active_queue],
                        serializer='pickle',
                    )
                    safe_drain(conn, 1)

        assert received == [{'hello': 'world'}]
