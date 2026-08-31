from __future__ import annotations

import os
import socket
from time import sleep

import pytest
import redis

import kombu
from kombu.transport.redis import SUBCLIENT_MAX_MISSED_HEALTH_CHECKS, Transport
from kombu.utils.json import loads

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
def redis_client(connection):
    """Direct Redis client for verification."""
    conn_info = connection.info()
    host = conn_info['hostname'] or 'localhost'
    port = conn_info['port'] or 6379

    return redis.Redis(
        host=host,
        port=port,
        decode_responses=True
    )


@pytest.fixture()
def invalid_connection():
    return kombu.Connection('redis://localhost:12345')


@pytest.mark.env('redis')
def test_failed_credentials():
    """Tests denied connection when wrong credentials were provided"""
    with pytest.raises(redis.exceptions.AuthenticationError):
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
                    # drain_events() consumes only one value unlike in py-amqp.
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
                    priority=0  # highest priority
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


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisPublishBatch:

    def test_retry_on_timeout_keeps_publication_immediate(self, connection):
        connection.transport_options = {
            **connection.transport_options,
            'retry_on_timeout': True,
        }
        queue = kombu.Queue('batch_retry_on_timeout_queue')

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel, serializer='json')

                assert producer.supports_batch_publish is False
                with producer.batch():
                    producer.publish(
                        {'delivery': 'immediate'},
                        exchange='',
                        routing_key=queue.name,
                        declare=[queue],
                    )
                    message = queue(channel).get(no_ack=True)

        assert message.payload == {'delivery': 'immediate'}

    def test_manual_flush_sends_and_batch_continues(self, connection):
        queue = kombu.Queue('batch_manual_flush_queue')

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel, serializer='json')
                bound_queue = queue(channel)

                with producer.batch() as batch:
                    producer.publish(
                        {'position': 'first'},
                        exchange='',
                        routing_key=queue.name,
                        declare=[queue],
                    )
                    batch.flush()
                    first = bound_queue.get(no_ack=True)
                    producer.publish(
                        {'position': 'second'},
                        exchange='',
                        routing_key=queue.name,
                    )

                second = bound_queue.get(no_ack=True)

        assert first.payload == {'position': 'first'}
        assert second.payload == {'position': 'second'}

    def test_direct_priority_and_fifo(self, connection):
        exchange = kombu.Exchange('batch_direct_exchange', type='direct')
        queue = kombu.Queue(
            'batch_direct_queue',
            exchange=exchange,
            routing_key='batch.direct',
            max_priority=10,
        )

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                with producer.batch():
                    for body, priority in [
                        ({'position': 'first'}, 6),
                        ({'position': 'second'}, 3),
                        ({'position': 'third'}, 6),
                    ]:
                        producer.publish(
                            body,
                            exchange=exchange,
                            routing_key='batch.direct',
                            declare=[queue],
                            serializer='json',
                            priority=priority,
                        )

                bound_queue = queue(channel)
                received = [
                    bound_queue.get(no_ack=True).payload
                    for _ in range(3)
                ]

        assert received == [
            {'position': 'second'},
            {'position': 'first'},
            {'position': 'third'},
        ]

    def test_topic_routing(self, connection):
        exchange = kombu.Exchange('batch_topic_exchange', type='topic')
        queue = kombu.Queue(
            'batch_topic_queue',
            exchange=exchange,
            routing_key='events.*',
        )

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                with producer.batch():
                    producer.publish(
                        {'event': 'matching'},
                        exchange=exchange,
                        routing_key='events.created',
                        declare=[queue],
                        serializer='json',
                    )
                    producer.publish(
                        {'event': 'other'},
                        exchange=exchange,
                        routing_key='other.created',
                        serializer='json',
                    )

                bound_queue = queue(channel)
                message = bound_queue.get(no_ack=True)
                assert message.payload == {'event': 'matching'}
                assert bound_queue.get(no_ack=True) is None

    def test_fanout_is_deferred_until_flush(self, connection, redis_client):
        exchange = kombu.Exchange('batch_fanout_exchange', type='fanout')

        with connection as conn:
            with conn.channel() as channel:
                channel.pool
                topic = channel._get_publish_topic(
                    exchange.name,
                    'worker.created',
                )
                keyprefix = connection.transport_options.get(
                    'global_keyprefix',
                    '',
                )
                with redis_client.pubsub() as subscriber:
                    subscriber.subscribe(f'{keyprefix}{topic}')
                    subscribed = subscriber.get_message(timeout=1)
                    assert subscribed['type'] == 'subscribe'

                    producer = kombu.Producer(channel)
                    with producer.batch():
                        producer.publish(
                            {'event': 'fanout'},
                            exchange=exchange,
                            routing_key='worker.created',
                            declare=[exchange],
                            serializer='json',
                        )
                        assert subscriber.get_message(timeout=0.05) is None

                    published = subscriber.get_message(timeout=1)

        assert published['type'] == 'message'
        message = loads(published['data'])
        assert message['properties']['delivery_info'] == {
            'exchange': exchange.name,
            'routing_key': 'worker.created',
        }


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisMessage(BaseMessage):
    pass


@pytest.mark.env('redis')
def test_RedisConnectTimeout(monkeypatch):
    # simulate a connection timeout for a new connection
    def connect_timeout(self):
        raise socket.timeout
    monkeypatch.setattr(
        redis.connection.Connection, "_connect", connect_timeout)

    # ensure the timeout raises a TimeoutError
    with pytest.raises(redis.exceptions.TimeoutError):
        # note the host/port here is irrelevant because
        # connect will raise a socket.timeout
        kombu.Connection('redis://localhost:12345').connect()


@pytest.mark.env('redis')
def test_RedisConnection_check_hostname(monkeypatch):
    # simulate a connection timeout for a new connection
    def connect_check_certificate(self):
        if self.check_hostname:
            raise OSError("check_hostname=True")
        raise socket.timeout("check_hostname=False")
    monkeypatch.setattr(
        redis.connection.SSLConnection, "_connect", connect_check_certificate)

    # ensure the timeout raises a TimeoutError
    with pytest.raises(redis.exceptions.TimeoutError):
        # note the host/port here is irrelevant because
        # connect will raise a socket.timeout, not a CertificateError
        kombu.Connection('rediss://localhost:12345?ssl_check_hostname=false').connect()
    with pytest.raises(redis.exceptions.ConnectionError):
        # note the host/port here is irrelevant because
        # connect will raise a CertificateError due to hostname mismatch
        kombu.Connection('rediss://localhost:12345?ssl_check_hostname=true').connect()


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisQueueExpiration:
    """Integration tests for Redis queue expiration feature."""

    def test_queue_expiration_set(self, connection, redis_client):
        """Test that expiration is set correctly on queue using direct expires parameter."""
        expires_ms = 2000
        expires_sec = expires_ms / 1000

        test_queue = kombu.Queue(
            'expire_test_queue',
            routing_key='expire_test_queue',
            expires=expires_sec
        )

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                producer.publish(
                    {'msg': 'test message'},
                    retry=True,
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='json'
                )

        keyprefix = connection.transport_options.get('global_keyprefix', '')
        queue_key = f"{keyprefix}{test_queue.name}"
        ttl = redis_client.pttl(queue_key)
        assert ttl > 0 and ttl <= expires_ms, f"Expected TTL to be set but got {ttl}"

        sleep(expires_sec + 5)
        assert redis_client.pttl(queue_key) == -2, "Queue key should be gone after TTL"

    def test_expiration_gets_reset_on_put(self, connection, redis_client):
        """Test that expiration gets reset when putting new message to queue."""
        expires_ms = 5000
        expires_sec = expires_ms / 1000

        test_queue = kombu.Queue(
            'expire_reset_test_queue',
            routing_key='expire_reset_test_queue',
            expires=expires_sec
        )

        keyprefix = connection.transport_options.get('global_keyprefix', '')
        queue_key = f"{keyprefix}{test_queue.name}"

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                producer.publish(
                    {'msg': 'first message'},
                    retry=True,
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='json'
                )

                sleep(expires_ms / 2000)  # Wait for half the TTL
                producer.publish(
                    {'msg': 'second message'},
                    retry=True,
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='json'
                )

        ttl = redis_client.pttl(queue_key)
        assert ttl > 0, "TTL should be updated after publishing a new message"

    def test_expiration_gets_reset_on_get(self, connection, redis_client):
        """Test that expiration gets reset when getting a message from queue."""
        expires_ms = 5000
        expires_sec = expires_ms / 1000

        test_queue = kombu.Queue(
            'expire_get_test_queue',
            routing_key='expire_get_test_queue',
            expires=expires_sec
        )

        keyprefix = connection.transport_options.get('global_keyprefix', '')
        queue_key = f"{keyprefix}{test_queue.name}"

        received_messages = []

        def callback(body, message):
            received_messages.append(body)
            message.ack()

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                consumer = kombu.Consumer(
                    conn, [test_queue], accept=['json']
                )
                consumer.register_callback(callback)

                for i in range(3):
                    producer.publish(
                        {'msg': f'message {i}'},
                        retry=True,
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='json'
                    )

                # Wait for some time but not enough for queue to expire
                sleep(expires_ms / 2000)  # Wait for half the TTL
                with consumer:
                    conn.drain_events(timeout=1)

        assert len(received_messages) == 1, "Should have received one message"
        ttl = redis_client.pttl(queue_key)
        assert ttl > 0, "TTL should be updated after consuming a message"

    def test_queue_expires_for_all_priorities(self, connection, redis_client):
        """Test that expiration is set for all priority queues."""
        expires_ms = 2000
        expires_sec = expires_ms / 1000

        test_queue = kombu.Queue(
            'expire_priority_test_queue',
            routing_key='expire_priority_test_queue',
            expires=expires_sec,
            max_priority=10
        )

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                for priority in [0, 3, 6, 9]:
                    producer.publish(
                        {'msg': f'priority {priority} message'},
                        retry=True,
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='json',
                        priority=priority
                    )

        keyprefix = connection.transport_options.get('global_keyprefix', '')
        pattern = f"{keyprefix}{test_queue.name}*"
        priority_keys = list(redis_client.scan_iter(match=pattern))
        assert priority_keys, "Expected to find queue keys with priorities"

        for key in priority_keys:
            ttl = redis_client.pttl(key)
            assert ttl > 0 and ttl <= expires_ms, f"Expected TTL for {key} to be set but got {ttl}"


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisSubclientHealthCheck:
    """Integration tests for dropping half-open fanout (pub/sub) connections.

    On a half-open socket (managed broker failover, expired NAT entry) the
    health check PING write lands in the kernel buffer and no error is
    raised, so the subclient health check alone never notices the dead
    connection.  kombu counts unanswered pings through redis-py's
    ``health_check_response_counter`` and drops the connection once two
    full health check intervals pass with no PONG.
    """

    def _fanout_consumer(self, conn, name, received):
        """Return a channel subscribed to a fanout exchange over pub/sub."""
        exchange = kombu.Exchange(f'{name}_exchange', type='fanout')
        queue = kombu.Queue(f'{name}_queue', exchange=exchange)
        channel = conn.channel()
        consumer = kombu.Consumer(
            channel, [queue], accept=['json'], no_ack=True)
        consumer.register_callback(
            lambda body, message: received.append(body))
        consumer.consume()
        # first drain registers the subclient with the poller (LISTEN mode)
        # and sends SUBSCRIBE; it may return on the subscribe confirmation
        # or time out waiting for one
        try:
            conn.drain_events(timeout=0.1)
        except socket.timeout:
            pass
        return channel, exchange

    def test_healthy_subclient_connection_not_dropped(self, connection):
        """A pub/sub connection with no missed PONGs must stay up."""
        received = []
        with connection as conn:
            channel, exchange = self._fanout_consumer(
                conn, 'health_live', received)
            subclient = channel.__dict__['subclient']
            assert subclient.connection._sock is not None

            conn.transport.cycle.maybe_check_subclient_health()

            # below the missed-pong threshold the connection is kept
            assert subclient.connection._sock is not None

            producer = kombu.Producer(channel)
            producer.publish(
                {'msg': 'alive'}, exchange=exchange, serializer='json')
            conn.drain_events(timeout=1)

        assert received == [{'msg': 'alive'}]

    def test_missed_pongs_drop_connection_and_resubscribe(self, connection):
        """Unanswered PINGs drop the connection; the next poll resubscribes."""
        received = []
        with connection as conn:
            channel, exchange = self._fanout_consumer(
                conn, 'health_drop', received)
            subclient = channel.__dict__['subclient']
            assert subclient.connection._sock is not None

            # two health check intervals passed without a PONG: the socket
            # is half-open and the connection must be dropped
            subclient.health_check_response_counter = \
                SUBCLIENT_MAX_MISSED_HEALTH_CHECKS

            conn.transport.cycle.maybe_check_subclient_health()

            assert subclient.connection._sock is None
            assert subclient.health_check_response_counter == 0

            # the next poll cycle reconnects and resubscribes
            try:
                conn.drain_events(timeout=0.1)
            except socket.timeout:
                pass

            # fanout messages flow again
            producer = kombu.Producer(channel)
            producer.publish(
                {'msg': 'recovered'}, exchange=exchange, serializer='json')
            conn.drain_events(timeout=1)

        assert received == [{'msg': 'recovered'}]
