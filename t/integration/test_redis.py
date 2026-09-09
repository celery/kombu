from __future__ import annotations

import os
import socket
from time import monotonic, sleep
from unittest.mock import patch
from uuid import uuid4

import pytest
import redis

import kombu
from kombu.asynchronous.hub import Hub
from kombu.transport.redis import (SUBCLIENT_MAX_MISSED_HEALTH_CHECKS, Channel,
                                   SentinelChannel, Transport)
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
def test_event_loop_consumes_after_connection_reconnect(connection):
    queue = kombu.Queue(f'reconnect-{uuid4().hex}')
    received = []

    def on_message(body, message):
        received.append(body)
        message.ack()

    hub = Hub()
    try:
        with connection:
            with kombu.Consumer(connection, queues=[queue], callbacks=[on_message]) as consumer:
                connection.register_with_event_loop(hub)
                for tick in hub.on_tick:
                    tick()
                cycle = connection.transport.cycle
                redis_connection = consumer.channel.client.connection
                try:
                    for sequence in range(3):
                        # Reconnect before the next poll tick, as redis-py can do
                        # internally while retrying a command or health check.
                        redis_connection.disconnect()
                        redis_connection.connect()
                        with connection.clone() as publisher:
                            kombu.Producer(publisher).publish(sequence, routing_key=queue.name)
                        deadline = monotonic() + 5
                        while len(received) <= sequence and monotonic() < deadline:
                            for tick in hub.on_tick:
                                tick()
                            for fd, _ in hub.poller.poll(0.1):
                                callback, args = hub.readers[fd]
                                callback(*args)
                        assert received == list(range(sequence + 1))
                        assert 'subclient' not in consumer.channel.__dict__
                finally:
                    queue(consumer.channel).delete()
        assert not cycle._chan_to_sock
        assert not cycle.fds
        assert not hub.readers
    finally:
        hub.close()


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
class test_RedisRestoreVisible:
    """Ack-emulation restores unacked messages after ``visibility_timeout``.

    ``restore_visible`` scans the unacked index with
    ``ZRANGE ... BYSCORE REV`` (the replacement for the deprecated
    ``ZREVRANGEBYSCORE``, see #2050), so this drives that query against a
    real server with and without ``global_keyprefix``.
    """

    def test_restore_visible_requeues_expired_unacked(
            self, connection, redis_client):
        visibility_timeout = 1
        # Private unacked keys so the sweep cannot touch messages that other
        # (possibly concurrent) tests are holding unacked.
        unacked_key = 'restore_visible_test_unacked'
        unacked_index_key = 'restore_visible_test_unacked_index'
        unacked_mutex_key = 'restore_visible_test_unacked_mutex'
        connection = connection.clone(transport_options={
            **connection.transport_options,
            'visibility_timeout': visibility_timeout,
            'unacked_key': unacked_key,
            'unacked_index_key': unacked_index_key,
            'unacked_mutex_key': unacked_mutex_key,
        })
        keyprefix = connection.transport_options.get('global_keyprefix', '')
        unacked_key = f'{keyprefix}{unacked_key}'
        unacked_index_key = f'{keyprefix}{unacked_index_key}'
        unacked_mutex_key = f'{keyprefix}{unacked_mutex_key}'
        # Clear leftovers from an earlier run.  A stale mutex in particular
        # would make restore_visible() skip the sweep until its TTL expires.
        redis_client.delete(unacked_key, unacked_index_key, unacked_mutex_key)

        test_queue = kombu.Queue(
            'restore_visible_test', routing_key='restore_visible_test'
        )
        payload = {'msg': 'restore me'}

        with connection as conn:
            with conn.channel() as channel:
                bound_queue = test_queue(channel)
                bound_queue.declare()
                bound_queue.purge()

                kombu.Producer(channel).publish(
                    payload,
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    serializer='json',
                )

                message = bound_queue.get(no_ack=False)
                assert message.payload == payload
                tag = message.delivery_tag
                assert redis_client.hexists(unacked_key, tag)
                assert redis_client.zscore(unacked_index_key, tag) is not None

                # Still within the visibility timeout: nothing is restored.
                channel.qos.restore_visible(interval=1)
                assert bound_queue.get(no_ack=True) is None
                assert redis_client.hexists(unacked_key, tag)

                sleep(visibility_timeout + 0.5)
                channel.qos.restore_visible(interval=1)

                assert not redis_client.hexists(unacked_key, tag)
                assert redis_client.zscore(unacked_index_key, tag) is None
                restored = bound_queue.get(no_ack=True)
                assert restored is not None
                assert restored.payload == payload
                assert restored.headers['redelivered'] is True


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


class _LegacySentinelChannel(Channel):
    """Behave like the ``SentinelChannel`` of kombu < 5.4.0.

    Those versions never substituted the database number into the fanout
    prefix, so their PUB/SUB topics are literally ``/{db}.<exchange>``.
    """

    def _get_pool(self, asynchronous=False):
        return redis.ConnectionPool(**self._connparams(asynchronous))


class _LegacySentinelTransport(Transport):
    Channel = _LegacySentinelChannel


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisSentinelFanoutCompat:
    """``sentinel_fanout_compat`` keeps fanout working across kombu versions.

    kombu < 5.4.0 sentinel workers use the literal ``/{db}.<exchange>``
    PUB/SUB topic while newer ones use ``/<db>.<exchange>``, so broadcast
    messages such as Celery control commands stop flowing between them
    (celery/kombu#2152).

    The integration environment runs no Sentinel, so master discovery is
    bypassed and ``SentinelChannel`` connects straight to the Redis server;
    the topic selection, PUB/SUB handling and de-duplication under test are
    the real code.  The old side of the conversation is played by
    :class:`_LegacySentinelChannel`.
    """

    @pytest.fixture(autouse=True)
    def direct_sentinel_channel(self):
        def direct_pool(channel, asynchronous=False):
            return redis.ConnectionPool(**channel._connparams(asynchronous))

        with patch.object(SentinelChannel, 'connection_class',
                          redis.Connection), \
                patch.object(SentinelChannel, '_sentinel_managed_pool',
                             direct_pool):
            yield

    @staticmethod
    def _host_port():
        return (os.environ.get('REDIS_HOST', 'localhost'),
                os.environ.get('REDIS_6379_TCP', '6379'))

    def _sentinel_connection(self, **transport_options):
        host, port = self._host_port()
        transport_options.setdefault('master_name', 'mymaster')
        return kombu.Connection(
            f'sentinel://{host}:{port}/0',
            transport_options=transport_options,
        )

    def _legacy_connection(self):
        """Return a connection behaving like a kombu < 5.4.0 sentinel worker."""
        host, port = self._host_port()
        return kombu.Connection(
            f'redis://{host}:{port}/0', transport=_LegacySentinelTransport)

    @staticmethod
    def _consume(channel, exchange, queue_name, received):
        """Consume *exchange* on *channel*, collecting bodies in *received*."""
        queue = kombu.Queue(queue_name, exchange=exchange)
        consumer = kombu.Consumer(
            channel, [queue], accept=['json'], no_ack=True)
        consumer.register_callback(
            lambda body, message: received.append(body))
        consumer.consume()
        return consumer

    @staticmethod
    def _drain(conn, timeout):
        """Handle events on *conn* for *timeout* seconds.

        ``drain_events()`` returns on any readable event, including
        subscription confirmations, so keep draining until the time is
        up and let the tests assert on what was received.
        """
        deadline = monotonic() + timeout
        while (remaining := deadline - monotonic()) > 0:
            try:
                conn.drain_events(timeout=remaining)
            except socket.timeout:
                return

    @classmethod
    def _subscribe(cls, conn):
        # the first drain registers the pub/sub connection with the poller
        # and sends PSUBSCRIBE; give the server time to confirm it.
        cls._drain(conn, 0.2)

    @staticmethod
    def _publish(conn, exchange, body):
        # declare the exchange on the publishing channel, otherwise the
        # virtual transport treats an unknown exchange as a direct one.
        kombu.Producer(
            conn.default_channel, exchange=exchange, serializer='json',
        ).publish(body, declare=[exchange])

    def test_legacy_consumer_receives_compat_publisher(self):
        exchange = kombu.Exchange('sfc_compat_to_legacy', type='fanout')
        received = []
        with self._legacy_connection() as legacy, \
                self._sentinel_connection(sentinel_fanout_compat=True) as new:
            self._consume(legacy, exchange, 'sfc_compat_to_legacy_q', received)
            self._subscribe(legacy)

            self._publish(new, exchange, {'cmd': 'ping'})
            self._drain(legacy, 1)

        assert received == [{'cmd': 'ping'}]

    def test_compat_consumer_receives_legacy_publisher(self):
        exchange = kombu.Exchange('sfc_legacy_to_compat', type='fanout')
        received = []
        with self._legacy_connection() as legacy, \
                self._sentinel_connection(sentinel_fanout_compat=True) as new:
            self._consume(new, exchange, 'sfc_legacy_to_compat_q', received)
            self._subscribe(new)

            self._publish(legacy, exchange, {'cmd': 'ping'})
            self._drain(new, 1)

        assert received == [{'cmd': 'ping'}]

    def test_compat_peers_receive_each_message_once(self):
        exchange = kombu.Exchange('sfc_compat_to_compat', type='fanout')
        received = []
        with self._sentinel_connection(sentinel_fanout_compat=True) as one, \
                self._sentinel_connection(sentinel_fanout_compat=True) as two:
            self._consume(one, exchange, 'sfc_compat_to_compat_q', received)
            self._subscribe(one)

            # published to both topics, so it arrives twice on the
            # subscription connection and must be delivered once.
            self._publish(two, exchange, {'cmd': 'ping'})
            self._drain(one, 1)

        assert received == [{'cmd': 'ping'}]

    def test_without_compat_legacy_and_current_topics_are_isolated(self):
        # the situation reported in celery/kombu#2152
        exchange = kombu.Exchange('sfc_isolated', type='fanout')
        received = []
        with self._legacy_connection() as legacy, \
                self._sentinel_connection() as new:
            self._consume(new, exchange, 'sfc_isolated_new_q', received)
            self._subscribe(new)
            self._publish(legacy, exchange, {'cmd': 'ping'})
            self._drain(new, 1)

            self._consume(legacy, exchange, 'sfc_isolated_legacy_q', received)
            self._subscribe(legacy)
            self._publish(new, exchange, {'cmd': 'ping'})
            self._drain(legacy, 1)

        assert received == []

    def test_cancelled_consumer_no_longer_receives(self):
        """Cancelling must PUNSUBSCRIBE from both topics.

        A leaked pattern subscription would deliver the message published
        after the cancellation for an exchange the channel no longer
        consumes from.
        """
        cancelled = kombu.Exchange('sfc_cancelled', type='fanout')
        kept = kombu.Exchange('sfc_kept', type='fanout')
        received = []
        with self._sentinel_connection(sentinel_fanout_compat=True) as new, \
                self._legacy_connection() as legacy:
            consumer = self._consume(
                new, cancelled, 'sfc_cancelled_q', received)
            self._consume(new, kept, 'sfc_kept_q', received)
            self._subscribe(new)

            consumer.cancel()
            self._publish(legacy, cancelled, {'cmd': 'stale'})
            self._publish(legacy, kept, {'cmd': 'ping'})
            self._drain(new, 1)

        assert received == [{'cmd': 'ping'}]


@pytest.mark.env('redis')
class test_SentinelManagerClose:
    """``_disconnect_pools()`` must close both Sentinel managers.

    celery/kombu#1108: when a sentinel node goes away, kombu never
    closes the socket whose peer is gone.  The integration environment
    runs no Sentinel, so the Sentinel constructor is patched to return a
    sentinel-like object whose ``master_for`` delegates to a real Redis
    connection (enabling channel setup), while exposing ``close()`` for
    verification.
    """

    @staticmethod
    def _host_port():
        return (os.environ.get('REDIS_HOST', 'localhost'),
                os.environ.get('REDIS_6379_TCP', '6379'))

    def test_disconnect_pools_closes_both_managers(self):
        host, port = self._host_port()

        class PatchedSentinel:
            def __init__(self, sentinels, **kw):
                self.closed = False

            def master_for(self, service_name, redis_class=redis.Redis, **kw):
                return redis_class(host=host, port=int(port))

            def slave_for(self, service_name, redis_class=redis.Redis, **kw):
                return redis_class(host=host, port=int(port))

            def close(self):
                self.closed = True

        with patch('redis.sentinel.Sentinel', PatchedSentinel):
            connection = kombu.Connection(
                f'sentinel://{host}:{port}/0',
                transport_options={'master_name': 'mymaster'},
            )
            channel = connection.channel()
            # trigger creation of both managers
            _ = channel.client  # async manager
            _ = channel.pool   # sync manager

            async_mgr = channel._async_sentinel_manager
            sync_mgr = channel._sentinel_manager
            assert isinstance(async_mgr, PatchedSentinel)
            assert isinstance(sync_mgr, PatchedSentinel)
            assert not async_mgr.closed
            assert not sync_mgr.closed

            channel._disconnect_pools()

            assert async_mgr.closed
            assert sync_mgr.closed
            assert channel._async_sentinel_manager is None
            assert channel._sentinel_manager is None
