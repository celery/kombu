from __future__ import absolute_import, unicode_literals

import pytest
import socket

from amqp import RecoverableConnectionError
from case import ContextMock, Mock, patch

from kombu import common
from kombu.common import (
    Broadcast, maybe_declare,
    send_reply, collect_replies,
    declaration_cached, ignore_errors,
    QoS, PREFETCH_COUNT_MAX, generate_oid
)

from t.mocks import MockPool


def test_generate_oid():
    from uuid import NAMESPACE_OID
    from kombu.five import bytes_if_py2

    instance = Mock()

    args = (1, 1001, 2001, id(instance))
    ent = bytes_if_py2('%x-%x-%x-%x' % args)

    with patch('kombu.common.uuid3') as mock_uuid3, \
            patch('kombu.common.uuid5') as mock_uuid5:
        mock_uuid3.side_effect = ValueError
        mock_uuid3.return_value = 'uuid3-6ba7b812-9dad-11d1-80b4'
        mock_uuid5.return_value = 'uuid5-6ba7b812-9dad-11d1-80b4'
        oid = generate_oid(1, 1001, 2001, instance)
        mock_uuid5.assert_called_once_with(NAMESPACE_OID, ent)
        assert oid == 'uuid5-6ba7b812-9dad-11d1-80b4'


def test_ignore_errors():
    connection = Mock()
    connection.channel_errors = (KeyError,)
    connection.connection_errors = (KeyError,)

    with ignore_errors(connection):
        raise KeyError()

    def raising():
        raise KeyError()

    ignore_errors(connection, raising)

    connection.channel_errors = connection.connection_errors = ()

    with pytest.raises(KeyError):
        with ignore_errors(connection):
            raise KeyError()


class test_declaration_cached:

    def test_when_cached(self):
        chan = Mock()
        chan.connection.client.declared_entities = ['foo']
        assert declaration_cached('foo', chan)

    def test_when_not_cached(self):
        chan = Mock()
        chan.connection.client.declared_entities = ['bar']
        assert not declaration_cached('foo', chan)


class test_Broadcast:

    def test_arguments(self):
        with patch('kombu.common.uuid',
                   return_value='test') as uuid_mock:
            q = Broadcast(name='test_Broadcast')
            uuid_mock.assert_called_with()
            assert q.name == 'bcast.test'
            assert q.alias == 'test_Broadcast'
            assert q.auto_delete
            assert q.exchange.name == 'test_Broadcast'
            assert q.exchange.type == 'fanout'

        q = Broadcast('test_Broadcast', 'explicit_queue_name')
        assert q.name == 'explicit_queue_name'
        assert q.exchange.name == 'test_Broadcast'

        q2 = q(Mock())
        assert q2.name == q.name

        with patch('kombu.common.uuid',
                   return_value='test') as uuid_mock:
            q = Broadcast('test_Broadcast',
                          'explicit_queue_name',
                          unique=True)
            uuid_mock.assert_called_with()
            assert q.name == 'explicit_queue_name.test'

            q2 = q(Mock())
            assert q2.name.split('.')[0] == q.name.split('.')[0]


class test_maybe_declare:

    def _get_mock_channel(self):
        # Given: A mock Channel with mock'd connection/client/entities
        channel = Mock()
        channel.connection.client.declared_entities = set()
        return channel

    def _get_mock_entity(self, is_bound=False, can_cache_declaration=True):
        # Given: Unbound mock Entity (will bind to channel when bind called
        entity = Mock()
        entity.can_cache_declaration = can_cache_declaration
        entity.is_bound = is_bound

        def _bind_entity(channel):
            entity.channel = channel
            entity.is_bound = True
            return entity
        entity.bind = _bind_entity
        return entity

    def test_cacheable(self):
        # Given: A mock Channel and mock entity
        channel = self._get_mock_channel()
        # Given: A mock Entity that is already bound
        entity = self._get_mock_entity(
            is_bound=True, can_cache_declaration=True)
        entity.channel = channel
        entity.auto_delete = False
        assert entity.is_bound, "Expected entity is bound to begin this test."

        # When: Calling maybe_declare default
        maybe_declare(entity, channel)

        # Then: It called declare on the entity queue and added it to list
        assert entity.declare.call_count == 1
        assert hash(entity) in channel.connection.client.declared_entities

        # When: Calling maybe_declare default (again)
        maybe_declare(entity, channel)
        # Then: we did not call declare again because its already in our list
        assert entity.declare.call_count == 1

        # When: Entity channel connection has gone away
        entity.channel.connection = None
        # Then: maybe_declare must raise a RecoverableConnectionError
        with pytest.raises(RecoverableConnectionError):
            maybe_declare(entity)

    def test_binds_entities(self):
        # Given: A mock Channel and mock entity
        channel = self._get_mock_channel()
        # Given: A mock Entity that is not bound
        entity = self._get_mock_entity()
        assert not entity.is_bound, "Expected entity unbound to begin test."

        # When: calling maybe_declare with default of no retry policy
        maybe_declare(entity, channel)

        # Then: the entity is now bound because it called to bind it
        assert entity.is_bound is True, "Expected entity is now marked bound."

    def test_binds_entities_when_retry_policy(self):
        # Given: A mock Channel and mock entity
        channel = self._get_mock_channel()
        # Given: A mock Entity that is not bound
        entity = self._get_mock_entity()
        assert not entity.is_bound, "Expected entity unbound to begin test."

        # Given: A retry policy
        sample_retry_policy = {
            'interval_start': 0,
            'interval_max': 1,
            'max_retries': 3,
            'interval_step': 0.2,
            'errback': lambda x: "Called test errback retry policy",
        }

        # When: calling maybe_declare with retry enabled
        maybe_declare(entity, channel, retry=True, **sample_retry_policy)

        # Then: the entity is now bound because it called to bind it
        assert entity.is_bound is True, "Expected entity is now marked bound."

    def test_with_retry(self):
        # Given: A mock Channel and mock entity
        channel = self._get_mock_channel()
        # Given: A mock Entity that is already bound
        entity = self._get_mock_entity(
            is_bound=True, can_cache_declaration=True)
        entity.channel = channel
        assert entity.is_bound, "Expected entity is bound to begin this test."
        # When calling maybe_declare with retry enabled (default policy)
        maybe_declare(entity, channel, retry=True)
        # Then: the connection client used ensure to ensure the retry policy
        assert channel.connection.client.ensure.call_count

    def test_with_retry_dropped_connection(self):
        # Given: A mock Channel and mock entity
        channel = self._get_mock_channel()
        # Given: A mock Entity that is already bound
        entity = self._get_mock_entity(
            is_bound=True, can_cache_declaration=True)
        entity.channel = channel
        assert entity.is_bound, "Expected entity is bound to begin this test."
        # When: Entity channel connection has gone away
        entity.channel.connection = None
        # When: calling maybe_declare with retry
        # Then: the RecoverableConnectionError should be raised
        with pytest.raises(RecoverableConnectionError):
            maybe_declare(entity, channel, retry=True)


class test_replies:

    def test_send_reply(self):
        req = Mock()
        req.content_type = 'application/json'
        req.content_encoding = 'binary'
        req.properties = {'reply_to': 'hello',
                          'correlation_id': 'world'}
        channel = Mock()
        exchange = Mock()
        exchange.is_bound = True
        exchange.channel = channel
        producer = Mock()
        producer.channel = channel
        producer.channel.connection.client.declared_entities = set()
        send_reply(exchange, req, {'hello': 'world'}, producer)

        assert producer.publish.call_count
        args = producer.publish.call_args
        assert args[0][0] == {'hello': 'world'}
        assert args[1] == {
            'exchange': exchange,
            'routing_key': 'hello',
            'correlation_id': 'world',
            'serializer': 'json',
            'retry': False,
            'retry_policy': None,
            'content_encoding': 'binary',
        }

    @patch('kombu.common.itermessages')
    def test_collect_replies_with_ack(self, itermessages):
        conn, channel, queue = Mock(), Mock(), Mock()
        body, message = Mock(), Mock()
        itermessages.return_value = [(body, message)]
        it = collect_replies(conn, channel, queue, no_ack=False)
        m = next(it)
        assert m is body
        itermessages.assert_called_with(conn, channel, queue, no_ack=False)
        message.ack.assert_called_with()

        with pytest.raises(StopIteration):
            next(it)

        channel.after_reply_message_received.assert_called_with(queue.name)

    @patch('kombu.common.itermessages')
    def test_collect_replies_no_ack(self, itermessages):
        conn, channel, queue = Mock(), Mock(), Mock()
        body, message = Mock(), Mock()
        itermessages.return_value = [(body, message)]
        it = collect_replies(conn, channel, queue)
        m = next(it)
        assert m is body
        itermessages.assert_called_with(conn, channel, queue, no_ack=True)
        message.ack.assert_not_called()

    @patch('kombu.common.itermessages')
    def test_collect_replies_no_replies(self, itermessages):
        conn, channel, queue = Mock(), Mock(), Mock()
        itermessages.return_value = []
        it = collect_replies(conn, channel, queue)
        with pytest.raises(StopIteration):
            next(it)
        channel.after_reply_message_received.assert_not_called()


class test_insured:

    @patch('kombu.common.logger')
    def test_ensure_errback(self, logger):
        common._ensure_errback('foo', 30)
        logger.error.assert_called()

    def test_revive_connection(self):
        on_revive = Mock()
        channel = Mock()
        common.revive_connection(Mock(), channel, on_revive)
        on_revive.assert_called_with(channel)

        common.revive_connection(Mock(), channel, None)

    def get_insured_mocks(self, insured_returns=('works', 'ignored')):
        conn = ContextMock()
        pool = MockPool(conn)
        fun = Mock()
        insured = conn.autoretry.return_value = Mock()
        insured.return_value = insured_returns
        return conn, pool, fun, insured

    def test_insured(self):
        conn, pool, fun, insured = self.get_insured_mocks()

        ret = common.insured(pool, fun, (2, 2), {'foo': 'bar'})
        assert ret == 'works'
        conn.ensure_connection.assert_called_with(
            errback=common._ensure_errback,
        )

        insured.assert_called()
        i_args, i_kwargs = insured.call_args
        assert i_args == (2, 2)
        assert i_kwargs == {'foo': 'bar', 'connection': conn}

        conn.autoretry.assert_called()
        ar_args, ar_kwargs = conn.autoretry.call_args
        assert ar_args == (fun, conn.default_channel)
        assert ar_kwargs.get('on_revive')
        assert ar_kwargs.get('errback')

    def test_insured_custom_errback(self):
        conn, pool, fun, insured = self.get_insured_mocks()

        custom_errback = Mock()
        common.insured(pool, fun, (2, 2), {'foo': 'bar'},
                       errback=custom_errback)
        conn.ensure_connection.assert_called_with(errback=custom_errback)


class MockConsumer(object):
    consumers = set()

    def __init__(self, channel, queues=None, callbacks=None, **kwargs):
        self.channel = channel
        self.queues = queues
        self.callbacks = callbacks

    def __enter__(self):
        self.consumers.add(self)
        return self

    def __exit__(self, *exc_info):
        self.consumers.discard(self)


class test_itermessages:

    class MockConnection(object):
        should_raise_timeout = False

        def drain_events(self, **kwargs):
            if self.should_raise_timeout:
                raise socket.timeout()
            for consumer in MockConsumer.consumers:
                for callback in consumer.callbacks:
                    callback('body', 'message')

    def test_default(self):
        conn = self.MockConnection()
        channel = Mock()
        channel.connection.client = conn
        conn.Consumer = MockConsumer
        it = common.itermessages(conn, channel, 'q', limit=1)

        ret = next(it)
        assert ret == ('body', 'message')

        with pytest.raises(StopIteration):
            next(it)

    def test_when_raises_socket_timeout(self):
        conn = self.MockConnection()
        conn.should_raise_timeout = True
        channel = Mock()
        channel.connection.client = conn
        conn.Consumer = MockConsumer
        it = common.itermessages(conn, channel, 'q', limit=1)

        with pytest.raises(StopIteration):
            next(it)

    @patch('kombu.common.deque')
    def test_when_raises_IndexError(self, deque):
        deque_instance = deque.return_value = Mock()
        deque_instance.popleft.side_effect = IndexError()
        conn = self.MockConnection()
        channel = Mock()
        conn.Consumer = MockConsumer
        it = common.itermessages(conn, channel, 'q', limit=1)

        with pytest.raises(StopIteration):
            next(it)


class test_QoS:

    class _QoS(QoS):
        def __init__(self, value):
            self.value = value
            QoS.__init__(self, None, value)

        def set(self, value):
            return value

    def test_qos_exceeds_16bit(self):
        with patch('kombu.common.logger') as logger:
            callback = Mock()
            qos = QoS(callback, 10)
            qos.prev = 100
            # cannot use 2 ** 32 because of a bug on macOS Py2.5:
            # https://jira.mongodb.org/browse/PYTHON-389
            qos.set(4294967296)
            logger.warning.assert_called()
            callback.assert_called_with(prefetch_count=0)

    def test_qos_increment_decrement(self):
        qos = self._QoS(10)
        assert qos.increment_eventually() == 11
        assert qos.increment_eventually(3) == 14
        assert qos.increment_eventually(-30) == 14
        assert qos.decrement_eventually(7) == 7
        assert qos.decrement_eventually() == 6

    def test_qos_disabled_increment_decrement(self):
        qos = self._QoS(0)
        assert qos.increment_eventually() == 0
        assert qos.increment_eventually(3) == 0
        assert qos.increment_eventually(-30) == 0
        assert qos.decrement_eventually(7) == 0
        assert qos.decrement_eventually() == 0
        assert qos.decrement_eventually(10) == 0

    def test_qos_thread_safe(self):
        qos = self._QoS(10)

        def add():
            for i in range(1000):
                qos.increment_eventually()

        def sub():
            for i in range(1000):
                qos.decrement_eventually()

        def threaded(funs):
            from threading import Thread
            threads = [Thread(target=fun) for fun in funs]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        threaded([add, add])
        assert qos.value == 2010

        qos.value = 1000
        threaded([add, sub])  # n = 2
        assert qos.value == 1000

    def test_exceeds_short(self):
        qos = QoS(Mock(), PREFETCH_COUNT_MAX - 1)
        qos.update()
        assert qos.value == PREFETCH_COUNT_MAX - 1
        qos.increment_eventually()
        assert qos.value == PREFETCH_COUNT_MAX
        qos.increment_eventually()
        assert qos.value == PREFETCH_COUNT_MAX + 1
        qos.decrement_eventually()
        assert qos.value == PREFETCH_COUNT_MAX
        qos.decrement_eventually()
        assert qos.value == PREFETCH_COUNT_MAX - 1

    def test_consumer_increment_decrement(self):
        mconsumer = Mock()
        qos = QoS(mconsumer.qos, 10)
        qos.update()
        assert qos.value == 10
        mconsumer.qos.assert_called_with(prefetch_count=10)
        qos.decrement_eventually()
        qos.update()
        assert qos.value == 9
        mconsumer.qos.assert_called_with(prefetch_count=9)
        qos.decrement_eventually()
        assert qos.value == 8
        mconsumer.qos.assert_called_with(prefetch_count=9)
        assert {'prefetch_count': 9} in mconsumer.qos.call_args

        # Does not decrement 0 value
        qos.value = 0
        qos.decrement_eventually()
        assert qos.value == 0
        qos.increment_eventually()
        assert qos.value == 0

    def test_consumer_decrement_eventually(self):
        mconsumer = Mock()
        qos = QoS(mconsumer.qos, 10)
        qos.decrement_eventually()
        assert qos.value == 9
        qos.value = 0
        qos.decrement_eventually()
        assert qos.value == 0

    def test_set(self):
        mconsumer = Mock()
        qos = QoS(mconsumer.qos, 10)
        qos.set(12)
        assert qos.prev == 12
        qos.set(qos.prev)
