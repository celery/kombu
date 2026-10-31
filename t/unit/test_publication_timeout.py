"""Whole publication budgets, independent of RabbitMQ blocked callbacks."""
from __future__ import annotations

import socket
from time import monotonic
from unittest.mock import Mock

import amqp
import pytest

from kombu import Connection, Producer
from kombu.exceptions import OperationalError, PublicationTimeout
from kombu.pools import ProducerPool
from kombu.utils.functional import ChannelPromise, retry_over_time
from kombu.utils.time import (Deadline, current_publication_deadline,
                              publication_timeout)

requires_operation_timeout = pytest.mark.skipif(
    not hasattr(amqp, 'operation_timeout'),
    reason='requires py-amqp operation_timeout support',
)


@pytest.mark.parametrize('value', [-1, True, '1', float('nan'), float('inf'), 1e20, 10**1000])
def test_invalid_or_zero_does_not_resolve_connection(value):
    connection = Connection('amqp://')
    connection._establish_connection = Mock()
    producer = Producer(connection)
    with pytest.raises(ValueError):
        producer.publish({}, publication_timeout=value)
    connection._establish_connection.assert_not_called()


def test_zero_budget_does_not_start_publication():
    connection = Connection('amqp://')
    connection._establish_connection = Mock()
    with pytest.raises(PublicationTimeout):
        Producer(connection).publish({}, publication_timeout=0)
    connection._establish_connection.assert_not_called()


@pytest.mark.parametrize('nested', [False, True])
def test_pool_waits_are_bounded_and_recover(nested):
    connection = Connection('amqp://')
    connections = connection.Pool(limit=1)
    pool = ProducerPool(connections, limit=1)
    held = connections.acquire() if nested else pool.acquire()
    try:
        started = monotonic()
        with pytest.raises(PublicationTimeout), publication_timeout(.04):
            pool.acquire(block=True)
        assert .03 <= monotonic() - started < .5
        assert len(pool._dirty) == (0 if nested else 1)
    finally:
        held.release()
    with pool.acquire(block=True, timeout=.1):
        pass
    pool.force_close_all()
    connections.force_close_all()


@pytest.mark.parametrize('cached', [False, True])
def test_producer_acquire_timeout_preserves_other_connection_owner(cached):
    connections = Connection('amqp://').Pool(limit=1)
    producers = ProducerPool(connections, limit=1)
    try:
        if cached:
            with producers.acquire():
                pass
        with connections.acquire() as held:
            for _ in range(3):
                with pytest.raises(PublicationTimeout), publication_timeout(.02):
                    producers.acquire(block=True)
                assert connections._dirty == {held}
                assert connections._resource.qsize() == 0
                assert not producers._dirty
                assert producers._resource.qsize() == 1
                with pytest.raises(connections.LimitExceeded):
                    connections.acquire(block=False)
        with producers.acquire(block=True, timeout=.1) as producer:
            assert producer.__connection__ is held
            assert connections._dirty == {held}
        assert not connections._dirty
        assert connections._resource.qsize() == 1
    finally:
        producers.force_close_all()
        connections.force_close_all()


def test_retry_sleep_uses_remaining_budget(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr('kombu.utils.time.monotonic', lambda: clock[0])
    sleep = Mock(side_effect=lambda interval: clock.__setitem__(0, clock[0] + interval))
    monkeypatch.setattr('kombu.utils.functional.sleep', sleep)
    fun = Mock(side_effect=socket.error('not connected'))
    with pytest.raises(PublicationTimeout):
        retry_over_time(fun, socket.error, interval_start=60, interval_max=60,
                        deadline=Deadline(.2))
    fun.assert_called_once()
    sleep.assert_called_once_with(.2)


def test_total_timeout_is_terminal_even_in_retry_errors(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr('kombu.utils.time.monotonic', lambda: clock[0])
    connection = Connection('amqp://')
    obj = Mock()

    def fail():
        clock[0] = .2
        raise OSError('broken')

    fun = Mock(side_effect=fail)
    fun.__name__ = 'publish'
    with pytest.raises(PublicationTimeout), publication_timeout(.1):
        connection.ensure(obj, fun, retry_errors=(Exception,))()
    fun.assert_called_once()
    obj.revive.assert_not_called()


@requires_operation_timeout
def test_writer_lock_timeout_keeps_usable_connection():
    connection = Connection('amqp://')
    fun = Mock(side_effect=amqp.OperationTimeout('writer busy', connection_usable=True))
    fun.__name__ = 'publish'
    connection.collect = Mock()
    with pytest.raises(PublicationTimeout) as info, publication_timeout(1):
        connection.ensure(Mock(), fun, retry_errors=(Exception,))()
    assert info.value.connection_usable
    connection.collect.assert_not_called()
    fun.assert_called_once()


@pytest.mark.parametrize('discarded', [False, True])
@pytest.mark.parametrize('budget', [None, 1])
def test_channel_error_reconnects_discarded_connection_with_one_retry(
        discarded, budget):
    connection = Connection('amqp://')
    connection.collect = Mock()
    connection._ensure_connection = Mock()
    connection._default_channel = Mock()
    producer = Mock()
    error = amqp.exceptions.ResourceLocked('queue in use')
    if discarded:
        error.connection_usable = False
    attempts = []

    def publish():
        attempts.append(True)
        if len(attempts) == 1:
            raise error
        if discarded and not producer.revive.called:
            raise amqp.RecoverableConnectionError('connection closed')
        return 'published'

    with publication_timeout(budget):
        assert connection.ensure(producer, publish, max_retries=1)() == 'published'
    assert len(attempts) == 2
    if discarded:
        producer.revive.assert_called_once_with(connection._default_channel)
        connection.collect.assert_called_once_with()
    else:
        producer.revive.assert_not_called()
        connection.collect.assert_not_called()


@pytest.mark.parametrize('error_type,max_retries', [
    (amqp.exceptions.ResourceLocked, 0),
    (amqp.exceptions.NotFound, 1),
    (amqp.exceptions.PreconditionFailed, 1),
])
def test_discarded_channel_error_preserves_retry_limit_and_class(
        error_type, max_retries):
    connection = Connection('amqp://')
    connection.collect = Mock()
    producer = Mock()
    error = error_type('broker reason')
    error.connection_usable = False
    publish = Mock(side_effect=error)
    publish.__name__ = 'publish'
    expected = OperationalError if max_retries == 0 else error_type
    with pytest.raises(expected) as info, publication_timeout(1):
        connection.ensure(producer, publish, max_retries=max_retries)()
    assert (info.value.__cause__ if max_retries == 0 else info.value) is error
    publish.assert_called_once()
    producer.revive.assert_not_called()
    connection.collect.assert_not_called()


def test_nested_scope_inherits_and_restores(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr('kombu.utils.time.monotonic', lambda: clock[0])
    with publication_timeout(1) as outer:
        clock[0] = .2
        with publication_timeout(9) as inner:
            assert inner is outer
        with publication_timeout(.1) as inner:
            assert inner.expires == pytest.approx(.3)
        assert current_publication_deadline() is outer
    assert current_publication_deadline() is None


@requires_operation_timeout
def test_options_never_become_message_properties():
    connection = Connection('amqp://')
    producer = Producer(connection)
    producer._publish = Mock()
    producer.publish({}, publication_timeout=1, custom_property='ok')
    properties = producer._publish.call_args.args[5]
    assert properties['custom_property'] == 'ok'
    assert 'publication_timeout' not in properties


def test_unsupported_transport_fails_before_channel_acquisition():
    connection = Connection('memory://')
    connection._establish_connection = Mock()
    with pytest.raises(NotImplementedError, match='pyamqp'):
        Producer(connection).publish({}, publication_timeout=1)
    connection._establish_connection.assert_not_called()


def test_old_amqp_fails_explicitly_only_when_enabled(monkeypatch):
    monkeypatch.delattr(amqp, 'operation_timeout', raising=False)
    producer = Producer(Connection('amqp://'))
    producer._publish = Mock()
    producer.publish({})
    producer._publish.assert_called_once()
    with pytest.raises(NotImplementedError, match='py-amqp'):
        producer.publish({}, publication_timeout=1)
    producer._publish.assert_called_once()


def test_arbitrary_lazy_channel_is_not_evaluated():
    resolve = Mock()
    producer = Producer(ChannelPromise(resolve))
    with pytest.raises(NotImplementedError, match='ChannelPromise'):
        producer.publish({}, publication_timeout=1)
    resolve.assert_not_called()


def test_batch_rejected_before_deferred_publish():
    producer = Producer(Connection('amqp://'))
    producer._publish = Mock()
    with producer.batch():
        with pytest.raises(NotImplementedError, match='batch'):
            producer.publish({}, publication_timeout=1)
    producer._publish.assert_not_called()


def test_old_amqp_rejection_keeps_pooled_connection(monkeypatch):
    monkeypatch.delattr(amqp, 'operation_timeout', raising=False)
    connections = Connection('amqp://').Pool(limit=1)
    connection = connections.acquire()
    connection._close = Mock(side_effect=AssertionError('must retain healthy connection'))
    connection.release()
    producers = ProducerPool(connections, limit=1)
    with pytest.raises(NotImplementedError) as info, publication_timeout(1):
        with producers.acquire() as producer:
            producer.publish({})
    assert info.value.connection_usable
    assert not producers._dirty
    assert not connections._dirty
    with connections.acquire() as same:
        assert same is connection
    connection._close.assert_not_called()


def test_pooled_connection_context_preserves_writer_lock_owner():
    connections = Connection('amqp://').Pool(limit=1)
    connection = connections.acquire()
    connection._close = Mock(side_effect=AssertionError('must retain active writer'))
    with pytest.raises(PublicationTimeout):
        with connection:
            raise PublicationTimeout(connection_usable=True)
    connection._close.assert_not_called()
    assert not connections._dirty
    with connections.acquire() as same:
        assert same is connection


@pytest.mark.parametrize('phase', [
    'collect', 'establish', 'errback', 'revive', 'on_revive',
])
@pytest.mark.parametrize('usable', [False, True])
@requires_operation_timeout
def test_retry_recovery_keeps_terminal_timeout(phase, usable):
    connection = Connection('amqp://')
    connection.collect = Mock()
    connection._ensure_connection = Mock()
    connection._default_channel = Mock()
    producer = Mock()
    error = amqp.OperationTimeout('recovery expired', connection_usable=usable)
    callback = Mock(side_effect=error)
    options = {}
    if phase == 'collect':
        connection.collect = callback
    elif phase == 'establish':
        connection._ensure_connection = callback
    elif phase == 'errback':
        options['errback'] = callback
    elif phase == 'revive':
        producer.revive = callback
    else:
        options['on_revive'] = callback
    publish = Mock(side_effect=socket.timeout('earlier confirm timeout'))
    publish.__name__ = 'publish'
    ensured = connection.ensure(producer, publish, max_retries=99, **options)
    with publication_timeout(1), connection._publication_timeout():
        with pytest.raises(PublicationTimeout) as caught:
            ensured()
    assert caught.value.__cause__ is error
    assert caught.value.connection_usable is usable
    assert publish.call_count == callback.call_count == 1


@pytest.mark.parametrize('enabled', [False, True])
def test_error_conversion_keeps_unrelated_errors(enabled):
    from kombu.exceptions import OperationalError

    connection = Connection('amqp://')
    error = ConnectionRefusedError('ordinary connection failure')
    with publication_timeout(1 if enabled else None):
        with pytest.raises(OperationalError) as caught:
            with connection._reraise_as_library_errors():
                raise error
    assert type(caught.value) is OperationalError
    assert caught.value.__cause__ is error


@requires_operation_timeout
def test_operation_error_conversion_disabled_without_publication_budget():
    from kombu.exceptions import OperationalError

    connection = Connection('amqp://')
    error = amqp.OperationTimeout('explicit lower-layer deadline')
    with pytest.raises(OperationalError) as caught:
        with connection._reraise_as_library_errors():
            raise error
    assert type(caught.value) is OperationalError
    assert caught.value.__cause__ is error
