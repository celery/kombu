from __future__ import absolute_import, unicode_literals

import pickle
import pytest

from case import Mock, call

from kombu import Connection, Exchange, Producer, Queue, binding
from kombu.abstract import MaybeChannelBound
from kombu.exceptions import NotBoundError
from kombu.serialization import registry

from t.mocks import Transport


def get_conn():
    return Connection(transport=Transport)


class test_binding:

    def test_constructor(self):
        x = binding(
            Exchange('foo'), 'rkey',
            arguments={'barg': 'bval'},
            unbind_arguments={'uarg': 'uval'},
        )
        assert x.exchange == Exchange('foo')
        assert x.routing_key == 'rkey'
        assert x.arguments == {'barg': 'bval'}
        assert x.unbind_arguments == {'uarg': 'uval'}

    def test_declare(self):
        chan = get_conn().channel()
        x = binding(Exchange('foo'), 'rkey')
        x.declare(chan)
        assert 'exchange_declare' in chan

    def test_declare_no_exchange(self):
        chan = get_conn().channel()
        x = binding()
        x.declare(chan)
        assert 'exchange_declare' not in chan

    def test_bind(self):
        chan = get_conn().channel()
        x = binding(Exchange('foo'))
        x.bind(Exchange('bar')(chan))
        assert 'exchange_bind' in chan

    def test_unbind(self):
        chan = get_conn().channel()
        x = binding(Exchange('foo'))
        x.unbind(Exchange('bar')(chan))
        assert 'exchange_unbind' in chan

    def test_repr(self):
        b = binding(Exchange('foo'), 'rkey')
        assert 'foo' in repr(b)
        assert 'rkey' in repr(b)


class test_Exchange:

    def test_bound(self):
        exchange = Exchange('foo', 'direct')
        assert not exchange.is_bound
        assert '<unbound' in repr(exchange)

        chan = get_conn().channel()
        bound = exchange.bind(chan)
        assert bound.is_bound
        assert bound.channel is chan
        assert 'bound to chan:%r' % (chan.channel_id,) in repr(bound)

    def test_hash(self):
        assert hash(Exchange('a')) == hash(Exchange('a'))
        assert hash(Exchange('a')) != hash(Exchange('b'))

    def test_can_cache_declaration(self):
        assert Exchange('a', durable=True).can_cache_declaration
        assert Exchange('a', durable=False).can_cache_declaration
        assert not Exchange('a', auto_delete=True).can_cache_declaration
        assert not Exchange(
            'a', durable=True, auto_delete=True,
        ).can_cache_declaration

    def test_pickle(self):
        e1 = Exchange('foo', 'direct')
        e2 = pickle.loads(pickle.dumps(e1))
        assert e1 == e2

    def test_eq(self):
        e1 = Exchange('foo', 'direct')
        e2 = Exchange('foo', 'direct')
        assert e1 == e2

        e3 = Exchange('foo', 'topic')
        assert e1 != e3

        assert e1.__eq__(True) == NotImplemented

    def test_revive(self):
        exchange = Exchange('foo', 'direct')
        conn = get_conn()
        chan = conn.channel()

        # reviving unbound channel is a noop.
        exchange.revive(chan)
        assert not exchange.is_bound
        assert exchange._channel is None

        bound = exchange.bind(chan)
        assert bound.is_bound
        assert bound.channel is chan

        chan2 = conn.channel()
        bound.revive(chan2)
        assert bound.is_bound
        assert bound._channel is chan2

    def test_assert_is_bound(self):
        exchange = Exchange('foo', 'direct')
        with pytest.raises(NotBoundError):
            exchange.declare()
        conn = get_conn()

        chan = conn.channel()
        exchange.bind(chan).declare()
        assert 'exchange_declare' in chan

    def test_set_transient_delivery_mode(self):
        exc = Exchange('foo', 'direct', delivery_mode='transient')
        assert exc.delivery_mode == Exchange.TRANSIENT_DELIVERY_MODE

    def test_set_passive_mode(self):
        exc = Exchange('foo', 'direct', passive=True)
        assert exc.passive

    def test_set_persistent_delivery_mode(self):
        exc = Exchange('foo', 'direct', delivery_mode='persistent')
        assert exc.delivery_mode == Exchange.PERSISTENT_DELIVERY_MODE

    def test_bind_at_instantiation(self):
        assert Exchange('foo', channel=get_conn().channel()).is_bound

    def test_create_message(self):
        chan = get_conn().channel()
        Exchange('foo', channel=chan).Message({'foo': 'bar'})
        assert 'prepare_message' in chan

    def test_publish(self):
        chan = get_conn().channel()
        Exchange('foo', channel=chan).publish('the quick brown fox')
        assert 'basic_publish' in chan

    def test_delete(self):
        chan = get_conn().channel()
        Exchange('foo', channel=chan).delete()
        assert 'exchange_delete' in chan

    def test__repr__(self):
        b = Exchange('foo', 'topic')
        assert 'foo(topic)' in repr(b)
        assert 'Exchange' in repr(b)

    def test_bind_to(self):
        chan = get_conn().channel()
        foo = Exchange('foo', 'topic')
        bar = Exchange('bar', 'topic')
        foo(chan).bind_to(bar)
        assert 'exchange_bind' in chan

    def test_bind_to_by_name(self):
        chan = get_conn().channel()
        foo = Exchange('foo', 'topic')
        foo(chan).bind_to('bar')
        assert 'exchange_bind' in chan

    def test_unbind_from(self):
        chan = get_conn().channel()
        foo = Exchange('foo', 'topic')
        bar = Exchange('bar', 'topic')
        foo(chan).unbind_from(bar)
        assert 'exchange_unbind' in chan

    def test_unbind_from_by_name(self):
        chan = get_conn().channel()
        foo = Exchange('foo', 'topic')
        foo(chan).unbind_from('bar')
        assert 'exchange_unbind' in chan

    def test_declare__no_declare(self):
        chan = get_conn().channel()
        foo = Exchange('foo', 'topic', no_declare=True)
        foo(chan).declare()
        assert 'exchange_declare' not in chan

    def test_declare__internal_exchange(self):
        chan = get_conn().channel()
        foo = Exchange('amq.rabbitmq.trace', 'topic')
        foo(chan).declare()
        assert 'exchange_declare' not in chan

    def test_declare(self):
        chan = get_conn().channel()
        foo = Exchange('foo', 'topic', no_declare=False)
        foo(chan).declare()
        assert 'exchange_declare' in chan


class test_Queue:

    def setup(self):
        self.exchange = Exchange('foo', 'direct')

    def test_constructor_with_actual_exchange(self):
        exchange = Exchange('exchange_name', 'direct')
        queue = Queue(name='queue_name', exchange=exchange)
        assert queue.exchange == exchange

    def test_constructor_with_string_exchange(self):
        exchange_name = str('exchange_name')
        queue = Queue(name='queue_name', exchange=exchange_name)
        assert queue.exchange == Exchange(exchange_name)

    def test_constructor_with_default_exchange(self):
        queue = Queue(name='queue_name')
        assert queue.exchange == Exchange('')

    def test_hash(self):
        assert hash(Queue('a')) == hash(Queue('a'))
        assert hash(Queue('a')) != hash(Queue('b'))

    def test_repr_with_bindings(self):
        ex = Exchange('foo')
        x = Queue('foo', bindings=[ex.binding('A'), ex.binding('B')])
        assert repr(x)

    def test_anonymous(self):
        chan = Mock()
        x = Queue(bindings=[binding(Exchange('foo'), 'rkey')])
        chan.queue_declare.return_value = 'generated', 0, 0
        xx = x(chan)
        xx.declare()
        assert xx.name == 'generated'

    def test_basic_get__accept_disallowed(self):
        conn = Connection('memory://')
        q = Queue('foo', exchange=self.exchange)
        p = Producer(conn)
        p.publish(
            {'complex': object()},
            declare=[q], exchange=self.exchange, serializer='pickle',
        )

        message = q(conn).get(no_ack=True)
        assert message is not None

        with pytest.raises(q.ContentDisallowed):
            message.decode()

    def test_basic_get__accept_allowed(self):
        conn = Connection('memory://')
        q = Queue('foo', exchange=self.exchange)
        p = Producer(conn)
        p.publish(
            {'complex': object()},
            declare=[q], exchange=self.exchange, serializer='pickle',
        )

        message = q(conn).get(accept=['pickle'], no_ack=True)
        assert message is not None

        payload = message.decode()
        assert payload['complex']

    def test_when_bound_but_no_exchange(self):
        q = Queue('a')
        q.exchange = None
        assert q.when_bound() is None

    def test_declare_but_no_exchange(self):
        q = Queue('a')
        q.queue_declare = Mock()
        q.queue_bind = Mock()
        q.exchange = None

        q.declare()
        q.queue_declare.assert_called_with(
            channel=None, nowait=False, passive=False)

    def test_declare__no_declare(self):
        q = Queue('a', no_declare=True)
        q.queue_declare = Mock()
        q.queue_bind = Mock()
        q.exchange = None

        q.declare()
        q.queue_declare.assert_not_called()
        q.queue_bind.assert_not_called()

    def test_bind_to_when_name(self):
        chan = Mock()
        q = Queue('a')
        q(chan).bind_to('ex')
        chan.queue_bind.assert_called()

    def test_get_when_no_m2p(self):
        chan = Mock()
        q = Queue('a')(chan)
        chan.message_to_python = None
        assert q.get()

    def test_multiple_bindings(self):
        chan = Mock()
        q = Queue('mul', [
            binding(Exchange('mul1'), 'rkey1'),
            binding(Exchange('mul2'), 'rkey2'),
            binding(Exchange('mul3'), 'rkey3'),
        ])
        q(chan).declare()
        assert call(
            nowait=False,
            exchange='mul1',
            auto_delete=False,
            passive=False,
            arguments=None,
            type='direct',
            durable=True,
        ) in chan.exchange_declare.call_args_list

    def test_can_cache_declaration(self):
        assert Queue('a', durable=True).can_cache_declaration
        assert Queue('a', durable=False).can_cache_declaration
        assert not Queue(
            'a', queue_arguments={'x-expires': 100}
        ).can_cache_declaration

    def test_eq(self):
        q1 = Queue('xxx', Exchange('xxx', 'direct'), 'xxx')
        q2 = Queue('xxx', Exchange('xxx', 'direct'), 'xxx')
        assert q1 == q2
        assert q1.__eq__(True) == NotImplemented

        q3 = Queue('yyy', Exchange('xxx', 'direct'), 'xxx')
        assert q1 != q3

    def test_exclusive_implies_auto_delete(self):
        assert Queue('foo', self.exchange, exclusive=True).auto_delete

    def test_binds_at_instantiation(self):
        assert Queue('foo', self.exchange,
                     channel=get_conn().channel()).is_bound

    def test_also_binds_exchange(self):
        chan = get_conn().channel()
        b = Queue('foo', self.exchange)
        assert not b.is_bound
        assert not b.exchange.is_bound
        b = b.bind(chan)
        assert b.is_bound
        assert b.exchange.is_bound
        assert b.channel is b.exchange.channel
        assert b.exchange is not self.exchange

    def test_declare(self):
        chan = get_conn().channel()
        b = Queue('foo', self.exchange, 'foo', channel=chan)
        assert b.is_bound
        b.declare()
        assert 'exchange_declare' in chan
        assert 'queue_declare' in chan
        assert 'queue_bind' in chan

    def test_get(self):
        b = Queue('foo', self.exchange, 'foo', channel=get_conn().channel())
        b.get()
        assert 'basic_get' in b.channel

    def test_purge(self):
        b = Queue('foo', self.exchange, 'foo', channel=get_conn().channel())
        b.purge()
        assert 'queue_purge' in b.channel

    def test_consume(self):
        b = Queue('foo', self.exchange, 'foo', channel=get_conn().channel())
        b.consume('fifafo', None)
        assert 'basic_consume' in b.channel

    def test_cancel(self):
        b = Queue('foo', self.exchange, 'foo', channel=get_conn().channel())
        b.cancel('fifafo')
        assert 'basic_cancel' in b.channel

    def test_delete(self):
        b = Queue('foo', self.exchange, 'foo', channel=get_conn().channel())
        b.delete()
        assert 'queue_delete' in b.channel

    def test_queue_unbind(self):
        b = Queue('foo', self.exchange, 'foo', channel=get_conn().channel())
        b.queue_unbind()
        assert 'queue_unbind' in b.channel

    def test_as_dict(self):
        q = Queue('foo', self.exchange, 'rk')
        d = q.as_dict(recurse=True)
        assert d['exchange']['name'] == self.exchange.name

    def test_queue_dump(self):
        b = binding(self.exchange, 'rk')
        q = Queue('foo', self.exchange, 'rk', bindings=[b])
        d = q.as_dict(recurse=True)
        assert d['bindings'][0]['routing_key'] == 'rk'
        registry.dumps(d)

    def test__repr__(self):
        b = Queue('foo', self.exchange, 'foo')
        assert 'foo' in repr(b)
        assert 'Queue' in repr(b)


class test_MaybeChannelBound:

    def test_repr(self):
        assert repr(MaybeChannelBound())
