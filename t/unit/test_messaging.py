from __future__ import absolute_import, unicode_literals

import pickle
import pytest
import sys

from collections import defaultdict

from case import Mock, patch

from kombu import Connection, Consumer, Producer, Exchange, Queue
from kombu.exceptions import MessageStateError
from kombu.utils import json
from kombu.utils.functional import ChannelPromise

from t.mocks import Transport


class test_Producer:

    def setup(self):
        self.exchange = Exchange('foo', 'direct')
        self.connection = Connection(transport=Transport)
        self.connection.connect()
        assert self.connection.connection.connected
        assert not self.exchange.is_bound

    def test_repr(self):
        p = Producer(self.connection)
        assert repr(p)

    def test_pickle(self):
        chan = Mock()
        producer = Producer(chan, serializer='pickle')
        p2 = pickle.loads(pickle.dumps(producer))
        assert p2.serializer == producer.serializer

    def test_no_channel(self):
        p = Producer(None)
        assert not p._channel

    @patch('kombu.messaging.maybe_declare')
    def test_maybe_declare(self, maybe_declare):
        p = self.connection.Producer()
        q = Queue('foo')
        p.maybe_declare(q)
        maybe_declare.assert_called_with(q, p.channel, False)

    @patch('kombu.common.maybe_declare')
    def test_maybe_declare_when_entity_false(self, maybe_declare):
        p = self.connection.Producer()
        p.maybe_declare(None)
        maybe_declare.assert_not_called()

    def test_auto_declare(self):
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, auto_declare=True)
        # creates Exchange clone at bind
        assert p.exchange is not self.exchange
        assert p.exchange.is_bound
        # auto_declare declares exchange'
        assert 'exchange_declare' in channel

    def test_manual_declare(self):
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, auto_declare=False)
        assert p.exchange.is_bound
        # auto_declare=False does not declare exchange
        assert 'exchange_declare' not in channel
        # p.declare() declares exchange')
        p.declare()
        assert 'exchange_declare' in channel

    def test_prepare(self):
        message = {'the quick brown fox': 'jumps over the lazy dog'}
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer='json')
        m, ctype, cencoding = p._prepare(message, headers={})
        assert json.loads(m) == message
        assert ctype == 'application/json'
        assert cencoding == 'utf-8'

    def test_prepare_compression(self):
        message = {'the quick brown fox': 'jumps over the lazy dog'}
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer='json')
        headers = {}
        m, ctype, cencoding = p._prepare(message, compression='zlib',
                                         headers=headers)
        assert ctype == 'application/json'
        assert cencoding == 'utf-8'
        assert headers['compression'] == 'application/x-gzip'
        import zlib
        assert json.loads(zlib.decompress(m).decode('utf-8')) == message

    def test_prepare_custom_content_type(self):
        message = 'the quick brown fox'.encode('utf-8')
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer='json')
        m, ctype, cencoding = p._prepare(message, content_type='custom')
        assert m == message
        assert ctype == 'custom'
        assert cencoding == 'binary'
        m, ctype, cencoding = p._prepare(message, content_type='custom',
                                         content_encoding='alien')
        assert m == message
        assert ctype == 'custom'
        assert cencoding == 'alien'

    def test_prepare_is_already_unicode(self):
        message = 'the quick brown fox'
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer='json')
        m, ctype, cencoding = p._prepare(message, content_type='text/plain')
        assert m == message.encode('utf-8')
        assert ctype == 'text/plain'
        assert cencoding == 'utf-8'
        m, ctype, cencoding = p._prepare(message, content_type='text/plain',
                                         content_encoding='utf-8')
        assert m == message.encode('utf-8')
        assert ctype == 'text/plain'
        assert cencoding == 'utf-8'

    def test_publish_with_Exchange_instance(self):
        p = self.connection.Producer()
        p.channel = Mock()
        p.publish('hello', exchange=Exchange('foo'), delivery_mode='transient')
        assert p._channel.basic_publish.call_args[1]['exchange'] == 'foo'

    def test_publish_with_expiration(self):
        p = self.connection.Producer()
        p.channel = Mock()
        p.publish('hello', exchange=Exchange('foo'), expiration=10)
        properties = p._channel.prepare_message.call_args[0][5]
        assert properties['expiration'] == '10000'

    def test_publish_with_reply_to(self):
        p = self.connection.Producer()
        p.channel = Mock()
        p.publish('hello', exchange=Exchange('foo'), reply_to=Queue('foo'))
        properties = p._channel.prepare_message.call_args[0][5]
        assert properties['reply_to'] == 'foo'

    def test_set_on_return(self):
        chan = Mock()
        chan.events = defaultdict(Mock)
        p = Producer(ChannelPromise(lambda: chan), on_return='on_return')
        p.channel
        chan.events['basic_return'].add.assert_called_with('on_return')

    def test_publish_retry_calls_ensure(self):
        p = Producer(Mock())
        p._connection = Mock()
        ensure = p.connection.ensure = Mock()
        p.publish('foo', exchange='foo', retry=True)
        ensure.assert_called()

    def test_publish_retry_with_declare(self):
        p = self.connection.Producer()
        p.maybe_declare = Mock()
        p.connection.ensure = Mock()
        ex = Exchange('foo')
        p._publish('hello', 0, '', '', {}, {}, 'rk', 0, 0, ex, declare=[ex])
        p.maybe_declare.assert_called_with(ex)

    def test_revive_when_channel_is_connection(self):
        p = self.connection.Producer()
        p.exchange = Mock()
        new_conn = Connection('memory://')
        defchan = new_conn.default_channel
        p.revive(new_conn)

        assert p.channel is defchan
        p.exchange.revive.assert_called_with(defchan)

    def test_enter_exit(self):
        p = self.connection.Producer()
        p.release = Mock()

        assert p.__enter__() is p
        p.__exit__()
        p.release.assert_called_with()

    def test_connection_property_handles_AttributeError(self):
        p = self.connection.Producer()
        p.channel = object()
        p.__connection__ = None
        assert p.connection is None

    def test_publish(self):
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer='json')
        message = {'the quick brown fox': 'jumps over the lazy dog'}
        ret = p.publish(message, routing_key='process')
        assert 'prepare_message' in channel
        assert 'basic_publish' in channel

        m, exc, rkey = ret
        assert json.loads(m['body']) == message
        assert m['content_type'] == 'application/json'
        assert m['content_encoding'] == 'utf-8'
        assert m['priority'] == 0
        assert m['properties']['delivery_mode'] == 2
        assert exc == p.exchange.name
        assert rkey == 'process'

    def test_no_exchange(self):
        chan = self.connection.channel()
        p = Producer(chan)
        assert not p.exchange.name

    def test_revive(self):
        chan = self.connection.channel()
        p = Producer(chan)
        chan2 = self.connection.channel()
        p.revive(chan2)
        assert p.channel is chan2
        assert p.exchange.channel is chan2

    def test_on_return(self):
        chan = self.connection.channel()

        def on_return(exception, exchange, routing_key, message):
            pass

        p = Producer(chan, on_return=on_return)
        assert on_return in chan.events['basic_return']
        assert p.on_return


class test_Consumer:

    def setup(self):
        self.connection = Connection(transport=Transport)
        self.connection.connect()
        assert self.connection.connection.connected
        self.exchange = Exchange('foo', 'direct')

    def test_accept(self):
        a = Consumer(self.connection)
        assert a.accept is None
        b = Consumer(self.connection, accept=['json', 'pickle'])
        assert b.accept == {
            'application/json', 'application/x-python-serialize',
        }
        c = Consumer(self.connection, accept=b.accept)
        assert b.accept == c.accept

    def test_enter_exit_cancel_raises(self):
        c = Consumer(self.connection)
        c.cancel = Mock(name='Consumer.cancel')
        c.cancel.side_effect = KeyError('foo')
        with c:
            pass
        c.cancel.assert_called_with()

    def test_receive_callback_accept(self):
        message = Mock(name='Message')
        message.errors = []
        callback = Mock(name='on_message')
        c = Consumer(self.connection, accept=['json'], on_message=callback)
        c.on_decode_error = None
        c.channel = Mock(name='channel')
        c.channel.message_to_python = None

        c._receive_callback(message)
        callback.assert_called_with(message)
        assert message.accept == c.accept

    def test_accept__content_disallowed(self):
        conn = Connection('memory://')
        q = Queue('foo', exchange=self.exchange)
        p = conn.Producer()
        p.publish(
            {'complex': object()},
            declare=[q], exchange=self.exchange, serializer='pickle',
        )

        callback = Mock(name='callback')
        with conn.Consumer(queues=[q], callbacks=[callback]) as consumer:
            with pytest.raises(consumer.ContentDisallowed):
                conn.drain_events(timeout=1)
        callback.assert_not_called()

    def test_accept__content_allowed(self):
        conn = Connection('memory://')
        q = Queue('foo', exchange=self.exchange)
        p = conn.Producer()
        p.publish(
            {'complex': object()},
            declare=[q], exchange=self.exchange, serializer='pickle',
        )

        callback = Mock(name='callback')
        with conn.Consumer(queues=[q], accept=['pickle'],
                           callbacks=[callback]):
            conn.drain_events(timeout=1)
        callback.assert_called()
        body, message = callback.call_args[0]
        assert body['complex']

    def test_set_no_channel(self):
        c = Consumer(None)
        assert c.channel is None
        c.revive(Mock())
        assert c.channel

    def test_set_no_ack(self):
        channel = self.connection.channel()
        queue = Queue('qname', self.exchange, 'rkey')
        consumer = Consumer(channel, queue, auto_declare=True, no_ack=True)
        assert consumer.no_ack

    def test_add_queue_when_auto_declare(self):
        consumer = self.connection.Consumer(auto_declare=True)
        q = Mock()
        q.return_value = q
        consumer.add_queue(q)
        assert q in consumer.queues
        q.declare.assert_called_with()

    def test_add_queue_when_not_auto_declare(self):
        consumer = self.connection.Consumer(auto_declare=False)
        q = Mock()
        q.return_value = q
        consumer.add_queue(q)
        assert q in consumer.queues
        assert not q.declare.call_count

    def test_consume_without_queues_returns(self):
        consumer = self.connection.Consumer()
        consumer.queues[:] = []
        assert consumer.consume() is None

    def test_consuming_from(self):
        consumer = self.connection.Consumer()
        consumer.queues[:] = [Queue('a'), Queue('b'), Queue('d')]
        consumer._active_tags = {'a': 1, 'b': 2}

        assert not consumer.consuming_from(Queue('c'))
        assert not consumer.consuming_from('c')
        assert not consumer.consuming_from(Queue('d'))
        assert not consumer.consuming_from('d')
        assert consumer.consuming_from(Queue('a'))
        assert consumer.consuming_from(Queue('b'))
        assert consumer.consuming_from('b')

    def test_receive_callback_without_m2p(self):
        channel = self.connection.channel()
        c = channel.Consumer()
        m2p = getattr(channel, 'message_to_python')
        channel.message_to_python = None
        try:
            message = Mock()
            message.errors = []
            message.decode.return_value = 'Hello'
            recv = c.receive = Mock()
            c._receive_callback(message)
            recv.assert_called_with('Hello', message)
        finally:
            channel.message_to_python = m2p

    def test_receive_callback__message_errors(self):
        channel = self.connection.channel()
        channel.message_to_python = None
        c = channel.Consumer()
        message = Mock()
        try:
            raise KeyError('foo')
        except KeyError:
            message.errors = [sys.exc_info()]
        message._reraise_error.side_effect = KeyError()
        with pytest.raises(KeyError):
            c._receive_callback(message)

    def test_set_callbacks(self):
        channel = self.connection.channel()
        queue = Queue('qname', self.exchange, 'rkey')
        callbacks = [lambda x, y: x,
                     lambda x, y: x]
        consumer = Consumer(channel, queue, auto_declare=True,
                            callbacks=callbacks)
        assert consumer.callbacks == callbacks

    def test_auto_declare(self):
        channel = self.connection.channel()
        queue = Queue('qname', self.exchange, 'rkey')
        consumer = Consumer(channel, queue, auto_declare=True)
        consumer.consume()
        consumer.consume()  # twice is a noop
        assert consumer.queues[0] is not queue
        assert consumer.queues[0].is_bound
        assert consumer.queues[0].exchange.is_bound
        assert consumer.queues[0].exchange is not self.exchange

        for meth in ('exchange_declare',
                     'queue_declare',
                     'queue_bind',
                     'basic_consume'):
            assert meth in channel
        assert channel.called.count('basic_consume') == 1
        assert consumer._active_tags

        consumer.cancel_by_queue(queue.name)
        consumer.cancel_by_queue(queue.name)
        assert not consumer._active_tags

    def test_consumer_tag_prefix(self):
        channel = self.connection.channel()
        queue = Queue('qname', self.exchange, 'rkey')
        consumer = Consumer(channel, queue, tag_prefix='consumer_')
        consumer.consume()

        assert consumer._active_tags[queue.name].startswith('consumer_')

    def test_manual_declare(self):
        channel = self.connection.channel()
        queue = Queue('qname', self.exchange, 'rkey')
        consumer = Consumer(channel, queue, auto_declare=False)
        assert consumer.queues[0] is not queue
        assert consumer.queues[0].is_bound
        assert consumer.queues[0].exchange.is_bound
        assert consumer.queues[0].exchange is not self.exchange

        for meth in ('exchange_declare',
                     'queue_declare',
                     'basic_consume'):
            assert meth not in channel

        consumer.declare()
        for meth in ('exchange_declare',
                     'queue_declare',
                     'queue_bind'):
            assert meth in channel
        assert 'basic_consume' not in channel

        consumer.consume()
        assert 'basic_consume' in channel

    def test_consume__cancel(self):
        channel = self.connection.channel()
        queue = Queue('qname', self.exchange, 'rkey')
        consumer = Consumer(channel, queue, auto_declare=True)
        consumer.consume()
        consumer.cancel()
        assert 'basic_cancel' in channel
        assert not consumer._active_tags

    def test___enter____exit__(self):
        channel = self.connection.channel()
        queue = Queue('qname', self.exchange, 'rkey')
        consumer = Consumer(channel, queue, auto_declare=True)
        context = consumer.__enter__()
        assert context is consumer
        assert consumer._active_tags
        res = consumer.__exit__(None, None, None)
        assert not res
        assert 'basic_cancel' in channel
        assert not consumer._active_tags

    def test_flow(self):
        channel = self.connection.channel()
        queue = Queue('qname', self.exchange, 'rkey')
        consumer = Consumer(channel, queue, auto_declare=True)
        consumer.flow(False)
        assert 'flow' in channel

    def test_qos(self):
        channel = self.connection.channel()
        queue = Queue('qname', self.exchange, 'rkey')
        consumer = Consumer(channel, queue, auto_declare=True)
        consumer.qos(30, 10, False)
        assert 'basic_qos' in channel

    def test_purge(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        b2 = Queue('qname2', self.exchange, 'rkey')
        b3 = Queue('qname3', self.exchange, 'rkey')
        b4 = Queue('qname4', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1, b2, b3, b4], auto_declare=True)
        consumer.purge()
        assert channel.called.count('queue_purge') == 4

    def test_multiple_queues(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        b2 = Queue('qname2', self.exchange, 'rkey')
        b3 = Queue('qname3', self.exchange, 'rkey')
        b4 = Queue('qname4', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1, b2, b3, b4])
        consumer.consume()
        assert channel.called.count('exchange_declare') == 4
        assert channel.called.count('queue_declare') == 4
        assert channel.called.count('queue_bind') == 4
        assert channel.called.count('basic_consume') == 4
        assert len(consumer._active_tags) == 4
        consumer.cancel()
        assert channel.called.count('basic_cancel') == 4
        assert not len(consumer._active_tags)

    def test_receive_callback(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])
        received = []

        def callback(message_data, message):
            received.append(message_data)
            message.ack()
            message.payload     # trigger cache

        consumer.register_callback(callback)
        consumer._receive_callback({'foo': 'bar'})

        assert 'basic_ack' in channel
        assert 'message_to_python' in channel
        assert received[0] == {'foo': 'bar'}

    def test_basic_ack_twice(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.ack()
            message.ack()

        consumer.register_callback(callback)
        with pytest.raises(MessageStateError):
            consumer._receive_callback({'foo': 'bar'})

    def test_basic_reject(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.reject()

        consumer.register_callback(callback)
        consumer._receive_callback({'foo': 'bar'})
        assert 'basic_reject' in channel

    def test_basic_reject_twice(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.reject()
            message.reject()

        consumer.register_callback(callback)
        with pytest.raises(MessageStateError):
            consumer._receive_callback({'foo': 'bar'})
        assert 'basic_reject' in channel

    def test_basic_reject__requeue(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.requeue()

        consumer.register_callback(callback)
        consumer._receive_callback({'foo': 'bar'})
        assert 'basic_reject:requeue' in channel

    def test_basic_reject__requeue_twice(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.requeue()
            message.requeue()

        consumer.register_callback(callback)
        with pytest.raises(MessageStateError):
            consumer._receive_callback({'foo': 'bar'})
        assert 'basic_reject:requeue' in channel

    def test_receive_without_callbacks_raises(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])
        with pytest.raises(NotImplementedError):
            consumer.receive(1, 2)

    def test_decode_error(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])
        consumer.channel.throw_decode_error = True

        with pytest.raises(ValueError):
            consumer._receive_callback({'foo': 'bar'})

    def test_on_decode_error_callback(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        thrown = []

        def on_decode_error(msg, exc):
            thrown.append((msg.body, exc))

        consumer = Consumer(channel, [b1], on_decode_error=on_decode_error)
        consumer.channel.throw_decode_error = True
        consumer._receive_callback({'foo': 'bar'})

        assert thrown
        m, exc = thrown[0]
        assert json.loads(m) == {'foo': 'bar'}
        assert isinstance(exc, ValueError)

    def test_recover(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])
        consumer.recover()
        assert 'basic_recover' in channel

    def test_revive(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        consumer = Consumer(channel, [b1])
        channel2 = self.connection.channel()
        consumer.revive(channel2)
        assert consumer.channel is channel2
        assert consumer.queues[0].channel is channel2
        assert consumer.queues[0].exchange.channel is channel2

    def test_revive__with_prefetch_count(self):
        channel = Mock(name='channel')
        b1 = Queue('qname1', self.exchange, 'rkey')
        Consumer(channel, [b1], prefetch_count=14)
        channel.basic_qos.assert_called_with(0, 14, False)

    def test__repr__(self):
        channel = self.connection.channel()
        b1 = Queue('qname1', self.exchange, 'rkey')
        assert repr(Consumer(channel, [b1]))

    def test_connection_property_handles_AttributeError(self):
        p = self.connection.Consumer()
        p.channel = object()
        assert p.connection is None
