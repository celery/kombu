import pytest

from case import Mock, patch

from kombu import Connection, Exchange, Queue
from kombu import compat

from t.mocks import Transport, Channel


class test_misc:

    def test_iterconsume(self):

        class MyConnection:
            drained = 0

            def drain_events(self, *args, **kwargs):
                self.drained += 1
                return self.drained

        class Consumer:
            active = False

            def consume(self, *args, **kwargs):
                self.active = True

        conn = MyConnection()
        consumer = Consumer()
        it = compat._iterconsume(conn, consumer)
        assert next(it) == 1
        assert consumer.active

        it2 = compat._iterconsume(conn, consumer, limit=10)
        assert list(it2), [2, 3, 4, 5, 6, 7, 8, 9, 10 == 11]

    def test_Queue_from_dict(self):
        defs = {'binding_key': 'foo.#',
                'exchange': 'fooex',
                'exchange_type': 'topic',
                'durable': True,
                'auto_delete': False}

        q1 = Queue.from_dict('foo', **dict(defs))
        assert q1.name == 'foo'
        assert q1.routing_key == 'foo.#'
        assert q1.exchange.name == 'fooex'
        assert q1.exchange.type == 'topic'
        assert q1.durable
        assert q1.exchange.durable
        assert not q1.auto_delete
        assert not q1.exchange.auto_delete

        q2 = Queue.from_dict('foo', **dict(defs,
                                           exchange_durable=False))
        assert q2.durable
        assert not q2.exchange.durable

        q3 = Queue.from_dict('foo', **dict(defs,
                                           exchange_auto_delete=True))
        assert not q3.auto_delete
        assert q3.exchange.auto_delete

        q4 = Queue.from_dict('foo', **dict(defs,
                                           queue_durable=False))
        assert not q4.durable
        assert q4.exchange.durable

        q5 = Queue.from_dict('foo', **dict(defs,
                                           queue_auto_delete=True))
        assert q5.auto_delete
        assert not q5.exchange.auto_delete

        assert (Queue.from_dict('foo', **dict(defs)) ==
                Queue.from_dict('foo', **dict(defs)))


class test_Publisher:

    def setup(self):
        self.connection = Connection(transport=Transport)

    def test_constructor(self):
        pub = compat.Publisher(self.connection,
                               exchange='test_Publisher_constructor',
                               routing_key='rkey')
        assert isinstance(pub.backend, Channel)
        assert pub.exchange.name == 'test_Publisher_constructor'
        assert pub.exchange.durable
        assert not pub.exchange.auto_delete
        assert pub.exchange.type == 'direct'

        pub2 = compat.Publisher(self.connection,
                                exchange='test_Publisher_constructor2',
                                routing_key='rkey',
                                auto_delete=True,
                                durable=False)
        assert pub2.exchange.auto_delete
        assert not pub2.exchange.durable

        explicit = Exchange('test_Publisher_constructor_explicit',
                            type='topic')
        pub3 = compat.Publisher(self.connection,
                                exchange=explicit)
        assert pub3.exchange == explicit

        compat.Publisher(self.connection,
                         exchange='test_Publisher_constructor3',
                         channel=self.connection.default_channel)

    def test_send(self):
        pub = compat.Publisher(self.connection,
                               exchange='test_Publisher_send',
                               routing_key='rkey')
        pub.send({'foo': 'bar'})
        assert 'basic_publish' in pub.backend
        pub.close()

    def test__enter__exit__(self):
        pub = compat.Publisher(self.connection,
                               exchange='test_Publisher_send',
                               routing_key='rkey')
        x = pub.__enter__()
        assert x is pub
        x.__exit__()
        assert pub._closed


class test_Consumer:

    def setup(self):
        self.connection = Connection(transport=Transport)

    @patch('kombu.compat._iterconsume')
    def test_iterconsume_calls__iterconsume(self, it, n='test_iterconsume'):
        c = compat.Consumer(self.connection, queue=n, exchange=n)
        c.iterconsume(limit=10, no_ack=True)
        it.assert_called_with(c.connection, c, True, 10)

    def test_constructor(self, n='test_Consumer_constructor'):
        c = compat.Consumer(self.connection, queue=n, exchange=n,
                            routing_key='rkey')
        assert isinstance(c.backend, Channel)
        q = c.queues[0]
        assert q.durable
        assert q.exchange.durable
        assert not q.auto_delete
        assert not q.exchange.auto_delete
        assert q.name == n
        assert q.exchange.name == n

        c2 = compat.Consumer(self.connection, queue=n + '2',
                             exchange=n + '2',
                             routing_key='rkey', durable=False,
                             auto_delete=True, exclusive=True)
        q2 = c2.queues[0]
        assert not q2.durable
        assert not q2.exchange.durable
        assert q2.auto_delete
        assert q2.exchange.auto_delete

    def test__enter__exit__(self, n='test__enter__exit__'):
        c = compat.Consumer(self.connection, queue=n, exchange=n,
                            routing_key='rkey')
        x = c.__enter__()
        assert x is c
        x.__exit__()
        assert c._closed

    def test_revive(self, n='test_revive'):
        c = compat.Consumer(self.connection, queue=n, exchange=n)

        with self.connection.channel() as c2:
            c.revive(c2)
            assert c.backend is c2

    def test__iter__(self, n='test__iter__'):
        c = compat.Consumer(self.connection, queue=n, exchange=n)
        c.iterqueue = Mock()

        c.__iter__()
        c.iterqueue.assert_called_with(infinite=True)

    def test_iter(self, n='test_iterqueue'):
        c = compat.Consumer(self.connection, queue=n, exchange=n,
                            routing_key='rkey')
        c.close()

    def test_process_next(self, n='test_process_next'):
        c = compat.Consumer(self.connection, queue=n, exchange=n,
                            routing_key='rkey')
        with pytest.raises(NotImplementedError):
            c.process_next()
        c.close()

    def test_iterconsume(self, n='test_iterconsume'):
        c = compat.Consumer(self.connection, queue=n, exchange=n,
                            routing_key='rkey')
        c.close()

    def test_discard_all(self, n='test_discard_all'):
        c = compat.Consumer(self.connection, queue=n, exchange=n,
                            routing_key='rkey')
        c.discard_all()
        assert 'queue_purge' in c.backend

    def test_fetch(self, n='test_fetch'):
        c = compat.Consumer(self.connection, queue=n, exchange=n,
                            routing_key='rkey')
        assert c.fetch() is None
        assert c.fetch(no_ack=True) is None
        assert 'basic_get' in c.backend

        callback_called = [False]

        def receive(payload, message):
            callback_called[0] = True

        c.backend.to_deliver.append('42')
        payload = c.fetch().payload
        assert payload == '42'
        c.backend.to_deliver.append('46')
        c.register_callback(receive)
        assert c.fetch(enable_callbacks=True).payload == '46'
        assert callback_called[0]

    def test_discard_all_filterfunc_not_supported(self, n='xjf21j21'):
        c = compat.Consumer(self.connection, queue=n, exchange=n,
                            routing_key='rkey')
        with pytest.raises(NotImplementedError):
            c.discard_all(filterfunc=lambda x: x)
        c.close()

    def test_wait(self, n='test_wait'):

        class C(compat.Consumer):

            def iterconsume(self, limit=None):
                yield from range(limit)

        c = C(self.connection,
              queue=n, exchange=n, routing_key='rkey')
        assert c.wait(10) == list(range(10))
        c.close()

    def test_iterqueue(self, n='test_iterqueue'):
        i = [0]

        class C(compat.Consumer):

            def fetch(self, limit=None):
                z = i[0]
                i[0] += 1
                return z

        c = C(self.connection,
              queue=n, exchange=n, routing_key='rkey')
        assert list(c.iterqueue(limit=10)) == list(range(10))
        c.close()


class test_ConsumerSet:

    def setup(self):
        self.connection = Connection(transport=Transport)

    def test_providing_channel(self):
        chan = Mock(name='channel')
        cs = compat.ConsumerSet(self.connection, channel=chan)
        assert cs._provided_channel
        assert cs.backend is chan

        cs.cancel = Mock(name='cancel')
        cs.close()
        chan.close.assert_not_called()

    @patch('kombu.compat._iterconsume')
    def test_iterconsume(self, _iterconsume, n='test_iterconsume'):
        c = compat.Consumer(self.connection, queue=n, exchange=n)
        cs = compat.ConsumerSet(self.connection, consumers=[c])
        cs.iterconsume(limit=10, no_ack=True)
        _iterconsume.assert_called_with(c.connection, cs, True, 10)

    def test_revive(self, n='test_revive'):
        c = compat.Consumer(self.connection, queue=n, exchange=n)
        cs = compat.ConsumerSet(self.connection, consumers=[c])

        with self.connection.channel() as c2:
            cs.revive(c2)
            assert cs.backend is c2

    def test_constructor(self, prefix='0daf8h21'):
        dcon = {'%s.xyx' % prefix: {'exchange': '%s.xyx' % prefix,
                                    'routing_key': 'xyx'},
                '%s.xyz' % prefix: {'exchange': '%s.xyz' % prefix,
                                    'routing_key': 'xyz'}}
        consumers = [compat.Consumer(self.connection, queue=prefix + str(i),
                                     exchange=prefix + str(i))
                     for i in range(3)]
        c = compat.ConsumerSet(self.connection, consumers=consumers)
        c2 = compat.ConsumerSet(self.connection, from_dict=dcon)

        assert len(c.queues) == 3
        assert len(c2.queues) == 2

        c.add_consumer(compat.Consumer(self.connection,
                                       queue=prefix + 'xaxxxa',
                                       exchange=prefix + 'xaxxxa'))
        assert len(c.queues) == 4
        for cq in c.queues:
            assert cq.channel is c.channel

        c2.add_consumer_from_dict(
            '%s.xxx' % prefix,
            exchange='%s.xxx' % prefix,
            routing_key='xxx',
        )
        assert len(c2.queues) == 3
        for c2q in c2.queues:
            assert c2q.channel is c2.channel

        c.discard_all()
        assert c.channel.called.count('queue_purge') == 4
        c.consume()

        c.close()
        c2.close()
        assert 'basic_cancel' in c.channel
        assert 'close' in c.channel
        assert 'close' in c2.channel
