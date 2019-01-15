import pytest

from case import Mock

from kombu import Connection, Producer
from kombu import pools
from kombu.connection import ConnectionPool
from kombu.utils.collections import eqhash


class test_ProducerPool:
    Pool = pools.ProducerPool

    class MyPool(pools.ProducerPool):

        def __init__(self, *args, **kwargs):
            self.instance = Mock()
            pools.ProducerPool.__init__(self, *args, **kwargs)

        def Producer(self, connection):
            return self.instance

    def setup(self):
        self.connections = Mock()
        self.pool = self.Pool(self.connections, limit=10)

    def test_close_resource(self):
        self.pool.close_resource(Mock(name='resource'))

    def test_releases_connection_when_Producer_raises(self):
        self.pool.Producer = Mock()
        self.pool.Producer.side_effect = IOError()
        acq = self.pool._acquire_connection = Mock()
        conn = acq.return_value = Mock()
        with pytest.raises(IOError):
            self.pool.create_producer()
        conn.release.assert_called_with()

    def test_prepare_release_connection_on_error(self):
        pp = Mock()
        p = pp.return_value = Mock()
        p.revive.side_effect = IOError()
        acq = self.pool._acquire_connection = Mock()
        conn = acq.return_value = Mock()
        p._channel = None
        with pytest.raises(IOError):
            self.pool.prepare(pp)
        conn.release.assert_called_with()

    def test_release_releases_connection(self):
        p = Mock()
        p.__connection__ = Mock()
        self.pool.release(p)
        p.__connection__.release.assert_called_with()
        p.__connection__ = None
        self.pool.release(p)

    def test_init(self):
        assert self.pool.connections is self.connections

    def test_Producer(self):
        assert isinstance(self.pool.Producer(Mock()), Producer)

    def test_acquire_connection(self):
        self.pool._acquire_connection()
        self.connections.acquire.assert_called_with(block=True)

    def test_new(self):
        promise = self.pool.new()
        producer = promise()
        assert isinstance(producer, Producer)
        self.connections.acquire.assert_called_with(block=True)

    def test_setup_unlimited(self):
        pool = self.Pool(self.connections, limit=None)
        pool.setup()
        assert not pool._resource.queue

    def test_setup(self):
        assert len(self.pool._resource.queue) == self.pool.limit

        first = self.pool._resource.get_nowait()
        producer = first()
        assert isinstance(producer, Producer)

    def test_prepare(self):
        connection = self.connections.acquire.return_value = Mock()
        pool = self.MyPool(self.connections, limit=10)
        pool.instance._channel = None
        first = pool._resource.get_nowait()
        producer = pool.prepare(first)
        self.connections.acquire.assert_called()
        producer.revive.assert_called_with(connection)

    def test_prepare_channel_already_created(self):
        self.connections.acquire.return_value = Mock()
        pool = self.MyPool(self.connections, limit=10)
        pool.instance._channel = Mock()
        first = pool._resource.get_nowait()
        self.connections.acquire.reset()
        producer = pool.prepare(first)
        producer.revive.assert_not_called()

    def test_prepare_not_callable(self):
        x = Producer(Mock)
        self.pool.prepare(x)

    def test_release(self):
        p = Mock()
        p.channel = Mock()
        p.__connection__ = Mock()
        self.pool.release(p)
        p.__connection__.release.assert_called_with()
        assert p.channel is None


class test_PoolGroup:
    Group = pools.PoolGroup

    class MyGroup(pools.PoolGroup):

        def create(self, resource, limit):
            return resource, limit

    def test_interface_create(self):
        g = self.Group()
        with pytest.raises(NotImplementedError):
            g.create(Mock(), 10)

    def test_getitem_using_global_limit(self):
        g = self.MyGroup(limit=pools.use_global_limit)
        res = g['foo']
        assert res == ('foo', pools.get_limit())

    def test_getitem_using_custom_limit(self):
        g = self.MyGroup(limit=102456)
        res = g['foo']
        assert res == ('foo', 102456)

    def test_delitem(self):
        g = self.MyGroup()
        g['foo']
        del(g['foo'])
        assert 'foo' not in g

    def test_Connections(self):
        conn = Connection('memory://')
        p = pools.connections[conn]
        assert p
        assert isinstance(p, ConnectionPool)
        assert p.connection is conn
        assert p.limit == pools.get_limit()

    def test_Producers(self):
        conn = Connection('memory://')
        p = pools.producers[conn]
        assert p
        assert isinstance(p, pools.ProducerPool)
        assert p.connections is pools.connections[conn]
        assert p.limit == p.connections.limit
        assert p.limit == pools.get_limit()

    def test_all_groups(self):
        conn = Connection('memory://')
        pools.connections[conn]

        assert list(pools._all_pools())

    def test_reset(self):
        pools.reset()

        class MyGroup(dict):
            clear_called = False

            def clear(self):
                self.clear_called = True

        p1 = pools.connections['foo'] = Mock()
        g1 = MyGroup()
        pools._groups.append(g1)

        pools.reset()
        p1.force_close_all.assert_called_with()
        assert g1.clear_called

        p1 = pools.connections['foo'] = Mock()
        p1.force_close_all.side_effect = KeyError()
        pools.reset()

    def test_set_limit(self):
        pools.reset()
        pools.set_limit(34576)
        limit = pools.get_limit()
        assert limit == 34576

        conn = Connection('memory://')
        pool = pools.connections[conn]
        with pool.acquire():
            pools.set_limit(limit + 1)
            assert pools.get_limit() == limit + 1
            limit = pools.get_limit()
            with pytest.raises(RuntimeError):
                pools.set_limit(limit - 1)
            pools.set_limit(limit - 1, force=True)
            assert pools.get_limit() == limit - 1

        pools.set_limit(pools.get_limit())

    def test_remove_limit(self):
        conn = Connection('memory://')
        pool = pools.connections[conn]
        pool.limit = 10
        with pool.acquire():
            pool.limit = 0


class test_fun_PoolGroup:

    def test_connections_behavior(self):
        c1u = 'memory://localhost:123'
        c2u = 'memory://localhost:124'
        c1 = Connection(c1u)
        c2 = Connection(c2u)
        c3 = Connection(c1u)

        assert eqhash(c1) != eqhash(c2)
        assert eqhash(c1) == eqhash(c3)

        c4 = Connection(c1u, transport_options={'confirm_publish': True})
        assert eqhash(c3) != eqhash(c4)

        p1 = pools.connections[c1]
        p2 = pools.connections[c2]
        p3 = pools.connections[c3]

        assert p1 is not p2
        assert p1 is p3

        r1 = p1.acquire()
        assert p1._dirty
        assert p3._dirty
        assert not p2._dirty
        r1.release()
        assert not p1._dirty
        assert not p3._dirty
