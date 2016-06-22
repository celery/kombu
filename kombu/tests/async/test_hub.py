from __future__ import absolute_import, unicode_literals

import errno

from vine import promise

from kombu.async import hub as _hub
from kombu.async import Hub, READ, WRITE, ERR
from kombu.async.debug import callback_for, repr_flag, _rcb
from kombu.async.hub import (
    Stop, get_event_loop, set_event_loop,
    _raise_stop_error, _dummy_context
)
from kombu.async.semaphore import DummyLock, LaxBoundedSemaphore

from kombu.tests.case import Case, Mock, call, patch


class File(object):

    def __init__(self, fd):
        self.fd = fd

    def fileno(self):
        return self.fd

    def __eq__(self, other):
        if isinstance(other, File):
            return self.fd == other.fd
        return NotImplemented

    def __hash__(self):
        return hash(self.fd)


class test_DummyLock(Case):

    def test_context(self):
        mutex = DummyLock()
        with mutex:
            pass


class test_LaxBoundedSemaphore(Case):

    def test_acquire_release(self):
        x = LaxBoundedSemaphore(2)

        c1 = Mock()
        x.acquire(c1, 1)
        self.assertEqual(x.value, 1)
        c1.assert_called_with(1)

        c2 = Mock()
        x.acquire(c2, 2)
        self.assertEqual(x.value, 0)
        c2.assert_called_with(2)

        c3 = Mock()
        x.acquire(c3, 3)
        self.assertEqual(x.value, 0)
        c3.assert_not_called()

        x.release()
        self.assertEqual(x.value, 0)
        x.release()
        self.assertEqual(x.value, 1)
        x.release()
        self.assertEqual(x.value, 2)
        c3.assert_called_with(3)

    def test_repr(self):
        self.assertTrue(repr(LaxBoundedSemaphore(2)))

    def test_bounded(self):
        x = LaxBoundedSemaphore(2)
        for i in range(100):
            x.release()
        self.assertEqual(x.value, 2)

    def test_grow_shrink(self):
        x = LaxBoundedSemaphore(1)
        self.assertEqual(x.initial_value, 1)
        cb1 = Mock()
        x.acquire(cb1, 1)
        cb1.assert_called_with(1)
        self.assertEqual(x.value, 0)

        cb2 = Mock()
        x.acquire(cb2, 2)
        cb2.assert_not_called()
        self.assertEqual(x.value, 0)

        cb3 = Mock()
        x.acquire(cb3, 3)
        cb3.assert_not_called()

        x.grow(2)
        cb2.assert_called_with(2)
        cb3.assert_called_with(3)
        self.assertEqual(x.value, 2)
        self.assertEqual(x.initial_value, 3)

        self.assertFalse(x._waiting)
        x.grow(3)
        for i in range(x.initial_value):
            self.assertTrue(x.acquire(Mock()))
        self.assertFalse(x.acquire(Mock()))
        x.clear()

        x.shrink(3)
        for i in range(x.initial_value):
            self.assertTrue(x.acquire(Mock()))
        self.assertFalse(x.acquire(Mock()))
        self.assertEqual(x.value, 0)

        for i in range(100):
            x.release()
        self.assertEqual(x.value, x.initial_value)

    def test_clear(self):
        x = LaxBoundedSemaphore(10)
        for i in range(11):
            x.acquire(Mock())
        self.assertTrue(x._waiting)
        self.assertEqual(x.value, 0)

        x.clear()
        self.assertFalse(x._waiting)
        self.assertEqual(x.value, x.initial_value)


class test_Utils(Case):

    def setup(self):
        self._prev_loop = get_event_loop()

    def teardown(self):
        set_event_loop(self._prev_loop)

    def test_get_set_event_loop(self):
        set_event_loop(None)
        self.assertIsNone(_hub._current_loop)
        self.assertIsNone(get_event_loop())
        hub = Hub()
        set_event_loop(hub)
        self.assertIs(_hub._current_loop, hub)
        self.assertIs(get_event_loop(), hub)

    def test_dummy_context(self):
        with _dummy_context():
            pass

    def test_raise_stop_error(self):
        with self.assertRaises(Stop):
            _raise_stop_error()


class test_Hub(Case):

    def setup(self):
        self.hub = Hub()

    def teardown(self):
        self.hub.close()

    def test_reset(self):
        self.hub.close = Mock(name='close')
        self.hub._create_poller = Mock(name='_create_poller')
        self.hub.reset()
        self.hub.close.assert_called_with()
        self.hub._create_poller.assert_called_with()

    def test__close_poller__no_poller(self):
        self.hub.poller = None
        self.hub._close_poller()

    def test__close_poller(self):
        poller = self.hub.poller = Mock(name='poller')
        self.hub._close_poller()
        poller.close.assert_called_with()
        self.assertIsNone(self.hub.poller)

    def test_stop(self):
        self.hub.call_soon = Mock(name='call_soon')
        self.hub.stop()
        self.hub.call_soon.assert_called_with(_raise_stop_error)

    @patch('kombu.async.hub.promise')
    def test_call_soon(self, promise):
        callback = Mock(name='callback')
        ret = self.hub.call_soon(callback, 1, 2, 3)
        promise.assert_called_with(callback, (1, 2, 3))
        self.assertIn(promise(), self.hub._ready)
        self.assertIs(ret, promise())

    def test_call_soon__promise_argument(self):
        callback = promise(Mock(name='callback'), (1, 2, 3))
        ret = self.hub.call_soon(callback)
        self.assertIs(ret, callback)
        self.assertIn(ret, self.hub._ready)

    def test_call_later(self):
        callback = Mock(name='callback')
        self.hub.timer = Mock(name='hub.timer')
        self.hub.call_later(10.0, callback, 1, 2)
        self.hub.timer.call_after.assert_called_with(10.0, callback, (1, 2))

    def test_call_at(self):
        callback = Mock(name='callback')
        self.hub.timer = Mock(name='hub.timer')
        self.hub.call_at(21231122, callback, 1, 2)
        self.hub.timer.call_at.assert_called_with(21231122, callback, (1, 2))

    def test_repr(self):
        self.assertTrue(repr(self.hub))

    def test_repr_flag(self):
        self.assertEqual(repr_flag(READ), 'R')
        self.assertEqual(repr_flag(WRITE), 'W')
        self.assertEqual(repr_flag(ERR), '!')
        self.assertEqual(repr_flag(READ | WRITE), 'RW')
        self.assertEqual(repr_flag(READ | ERR), 'R!')
        self.assertEqual(repr_flag(WRITE | ERR), 'W!')
        self.assertEqual(repr_flag(READ | WRITE | ERR), 'RW!')

    def test_repr_callback_rcb(self):

        def f():
            pass

        self.assertEqual(_rcb(f), f.__name__)
        self.assertEqual(_rcb('foo'), 'foo')

    @patch('kombu.async.hub.poll')
    def test_start_stop(self, poll):
        self.hub = Hub()
        poll.assert_called_with()

        poller = self.hub.poller
        self.hub.stop()
        self.hub.close()
        poller.close.assert_called_with()

    def test_fire_timers(self):
        self.hub.timer = Mock()
        self.hub.timer._queue = []
        self.assertEqual(
            self.hub.fire_timers(min_delay=42.324, max_delay=32.321),
            32.321,
        )

        self.hub.timer._queue = [1]
        self.hub.scheduler = iter([(3.743, None)])
        self.assertEqual(self.hub.fire_timers(), 3.743)

        e1, e2, e3 = Mock(), Mock(), Mock()
        entries = [e1, e2, e3]

        def reset():
            return [m.reset() for m in [e1, e2, e3]]

        def se():
            while 1:
                while entries:
                    yield None, entries.pop()
                yield 3.982, None
        self.hub.scheduler = se()

        self.assertEqual(self.hub.fire_timers(max_timers=10), 3.982)
        for E in [e3, e2, e1]:
            E.assert_called_with()
        reset()

        entries[:] = [Mock() for _ in range(11)]
        keep = list(entries)
        self.assertEqual(
            self.hub.fire_timers(max_timers=10, min_delay=1.13),
            1.13,
        )
        for E in reversed(keep[1:]):
            E.assert_called_with()
        reset()
        self.assertEqual(self.hub.fire_timers(max_timers=10), 3.982)
        keep[0].assert_called_with()

    def test_fire_timers_raises(self):
        eback = Mock()
        eback.side_effect = KeyError('foo')
        self.hub.timer = Mock()
        self.hub.scheduler = iter([(0, eback)])
        with self.assertRaises(KeyError):
            self.hub.fire_timers(propagate=(KeyError,))

        eback.side_effect = ValueError('foo')
        self.hub.scheduler = iter([(0, eback)])
        with patch('kombu.async.hub.logger') as logger:
            with self.assertRaises(StopIteration):
                self.hub.fire_timers()
            logger.error.assert_called()

        eback.side_effect = MemoryError('foo')
        self.hub.scheduler = iter([(0, eback)])
        with self.assertRaises(MemoryError):
            self.hub.fire_timers()

        eback.side_effect = OSError()
        eback.side_effect.errno = errno.ENOMEM
        self.hub.scheduler = iter([(0, eback)])
        with self.assertRaises(OSError):
            self.hub.fire_timers()

        eback.side_effect = OSError()
        eback.side_effect.errno = errno.ENOENT
        self.hub.scheduler = iter([(0, eback)])
        with patch('kombu.async.hub.logger') as logger:
            with self.assertRaises(StopIteration):
                self.hub.fire_timers()
            logger.error.assert_called()

    def test_add_raises_ValueError(self):
        self.hub.poller = Mock(name='hub.poller')
        self.hub.poller.register.side_effect = ValueError()
        self.hub._discard = Mock(name='hub.discard')
        with self.assertRaises(ValueError):
            self.hub.add(2, Mock(), READ)
        self.hub._discard.assert_called_with(2)

    def test_remove_reader(self):
        self.hub.poller = Mock(name='hub.poller')
        self.hub.add(2, Mock(), READ)
        self.hub.add(2, Mock(), WRITE)
        self.hub.remove_reader(2)
        self.assertNotIn(2, self.hub.readers)
        self.assertIn(2, self.hub.writers)

    def test_remove_reader__not_writeable(self):
        self.hub.poller = Mock(name='hub.poller')
        self.hub.add(2, Mock(), READ)
        self.hub.remove_reader(2)
        self.assertNotIn(2, self.hub.readers)

    def test_remove_writer(self):
        self.hub.poller = Mock(name='hub.poller')
        self.hub.add(2, Mock(), READ)
        self.hub.add(2, Mock(), WRITE)
        self.hub.remove_writer(2)
        self.assertIn(2, self.hub.readers)
        self.assertNotIn(2, self.hub.writers)

    def test_remove_writer__not_readable(self):
        self.hub.poller = Mock(name='hub.poller')
        self.hub.add(2, Mock(), WRITE)
        self.hub.remove_writer(2)
        self.assertNotIn(2, self.hub.writers)

    def test_add__consolidate(self):
        self.hub.poller = Mock(name='hub.poller')
        self.hub.add(2, Mock(), WRITE, consolidate=True)
        self.assertIn(2, self.hub.consolidate)
        self.assertIsNone(self.hub.writers[2])

    @patch('kombu.async.hub.logger')
    def test_on_callback_error(self, logger):
        self.hub.on_callback_error(Mock(name='callback'), KeyError())
        logger.error.assert_called()

    def test_loop_property(self):
        self.hub._loop = None
        self.hub.create_loop = Mock(name='hub.create_loop')
        self.assertIs(self.hub.loop, self.hub.create_loop())
        self.assertIs(self.hub._loop, self.hub.create_loop())

    def test_run_forever(self):
        self.hub.run_once = Mock(name='hub.run_once')
        self.hub.run_once.side_effect = Stop()
        self.hub.run_forever()

    def test_run_once(self):
        self.hub._loop = iter([1])
        self.hub.run_once()
        self.hub.run_once()
        self.assertIsNone(self.hub._loop)

    def test_repr_active(self):
        self.hub.readers = {1: Mock(), 2: Mock()}
        self.hub.writers = {3: Mock(), 4: Mock()}
        for value in list(
                self.hub.readers.values()) + list(self.hub.writers.values()):
            value.__name__ = 'mock'
        self.assertTrue(self.hub.repr_active())

    def test_repr_events(self):
        self.hub.readers = {6: Mock(), 7: Mock(), 8: Mock()}
        self.hub.writers = {9: Mock()}
        for value in list(
                self.hub.readers.values()) + list(self.hub.writers.values()):
            value.__name__ = 'mock'
        self.assertTrue(self.hub.repr_events([
            (6, READ),
            (7, ERR),
            (8, READ | ERR),
            (9, WRITE),
            (10, 13213),
        ]))

    def test_callback_for(self):
        reader, writer = Mock(), Mock()
        self.hub.readers = {6: reader}
        self.hub.writers = {7: writer}

        self.assertEqual(callback_for(self.hub, 6, READ), reader)
        self.assertEqual(callback_for(self.hub, 7, WRITE), writer)
        with self.assertRaises(KeyError):
            callback_for(self.hub, 6, WRITE)
        self.assertEqual(callback_for(self.hub, 6, WRITE, 'foo'), 'foo')

    def test_add_remove_readers(self):
        P = self.hub.poller = Mock()

        read_A = Mock()
        read_B = Mock()
        self.hub.add_reader(10, read_A, 10)
        self.hub.add_reader(File(11), read_B, 11)

        P.register.assert_has_calls([
            call(10, self.hub.READ | self.hub.ERR),
            call(11, self.hub.READ | self.hub.ERR),
        ], any_order=True)

        self.assertEqual(self.hub.readers[10], (read_A, (10,)))
        self.assertEqual(self.hub.readers[11], (read_B, (11,)))

        self.hub.remove(10)
        self.assertNotIn(10, self.hub.readers)
        self.hub.remove(File(11))
        self.assertNotIn(11, self.hub.readers)
        P.unregister.assert_has_calls([
            call(10), call(11),
        ])

    def test_can_remove_unknown_fds(self):
        self.hub.poller = Mock()
        self.hub.remove(30)
        self.hub.remove(File(301))

    def test_remove__unregister_raises(self):
        self.hub.poller = Mock()
        self.hub.poller.unregister.side_effect = OSError()

        self.hub.remove(313)

    def test_add_writers(self):
        P = self.hub.poller = Mock()

        write_A = Mock()
        write_B = Mock()
        self.hub.add_writer(20, write_A)
        self.hub.add_writer(File(21), write_B)

        P.register.assert_has_calls([
            call(20, self.hub.WRITE),
            call(21, self.hub.WRITE),
        ], any_order=True)

        self.assertEqual(self.hub.writers[20], (write_A, ()))
        self.assertEqual(self.hub.writers[21], (write_B, ()))

        self.hub.remove(20)
        self.assertNotIn(20, self.hub.writers)
        self.hub.remove(File(21))
        self.assertNotIn(21, self.hub.writers)
        P.unregister.assert_has_calls([
            call(20), call(21),
        ])

    def test_enter__exit(self):
        P = self.hub.poller = Mock()
        on_close = Mock()
        self.hub.on_close.add(on_close)

        try:
            read_A = Mock()
            read_B = Mock()
            self.hub.add_reader(10, read_A)
            self.hub.add_reader(File(11), read_B)
            write_A = Mock()
            write_B = Mock()
            self.hub.add_writer(20, write_A)
            self.hub.add_writer(File(21), write_B)
            self.assertTrue(self.hub.readers)
            self.assertTrue(self.hub.writers)
        finally:
            assert self.hub.poller
            self.hub.close()
        self.assertFalse(self.hub.readers)
        self.assertFalse(self.hub.writers)

        P.unregister.assert_has_calls([
            call(10), call(11), call(20), call(21),
        ], any_order=True)

        on_close.assert_called_with(self.hub)

    def test_scheduler_property(self):
        hub = Hub(timer=[1, 2, 3])
        self.assertEqual(list(hub.scheduler), [1, 2, 3])

    def test_loop__tick_callbacks(self):
        self.hub._ready = Mock(name='_ready')
        self.hub._ready.pop.side_effect = RuntimeError()
        ticks = [Mock(name='cb1'), Mock(name='cb2')]
        self.hub.on_tick = list(ticks)

        with self.assertRaises(RuntimeError):
            next(self.hub.loop)

        ticks[0].assert_called_once_with()
        ticks[1].assert_called_once_with()

    def test_loop__todo(self):
        self.hub.fire_timers = Mock(name='fire_timers')
        self.hub.fire_timers.side_effect = RuntimeError()
        self.hub.timer = Mock(name='timer')

        callbacks = [Mock(name='cb1'), Mock(name='cb2')]
        for cb in callbacks:
            self.hub.call_soon(cb)
        self.hub._ready.add(None)

        with self.assertRaises(RuntimeError):
            next(self.hub.loop)

        callbacks[0].assert_called_once_with()
        callbacks[1].assert_called_once_with()
