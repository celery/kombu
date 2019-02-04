from __future__ import absolute_import, unicode_literals

import pytest

from datetime import datetime

from case import Mock, patch

from kombu.asynchronous.timer import Entry, Timer, to_timestamp
from kombu.five import bytes_if_py2


class test_to_timestamp:

    def test_timestamp(self):
        assert to_timestamp(3.13) == 3.13

    def test_datetime(self):
        assert to_timestamp(datetime.utcnow())


class test_Entry:

    def test_call(self):
        fun = Mock(name='fun')
        tref = Entry(fun, (4, 4), {'moo': 'baz'})
        tref()
        fun.assert_called_with(4, 4, moo='baz')

    def test_cancel(self):
        tref = Entry(lambda x: x, (1,), {})
        assert not tref.canceled
        assert not tref.cancelled
        tref.cancel()
        assert tref.canceled
        assert tref.cancelled

    def test_repr(self):
        tref = Entry(lambda x: x(1,), {})
        assert repr(tref)

    def test_hash(self):
        assert hash(Entry(lambda: None))

    def test_ordering(self):
        # we don't care about results, just that it's possible
        Entry(lambda x: 1) < Entry(lambda x: 2)
        Entry(lambda x: 1) > Entry(lambda x: 2)
        Entry(lambda x: 1) >= Entry(lambda x: 2)
        Entry(lambda x: 1) <= Entry(lambda x: 2)

    def test_eq(self):
        x = Entry(lambda x: 1)
        y = Entry(lambda x: 1)
        assert x == x
        assert x != y


class test_Timer:

    def test_enter_exit(self):
        x = Timer()
        x.stop = Mock(name='timer.stop')
        with x:
            pass
        x.stop.assert_called_with()

    def test_supports_Timer_interface(self):
        x = Timer()
        x.stop()

        tref = Mock()
        x.cancel(tref)
        tref.cancel.assert_called_with()

        assert x.schedule is x

    def test_handle_error(self):
        from datetime import datetime
        on_error = Mock(name='on_error')

        s = Timer(on_error=on_error)

        with patch('kombu.asynchronous.timer.to_timestamp') as tot:
            tot.side_effect = OverflowError()
            s.enter_at(Entry(lambda: None, (), {}),
                       eta=datetime.now())
            s.enter_at(Entry(lambda: None, (), {}), eta=None)
            s.on_error = None
            with pytest.raises(OverflowError):
                s.enter_at(Entry(lambda: None, (), {}),
                           eta=datetime.now())
        on_error.assert_called_once()
        exc = on_error.call_args[0][0]
        assert isinstance(exc, OverflowError)

    def test_call_repeatedly(self):
        t = Timer()
        try:
            t.schedule.enter_after = Mock()

            myfun = Mock()
            myfun.__name__ = bytes_if_py2('myfun')
            t.call_repeatedly(0.03, myfun)

            assert t.schedule.enter_after.call_count == 1
            args1, _ = t.schedule.enter_after.call_args_list[0]
            sec1, tref1, _ = args1
            assert sec1 == 0.03
            tref1()

            assert t.schedule.enter_after.call_count == 2
            args2, _ = t.schedule.enter_after.call_args_list[1]
            sec2, tref2, _ = args2
            assert sec2 == 0.03
            tref2.canceled = True
            tref2()

            assert t.schedule.enter_after.call_count == 2
        finally:
            t.stop()

    @patch('kombu.asynchronous.timer.logger')
    def test_apply_entry_error_handled(self, logger):
        t = Timer()
        t.schedule.on_error = None

        fun = Mock()
        fun.side_effect = ValueError()

        t.schedule.apply_entry(fun)
        logger.error.assert_called()

    def test_apply_entry_error_not_handled(self, stdouts):
        t = Timer()
        t.schedule.on_error = Mock()

        fun = Mock()
        fun.side_effect = ValueError()
        t.schedule.apply_entry(fun)
        fun.assert_called_with()
        assert not stdouts.stderr.getvalue()

    def test_enter_after(self):
        t = Timer()
        t._enter = Mock()
        fun = Mock(name='fun')
        time = Mock(name='time')
        time.return_value = 10
        t.enter_after(10, fun, time=time)
        time.assert_called_with()
        t._enter.assert_called_with(20, 0, fun)

    def test_cancel(self):
        t = Timer()
        tref = Mock()
        t.cancel(tref)
        tref.cancel.assert_called_with()
