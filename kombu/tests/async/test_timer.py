from __future__ import absolute_import, unicode_literals

from datetime import datetime

from kombu.five import bytes_if_py2

from kombu.async.timer import Entry, Timer, to_timestamp

from kombu.tests.case import Case, Mock, mock, patch


class test_to_timestamp(Case):

    def test_timestamp(self):
        self.assertIs(to_timestamp(3.13), 3.13)

    def test_datetime(self):
        self.assertTrue(to_timestamp(datetime.utcnow()))


class test_Entry(Case):

    def test_call(self):
        scratch = [None]

        def timed(x, y, moo='foo'):
            scratch[0] = (x, y, moo)

        tref = Entry(timed, (4, 4), {'moo': 'baz'})
        tref()

        self.assertTupleEqual(scratch[0], (4, 4, 'baz'))

    def test_cancel(self):
        tref = Entry(lambda x: x, (1,), {})
        self.assertFalse(tref.canceled)
        self.assertFalse(tref.cancelled)
        tref.cancel()
        self.assertTrue(tref.canceled)
        self.assertTrue(tref.cancelled)

    def test_repr(self):
        tref = Entry(lambda x: x(1,), {})
        self.assertTrue(repr(tref))

    def test_hash(self):
        self.assertTrue(hash(Entry(lambda: None)))

    def test_ordering(self):
        # we don't care about results, just that it's possible
        Entry(lambda x: 1) < Entry(lambda x: 2)
        Entry(lambda x: 1) > Entry(lambda x: 2)
        Entry(lambda x: 1) >= Entry(lambda x: 2)
        Entry(lambda x: 1) <= Entry(lambda x: 2)

    def test_eq(self):
        x = Entry(lambda x: 1)
        y = Entry(lambda x: 1)
        self.assertEqual(x, x)
        self.assertNotEqual(x, y)


class test_Timer(Case):

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

        self.assertIs(x.schedule, x)

    def test_handle_error(self):
        from datetime import datetime
        scratch = [None]

        def on_error(exc_info):
            scratch[0] = exc_info

        s = Timer(on_error=on_error)

        with patch('kombu.async.timer.to_timestamp') as tot:
            tot.side_effect = OverflowError()
            s.enter_at(Entry(lambda: None, (), {}),
                       eta=datetime.now())
            s.enter_at(Entry(lambda: None, (), {}), eta=None)
            s.on_error = None
            with self.assertRaises(OverflowError):
                s.enter_at(Entry(lambda: None, (), {}),
                           eta=datetime.now())
        exc = scratch[0]
        self.assertIsInstance(exc, OverflowError)

    def test_call_repeatedly(self):
        t = Timer()
        try:
            t.schedule.enter_after = Mock()

            myfun = Mock()
            myfun.__name__ = bytes_if_py2('myfun')
            t.call_repeatedly(0.03, myfun)

            self.assertEqual(t.schedule.enter_after.call_count, 1)
            args1, _ = t.schedule.enter_after.call_args_list[0]
            sec1, tref1, _ = args1
            self.assertEqual(sec1, 0.03)
            tref1()

            self.assertEqual(t.schedule.enter_after.call_count, 2)
            args2, _ = t.schedule.enter_after.call_args_list[1]
            sec2, tref2, _ = args2
            self.assertEqual(sec2, 0.03)
            tref2.canceled = True
            tref2()

            self.assertEqual(t.schedule.enter_after.call_count, 2)
        finally:
            t.stop()

    @patch('kombu.async.timer.logger')
    def test_apply_entry_error_handled(self, logger):
        t = Timer()
        t.schedule.on_error = None

        fun = Mock()
        fun.side_effect = ValueError()

        t.schedule.apply_entry(fun)
        logger.error.assert_called()

    @mock.stdouts
    def test_apply_entry_error_not_handled(self, stdout, stderr):
        t = Timer()
        t.schedule.on_error = Mock()

        fun = Mock()
        fun.side_effect = ValueError()
        t.schedule.apply_entry(fun)
        fun.assert_called_with()
        self.assertFalse(stderr.getvalue())

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
