from __future__ import absolute_import, unicode_literals

import pickle

from itertools import count

from kombu.five import items
from kombu.utils import functional as utils
from kombu.utils.functional import (
    ChannelPromise, LRUCache, fxrange, fxrangemax, memoize, lazy,
    maybe_evaluate, maybe_list, reprcall, reprkwargs, retry_over_time,
)

from kombu.tests.case import Case, Mock, mock, skip


class test_ChannelPromise(Case):

    def test_repr(self):
        obj = Mock(name='cb')
        self.assertIn(
            'promise',
            repr(ChannelPromise(obj)),
        )
        obj.assert_not_called()


class test_shufflecycle(Case):

    def test_shuffles(self):
        prev_repeat, utils.repeat = utils.repeat, Mock()
        try:
            utils.repeat.return_value = list(range(10))
            values = {'A', 'B', 'C'}
            cycle = utils.shufflecycle(values)
            seen = set()
            for i in range(10):
                next(cycle)
            utils.repeat.assert_called_with(None)
            self.assertTrue(seen.issubset(values))
            with self.assertRaises(StopIteration):
                next(cycle)
                next(cycle)
        finally:
            utils.repeat = prev_repeat


def double(x):
    return x * 2


class test_LRUCache(Case):

    def test_expires(self):
        limit = 100
        x = LRUCache(limit=limit)
        slots = list(range(limit * 2))
        for i in slots:
            x[i] = i
        self.assertListEqual(list(x.keys()), list(slots[limit:]))
        self.assertTrue(x.items())
        self.assertTrue(x.values())

    def test_is_pickleable(self):
        x = LRUCache(limit=10)
        x.update(luke=1, leia=2)
        y = pickle.loads(pickle.dumps(x))
        self.assertEqual(y.limit, y.limit)
        self.assertEqual(y, x)

    def test_update_expires(self):
        limit = 100
        x = LRUCache(limit=limit)
        slots = list(range(limit * 2))
        for i in slots:
            x.update({i: i})

        self.assertListEqual(list(x.keys()), list(slots[limit:]))

    def test_least_recently_used(self):
        x = LRUCache(3)

        x[1], x[2], x[3] = 1, 2, 3
        self.assertEqual(list(x.keys()), [1, 2, 3])

        x[4], x[5] = 4, 5
        self.assertEqual(list(x.keys()), [3, 4, 5])

        # access 3, which makes it the last used key.
        x[3]
        x[6] = 6
        self.assertEqual(list(x.keys()), [5, 3, 6])

        x[7] = 7
        self.assertEqual(list(x.keys()), [3, 6, 7])

    def test_update_larger_than_cache_size(self):
        x = LRUCache(2)
        x.update({x: x for x in range(100)})
        self.assertEqual(list(x.keys()), [98, 99])

    def test_items(self):
        c = LRUCache()
        c.update(a=1, b=2, c=3)
        self.assertTrue(list(items(c)))

    def test_incr(self):
        c = LRUCache()
        c.update(a='1')
        c.incr('a')
        self.assertEqual(c['a'], '2')


class test_memoize(Case):

    def test_memoize(self):
        counter = count(1)

        @memoize(maxsize=2)
        def x(i):
            return next(counter)

        self.assertEqual(x(1), 1)
        self.assertEqual(x(1), 1)
        self.assertEqual(x(2), 2)
        self.assertEqual(x(3), 3)
        self.assertEqual(x(1), 4)
        x.clear()
        self.assertEqual(x(3), 5)


class test_lazy(Case):

    def test__str__(self):
        self.assertEqual(
            str(lazy(lambda: 'the quick brown fox')),
            'the quick brown fox',
        )

    def test__repr__(self):
        self.assertEqual(
            repr(lazy(lambda: 'fi fa fo')).strip('u'),
            "'fi fa fo'",
        )

    @skip.if_python3()
    def test__cmp__(self):
        self.assertEqual(lazy(lambda: 10).__cmp__(lazy(lambda: 20)), -1)
        self.assertEqual(lazy(lambda: 10).__cmp__(5), 1)

    def test_evaluate(self):
        self.assertEqual(lazy(lambda: 2 + 2)(), 4)
        self.assertEqual(lazy(lambda x: x * 4, 2), 8)
        self.assertEqual(lazy(lambda x: x * 8, 2)(), 16)

    def test_cmp(self):
        self.assertEqual(lazy(lambda: 10), lazy(lambda: 10))
        self.assertNotEqual(lazy(lambda: 10), lazy(lambda: 20))

    def test__reduce__(self):
        x = lazy(double, 4)
        y = pickle.loads(pickle.dumps(x))
        self.assertEqual(x(), y())

    def test__deepcopy__(self):
        from copy import deepcopy
        x = lazy(double, 4)
        y = deepcopy(x)
        self.assertEqual(x._fun, y._fun)
        self.assertEqual(x._args, y._args)
        self.assertEqual(x(), y())


class test_maybe_evaluate(Case):

    def test_evaluates(self):
        self.assertEqual(maybe_evaluate(lazy(lambda: 10)), 10)
        self.assertEqual(maybe_evaluate(20), 20)


class test_retry_over_time(Case):

    def setup(self):
        self.index = 0

    class Predicate(Exception):
        pass

    def myfun(self):
        if self.index < 9:
            raise self.Predicate()
        return 42

    def errback(self, exc, intervals, retries):
        interval = next(intervals)
        sleepvals = (None, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 16.0)
        self.index += 1
        self.assertEqual(interval, sleepvals[self.index])
        return interval

    @mock.sleepdeprived(module=utils)
    def test_simple(self):
        prev_count, utils.count = utils.count, Mock()
        try:
            utils.count.return_value = list(range(1))
            x = retry_over_time(self.myfun, self.Predicate,
                                errback=None, interval_max=14)
            self.assertIsNone(x)
            utils.count.return_value = list(range(10))
            cb = Mock()
            x = retry_over_time(self.myfun, self.Predicate,
                                errback=self.errback, callback=cb,
                                interval_max=14)
            self.assertEqual(x, 42)
            self.assertEqual(self.index, 9)
            cb.assert_called_with()
        finally:
            utils.count = prev_count

    @mock.sleepdeprived(module=utils)
    def test_retry_once(self):
        with self.assertRaises(self.Predicate):
            retry_over_time(
                self.myfun, self.Predicate,
                max_retries=1, errback=self.errback, interval_max=14,
            )
        self.assertEqual(self.index, 1)
        # no errback
        with self.assertRaises(self.Predicate):
            retry_over_time(
                self.myfun, self.Predicate,
                max_retries=1, errback=None, interval_max=14,
            )

    @mock.sleepdeprived(module=utils)
    def test_retry_always(self):
        Predicate = self.Predicate

        class Fun(object):

            def __init__(self):
                self.calls = 0

            def __call__(self, *args, **kwargs):
                try:
                    if self.calls >= 10:
                        return 42
                    raise Predicate()
                finally:
                    self.calls += 1
        fun = Fun()

        self.assertEqual(
            retry_over_time(
                fun, self.Predicate,
                max_retries=0, errback=None, interval_max=14,
            ),
            42,
        )
        self.assertEqual(fun.calls, 11)


class test_utils(Case):

    def test_maybe_list(self):
        self.assertIsNone(maybe_list(None))
        self.assertEqual(maybe_list(1), [1])
        self.assertEqual(maybe_list([1, 2, 3]), [1, 2, 3])

    def test_fxrange_no_repeatlast(self):
        self.assertEqual(list(fxrange(1.0, 3.0, 1.0)),
                         [1.0, 2.0, 3.0])

    def test_fxrangemax(self):
        self.assertEqual(list(fxrangemax(1.0, 3.0, 1.0, 30.0)),
                         [1.0, 2.0, 3.0, 3.0, 3.0, 3.0,
                          3.0, 3.0, 3.0, 3.0, 3.0])
        self.assertEqual(list(fxrangemax(1.0, None, 1.0, 30.0)),
                         [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])

    def test_reprkwargs(self):
        self.assertTrue(reprkwargs({'foo': 'bar', 1: 2, 'k': 'v'}))

    def test_reprcall(self):
        self.assertTrue(
            reprcall('add', (2, 2), {'copy': True}),
        )
