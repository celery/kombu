from __future__ import absolute_import, unicode_literals

import pickle

import pytest

from itertools import count

from case import Mock, mock, skip

from kombu.five import items
from kombu.utils import functional as utils
from kombu.utils.functional import (
    ChannelPromise, LRUCache, fxrange, fxrangemax, memoize, lazy,
    maybe_evaluate, maybe_list, reprcall, reprkwargs, retry_over_time,
)


class test_ChannelPromise:

    def test_repr(self):
        obj = Mock(name='cb')
        assert 'promise' in repr(ChannelPromise(obj))
        obj.assert_not_called()


class test_shufflecycle:

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
            assert seen.issubset(values)
            with pytest.raises(StopIteration):
                next(cycle)
                next(cycle)
        finally:
            utils.repeat = prev_repeat


def double(x):
    return x * 2


class test_LRUCache:

    def test_expires(self):
        limit = 100
        x = LRUCache(limit=limit)
        slots = list(range(limit * 2))
        for i in slots:
            x[i] = i
        assert list(x.keys()) == list(slots[limit:])
        assert x.items()
        assert x.values()

    def test_is_pickleable(self):
        x = LRUCache(limit=10)
        x.update(luke=1, leia=2)
        y = pickle.loads(pickle.dumps(x))
        assert y.limit == y.limit
        assert y == x

    def test_update_expires(self):
        limit = 100
        x = LRUCache(limit=limit)
        slots = list(range(limit * 2))
        for i in slots:
            x.update({i: i})

        assert list(x.keys()) == list(slots[limit:])

    def test_least_recently_used(self):
        x = LRUCache(3)

        x[1], x[2], x[3] = 1, 2, 3
        assert list(x.keys()), [1, 2 == 3]

        x[4], x[5] = 4, 5
        assert list(x.keys()), [3, 4 == 5]

        # access 3, which makes it the last used key.
        x[3]
        x[6] = 6
        assert list(x.keys()), [5, 3 == 6]

        x[7] = 7
        assert list(x.keys()), [3, 6 == 7]

    def test_update_larger_than_cache_size(self):
        x = LRUCache(2)
        x.update({x: x for x in range(100)})
        assert list(x.keys()), [98 == 99]

    def test_items(self):
        c = LRUCache()
        c.update(a=1, b=2, c=3)
        assert list(items(c))

    def test_incr(self):
        c = LRUCache()
        c.update(a='1')
        c.incr('a')
        assert c['a'] == '2'


def test_memoize():
    counter = count(1)

    @memoize(maxsize=2)
    def x(i):
        return next(counter)

    assert x(1) == 1
    assert x(1) == 1
    assert x(2) == 2
    assert x(3) == 3
    assert x(1) == 4
    x.clear()
    assert x(3) == 5


class test_lazy:

    def test__str__(self):
        assert (str(lazy(lambda: 'the quick brown fox')) ==
                'the quick brown fox')

    def test__repr__(self):
        assert repr(lazy(lambda: 'fi fa fo')).strip('u') == "'fi fa fo'"

    @skip.if_python3()
    def test__cmp__(self):
        assert lazy(lambda: 10).__cmp__(lazy(lambda: 20)) == -1
        assert lazy(lambda: 10).__cmp__(5) == 1

    def test_evaluate(self):
        assert lazy(lambda: 2 + 2)() == 4
        assert lazy(lambda x: x * 4, 2) == 8
        assert lazy(lambda x: x * 8, 2)() == 16

    def test_cmp(self):
        assert lazy(lambda: 10) == lazy(lambda: 10)
        assert lazy(lambda: 10) != lazy(lambda: 20)

    def test__reduce__(self):
        x = lazy(double, 4)
        y = pickle.loads(pickle.dumps(x))
        assert x() == y()

    def test__deepcopy__(self):
        from copy import deepcopy
        x = lazy(double, 4)
        y = deepcopy(x)
        assert x._fun == y._fun
        assert x._args == y._args
        assert x() == y()


@pytest.mark.parametrize('obj,expected', [
    (lazy(lambda: 10), 10),
    (20, 20),
])
def test_maybe_evaluate(obj, expected):
    assert maybe_evaluate(obj) == expected


class test_retry_over_time:

    class Predicate(Exception):
        pass

    def setup(self):
        self.index = 0

    def myfun(self):
        if self.index < 9:
            raise self.Predicate()
        return 42

    def errback(self, exc, intervals, retries):
        interval = next(intervals)
        sleepvals = (None, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 16.0)
        self.index += 1
        assert interval == sleepvals[self.index]
        return interval

    @mock.sleepdeprived(module=utils)
    def test_simple(self):
        prev_count, utils.count = utils.count, Mock()
        try:
            utils.count.return_value = list(range(1))
            x = retry_over_time(self.myfun, self.Predicate,
                                errback=None, interval_max=14)
            assert x is None
            utils.count.return_value = list(range(10))
            cb = Mock()
            x = retry_over_time(self.myfun, self.Predicate,
                                errback=self.errback, callback=cb,
                                interval_max=14)
            assert x == 42
            assert self.index == 9
            cb.assert_called_with()
        finally:
            utils.count = prev_count

    def test_retry_timeout(self):

        with pytest.raises(self.Predicate):
            retry_over_time(
                self.myfun, self.Predicate,
                errback=self.errback, interval_max=14, timeout=1
            )
        assert self.index == 1

        # no errback
        with pytest.raises(self.Predicate):
            retry_over_time(
                self.myfun, self.Predicate,
                errback=None, timeout=1,
            )

    @mock.sleepdeprived(module=utils)
    def test_retry_once(self):
        with pytest.raises(self.Predicate):
            retry_over_time(
                self.myfun, self.Predicate,
                max_retries=1, errback=self.errback, interval_max=14,
            )
        assert self.index == 1
        # no errback
        with pytest.raises(self.Predicate):
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

        assert retry_over_time(
            fun, self.Predicate,
            max_retries=0, errback=None, interval_max=14) == 42
        assert fun.calls == 11


@pytest.mark.parametrize('obj,expected', [
    (None, None),
    (1, [1]),
    ([1, 2, 3], [1, 2, 3]),
])
def test_maybe_list(obj, expected):
    assert maybe_list(obj) == expected


def test_fxrange__no_repeatlast():
    assert list(fxrange(1.0, 3.0, 1.0)) == [1.0, 2.0, 3.0]


@pytest.mark.parametrize('args,expected', [
    ((1.0, 3.0, 1.0, 30.0),
     [1.0, 2.0, 3.0, 3.0, 3.0, 3.0,
      3.0, 3.0, 3.0, 3.0, 3.0]),
    ((1.0, None, 1.0, 30.0),
     [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
])
def test_fxrangemax(args, expected):
    assert list(fxrangemax(*args)) == expected


def test_reprkwargs():
    assert reprkwargs({'foo': 'bar', 1: 2, 'k': 'v'})


def test_reprcall():
    assert reprcall('add', (2, 2), {'copy': True})
