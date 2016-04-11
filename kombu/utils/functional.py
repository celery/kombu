from __future__ import absolute_import, unicode_literals

import sys
import threading

from collections import Iterable, Mapping, OrderedDict

from vine.utils import wraps

from kombu.five import (
    UserDict, items, keys, python_2_unicode_compatible, string_t,
)

__all__ = [
    'LRUCache', 'memoize', 'lazy', 'maybe_evaluate',
    'is_list', 'maybe_list', 'dictfilter',
]

KEYWORD_MARK = object()


class LRUCache(UserDict):
    """LRU Cache implementation using a doubly linked list to track access.

    :keyword limit: The maximum number of keys to keep in the cache.
        When a new key is inserted and the limit has been exceeded,
        the *Least Recently Used* key will be discarded from the
        cache.

    """

    def __init__(self, limit=None):
        self.limit = limit
        self.mutex = threading.RLock()
        self.data = OrderedDict()

    def __getitem__(self, key):
        with self.mutex:
            value = self[key] = self.data.pop(key)
            return value

    def update(self, *args, **kwargs):
        with self.mutex:
            data, limit = self.data, self.limit
            data.update(*args, **kwargs)
            if limit and len(data) > limit:
                # pop additional items in case limit exceeded
                for _ in range(len(data) - limit):
                    data.popitem(last=False)

    def popitem(self, last=True):
        with self.mutex:
            return self.data.popitem(last)

    def __setitem__(self, key, value):
        # remove least recently used key.
        with self.mutex:
            if self.limit and len(self.data) >= self.limit:
                self.data.pop(next(iter(self.data)))
            self.data[key] = value

    def __iter__(self):
        return iter(self.data)

    def _iterate_items(self):
        with self.mutex:
            for k in self:
                try:
                    yield (k, self.data[k])
                except KeyError:  # pragma: no cover
                    pass
    iteritems = _iterate_items

    def _iterate_values(self):
        with self.mutex:
            for k in self:
                try:
                    yield self.data[k]
                except KeyError:  # pragma: no cover
                    pass

    itervalues = _iterate_values

    def _iterate_keys(self):
        # userdict.keys in py3k calls __getitem__
        with self.mutex:
            return keys(self.data)
    iterkeys = _iterate_keys

    def incr(self, key, delta=1):
        with self.mutex:
            # this acts as memcached does- store as a string, but return a
            # integer as long as it exists and we can cast it
            newval = int(self.data.pop(key)) + delta
            self[key] = str(newval)
            return newval

    def __getstate__(self):
        d = dict(vars(self))
        d.pop('mutex')
        return d

    def __setstate__(self, state):
        self.__dict__ = state
        self.mutex = threading.RLock()

    if sys.version_info[0] == 3:  # pragma: no cover
        keys = _iterate_keys
        values = _iterate_values
        items = _iterate_items
    else:  # noqa

        def keys(self):
            return list(self._iterate_keys())

        def values(self):
            return list(self._iterate_values())

        def items(self):
            return list(self._iterate_items())


def memoize(maxsize=None, keyfun=None, Cache=LRUCache):

    def _memoize(fun):
        mutex = threading.Lock()
        cache = Cache(limit=maxsize)

        @wraps(fun)
        def _M(*args, **kwargs):
            if keyfun:
                key = keyfun(args, kwargs)
            else:
                key = args + (KEYWORD_MARK,) + tuple(sorted(kwargs.items()))
            try:
                with mutex:
                    value = cache[key]
            except KeyError:
                value = fun(*args, **kwargs)
                _M.misses += 1
                with mutex:
                    cache[key] = value
            else:
                _M.hits += 1
            return value

        def clear():
            """Clear the cache and reset cache statistics."""
            cache.clear()
            _M.hits = _M.misses = 0

        _M.hits = _M.misses = 0
        _M.clear = clear
        _M.original_func = fun
        return _M

    return _memoize


@python_2_unicode_compatible
class lazy(object):
    """Holds lazy evaluation.

    Evaluated when called or if the :meth:`evaluate` method is called.
    The function is re-evaluated on every call.

    Overloaded operations that will evaluate the promise:
        :meth:`__str__`, :meth:`__repr__`, :meth:`__cmp__`.

    """

    def __init__(self, fun, *args, **kwargs):
        self._fun = fun
        self._args = args
        self._kwargs = kwargs

    def __call__(self):
        return self.evaluate()

    def evaluate(self):
        return self._fun(*self._args, **self._kwargs)

    def __str__(self):
        return str(self())

    def __repr__(self):
        return repr(self())

    def __eq__(self, rhs):
        return self() == rhs

    def __ne__(self, rhs):
        return self() != rhs

    def __deepcopy__(self, memo):
        memo[id(self)] = self
        return self

    def __reduce__(self):
        return (self.__class__, (self._fun,), {'_args': self._args,
                                               '_kwargs': self._kwargs})

    if sys.version_info[0] < 3:

        def __cmp__(self, rhs):
            if isinstance(rhs, self.__class__):
                return -cmp(rhs, self())
            return cmp(self(), rhs)


def maybe_evaluate(value):
    """Evaluates if the value is a :class:`lazy` instance."""
    if isinstance(value, lazy):
        return value.evaluate()
    return value


def is_list(l, scalars=(Mapping, string_t), iters=(Iterable,)):
    """Return true if the object is iterable (but not
    if object is a mapping or string)."""
    return isinstance(l, iters) and not isinstance(l, scalars or ())


def maybe_list(l, scalars=(Mapping, string_t)):
    """Return list of one element if ``l`` is a scalar."""
    return l if l is None or is_list(l, scalars) else [l]


def dictfilter(d=None, **kw):
    """Remove all keys from dict ``d`` whose value is :const:`None`"""
    d = kw if d is None else (dict(d, **kw) if kw else d)
    return {k: v for k, v in items(d) if v is not None}


# Compat names (before kombu 3.0)
promise = lazy
maybe_promise = maybe_evaluate
