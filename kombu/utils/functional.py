
import threading

from collections import Iterable, Mapping, OrderedDict, UserDict
from typing import (
    Any, Callable, Dict, Iterator, KeysView, Optional, Sequence, Tuple,
)

from vine.utils import wraps

__all__ = [
    'LRUCache', 'memoize', 'lazy', 'maybe_evaluate',
    'is_list', 'maybe_list', 'dictfilter',
]

KEYWORD_MARK = object()

MemoizeKeyFun = Callable[[Sequence, Mapping], Any]


class LRUCache(UserDict):
    """LRU Cache implementation using a doubly linked list to track access.

    :keyword limit: The maximum number of keys to keep in the cache.
        When a new key is inserted and the limit has been exceeded,
        the *Least Recently Used* key will be discarded from the
        cache.

    """

    def __init__(self, limit: Optional[int]=None) -> None:
        self.limit = limit
        self.mutex = threading.RLock()
        self.data = OrderedDict()  # type: OrderedDict

    def __getitem__(self, key: Any) -> Any:
        with self.mutex:
            value = self[key] = self.data.pop(key)
            return value

    def update(self, *args, **kwargs) -> None:
        with self.mutex:
            data, limit = self.data, self.limit
            data.update(*args, **kwargs)
            if limit and len(data) > limit:
                # pop additional items in case limit exceeded
                for _ in range(len(data) - limit):
                    data.popitem(last=False)

    def popitem(self, last: bool=True) -> Any:
        with self.mutex:
            return self.data.popitem(last)

    def __setitem__(self, key: Any, value: Any) -> None:
        # remove least recently used key.
        with self.mutex:
            if self.limit and len(self.data) >= self.limit:
                self.data.pop(next(iter(self.data)))
            self.data[key] = value

    def __iter__(self) -> Iterator:
        return iter(self.data)

    def items(self) -> Iterator[Tuple[Any, Any]]:
        with self.mutex:
            for k in self:
                try:
                    yield (k, self.data[k])
                except KeyError:  # pragma: no cover
                    pass

    def values(self) -> Iterator[Any]:
        with self.mutex:
            for k in self:
                try:
                    yield self.data[k]
                except KeyError:  # pragma: no cover
                    pass

    def keys(self) -> KeysView:
        # userdict.keys in py3k calls __getitem__
        with self.mutex:
            return self.data.keys()

    def incr(self, key: Any, delta: int=1) -> Any:
        with self.mutex:
            # this acts as memcached does- store as a string, but return a
            # integer as long as it exists and we can cast it
            newval = int(self.data.pop(key)) + delta
            self[key] = str(newval)
            return newval

    def __getstate__(self) -> Mapping[str, Any]:
        d = dict(vars(self))
        d.pop('mutex')
        return d

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__ = state
        self.mutex = threading.RLock()


def memoize(maxsize: Optional[int]=None,
            keyfun: Optional[MemoizeKeyFun]=None,
            Cache: Any=LRUCache) -> Callable:

    def _memoize(fun: Callable) -> Callable:
        mutex = threading.Lock()
        cache = Cache(limit=maxsize)

        @wraps(fun)
        def _M(*args, **kwargs) -> Any:
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

        def clear() -> None:
            """Clear the cache and reset cache statistics."""
            cache.clear()
            _M.hits = _M.misses = 0

        _M.hits = _M.misses = 0
        _M.clear = clear
        _M.original_func = fun
        return _M

    return _memoize


class lazy:
    """Holds lazy evaluation.

    Evaluated when called or if the :meth:`evaluate` method is called.
    The function is re-evaluated on every call.

    Overloaded operations that will evaluate the promise:
        :meth:`__str__`, :meth:`__repr__`, :meth:`__cmp__`.

    """

    def __init__(self, fun: Callable, *args, **kwargs) -> None:
        self._fun = fun
        self._args = args
        self._kwargs = kwargs

    def __call__(self) -> Any:
        return self.evaluate()

    def evaluate(self) -> Any:
        return self._fun(*self._args, **self._kwargs)

    def __str__(self) -> str:
        return str(self())

    def __repr__(self) -> str:
        return repr(self())

    def __eq__(self, rhs) -> bool:
        return self() == rhs

    def __ne__(self, rhs) -> bool:
        return self() != rhs

    def __deepcopy__(self, memo: Dict) -> Any:
        memo[id(self)] = self
        return self

    def __reduce__(self) -> Any:
        return (self.__class__, (self._fun,), {'_args': self._args,
                                               '_kwargs': self._kwargs})


def maybe_evaluate(value: Any) -> Any:
    """Evaluates if the value is a :class:`lazy` instance."""
    if isinstance(value, lazy):
        return value.evaluate()
    return value


def is_list(l: Any,
            scalars: tuple=(Mapping, str),
            iters: tuple=(Iterable,)) -> bool:
    """Return true if the object is iterable (but not
    if object is a mapping or string)."""
    return isinstance(l, iters) and not isinstance(l, scalars or ())


def maybe_list(l: Any, scalars: tuple=(Mapping, str)) -> Optional[Sequence]:
    """Return list of one element if ``l`` is a scalar."""
    return l if l is None or is_list(l, scalars) else [l]


def dictfilter(d: Optional[Mapping]=None, **kw) -> Mapping:
    """Remove all keys from dict ``d`` whose value is :const:`None`"""
    d = kw if d is None else (dict(d, **kw) if kw else d)
    return {k: v for k, v in d.items() if v is not None}


# Compat names (before kombu 3.0)
promise = lazy
maybe_promise = maybe_evaluate
