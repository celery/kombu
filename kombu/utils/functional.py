"""Functional Utilities."""
import random
import threading
from collections import OrderedDict, UserDict
from itertools import count, repeat
from time import sleep
from typing import (
    Any, Callable, Dict, Iterable, Iterator,
    KeysView, ItemsView, Mapping, Optional, Sequence, Tuple, ValuesView,
)
from amqp.types import ChannelT
from vine.utils import wraps
from ..types import ClientT
from .encoding import safe_repr as _safe_repr

__all__ = [
    'LRUCache', 'memoize', 'lazy', 'maybe_evaluate',
    'is_list', 'maybe_list', 'dictfilter',
]

KEYWORD_MARK = object()
MemoizeKeyFun = Callable[[Sequence, Mapping], Any]


class ChannelPromise(object):
    __value__: ChannelT

    def __init__(self, connection: ClientT) -> None:
        self.__connection__ = connection

    def __call__(self) -> Any:
        try:
            return self.__value__
        except AttributeError:
            value = self.__value__ = self.__connection__.default_channel
            return value

    async def resolve(self):
        try:
            return self.__value__
        except AttributeError:
            await self.__connection__.connect()
            value = self.__value__ = self.__connection__.default_channel
            await value.open()
            return value

    def __repr__(self) -> str:
        try:
            return repr(self.__value__)
        except AttributeError:
            return '<promise: 0x{0:x}>'.format(id(self.__connection__))


class LRUCache(UserDict):
    """LRU Cache implementation using a doubly linked list to track access.

    Arguments:
        limit (int): The maximum number of keys to keep in the cache.
            When a new key is inserted and the limit has been exceeded,
            the *Least Recently Used* key will be discarded from the
            cache.
    """

    def __init__(self, limit: int = None) -> None:
        self.limit = limit
        self.mutex = threading.RLock()
        self.data: OrderedDict = OrderedDict()

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

    def popitem(self, last: bool = True) -> Any:
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

    def items(self) -> ItemsView[Any, Any]:
        with self.mutex:
            for k in self:
                try:
                    yield (k, self.data[k])
                except KeyError:  # pragma: no cover
                    pass

    def values(self) -> ValuesView[Any]:
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

    def incr(self, key: Any, delta: int = 1) -> Any:
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


def memoize(maxsize: int = None,
            keyfun: MemoizeKeyFun = None,
            Cache: Any = LRUCache) -> Callable:
    """Decorator to cache function return value."""

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
        :meth:`__str__`, :meth:`__repr__`
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

    def __eq__(self, rhs: Any) -> bool:
        return self() == rhs

    def __ne__(self, rhs: Any) -> bool:
        return self() != rhs

    def __deepcopy__(self, memo: Dict) -> Any:
        memo[id(self)] = self
        return self

    def __reduce__(self) -> Any:
        return (self.__class__, (self._fun,), {
            '_args': self._args,
            '_kwargs': self._kwargs,
        })


def maybe_evaluate(value: Any) -> Any:
    """Evaluate value only if value is a :class:`lazy` instance."""
    if isinstance(value, lazy):
        return value.evaluate()
    return value


def is_list(l: Any,
            *,
            scalars: tuple = (Mapping, str),
            iters: tuple = (Iterable,)) -> bool:
    """Return true if the object is iterable.

    Note:
        Returns false if object is a mapping or string.
    """
    return isinstance(l, iters) and not isinstance(l, scalars or ())


def maybe_list(l: Any, *,
               scalars: tuple = (Mapping, str)) -> Optional[Sequence]:
    """Return list of one element if ``l`` is a scalar."""
    return l if l is None or is_list(l, scalars) else [l]


def dictfilter(d: Mapping = None, **kw) -> Mapping:
    """Remove all keys from dict ``d`` whose value is :const:`None`."""
    d = kw if d is None else (dict(d, **kw) if kw else d)
    return {k: v for k, v in d.items() if v is not None}


def shufflecycle(it: Sequence) -> Iterator:
    it = list(it)  # don't modify callers list
    shuffle = random.shuffle
    for _ in repeat(None):
        shuffle(it)
        yield it[0]


def fxrange(start: float = 1.0,
            stop: float = None,
            step: float = 1.0,
            repeatlast: bool = False) -> Iterator[float]:
    cur = start * 1.0
    while 1:
        if not stop or cur <= stop:
            yield cur
            cur += step
        else:
            if not repeatlast:
                break
            yield cur - step


def fxrangemax(start: float = 1.0,
               stop: float = None,
               step: float = 1.0,
               max: float = 100.0) -> Iterator[float]:
    sum_, cur = 0, start * 1.0
    while 1:
        if sum_ >= max:
            break
        yield cur
        if stop:
            cur = min(cur + step, stop)
        else:
            cur += step
        sum_ += cur


async def retry_over_time(
        fun: Callable,
        catch: Tuple[Any, ...],
        args: Sequence = [],
        kwargs: Mapping[str, Any] = {},
        *,
        errback: Callable = None,
        max_retries: int = None,
        interval_start: float = 2.0,
        interval_step: float = 2.0,
        interval_max: float = 30.0,
        callback: Callable = None) -> Any:
    """Retry the function over and over until max retries is exceeded.

    For each retry we sleep a for a while before we try again, this interval
    is increased for every retry until the max seconds is reached.

    Arguments:
        fun (Callable): The function to try
        catch (Tuple[BaseException]): Exceptions to catch, can be either
            tuple or a single exception class.

    Keyword Arguments:
        args (Tuple): Positional arguments passed on to the function.
        kwargs (Dict): Keyword arguments passed on to the function.
        errback (Callable): Callback for when an exception in ``catch``
            is raised.  The callback must take three arguments:
            ``exc``, ``interval_range`` and ``retries``, where ``exc``
            is the exception instance, ``interval_range`` is an iterator
            which return the time in seconds to sleep next, and ``retries``
            is the number of previous retries.
        max_retries (int): Maximum number of retries before we give up.
            If this is not set, we will retry forever.
        interval_start (float): How long (in seconds) we start sleeping
            between retries.
        interval_step (float): By how much the interval is increased for
            each retry.
        interval_max (float): Maximum number of seconds to sleep
            between retries.
    """
    retries = 0
    interval_range = fxrange(interval_start,
                             interval_max + interval_start,
                             interval_step, repeatlast=True)
    for retries in count():
        try:
            return await fun(*args, **kwargs)
        except catch as exc:
            if max_retries and retries >= max_retries:
                raise
            if callback:
                callback()
            tts = float(errback(exc, interval_range, retries) if errback
                        else next(interval_range))
            if tts:
                for _ in range(int(tts)):
                    if callback:
                        callback()
                    sleep(1.0)
                # sleep remainder after int truncation above.
                sleep(abs(int(tts) - tts))


def reprkwargs(kwargs: Mapping,
               *,
               sep: str = ', ', fmt: str = '{0}={1}') -> str:
    return sep.join(fmt.format(k, _safe_repr(v)) for k, v in kwargs.items())


def reprcall(name: str,
             args: Sequence = (),
             kwargs: Mapping = {},
             *,
             sep: str = ', ') -> str:
    return '{0}({1}{2}{3})'.format(
        name, sep.join(map(_safe_repr, args or ())),
        (args and kwargs) and sep or '',
        reprkwargs(kwargs, sep),
    )


# Compat names (before kombu 3.0)
promise = lazy
maybe_promise = maybe_evaluate
