from time import sleep
from uuid import UUID, uuid4, _uuid_generate_random

try:
    import ctypes
except ImportError:
    ctypes = None


def gen_unique_id():
    """Generate a unique id, having - hopefully - a very small chance of
    collission.

    For now this is provided by :func:`uuid.uuid4`.
    """
    # Workaround for http://bugs.python.org/issue4607
    if ctypes and _uuid_generate_random:
        buffer = ctypes.create_string_buffer(16)
        _uuid_generate_random(buffer)
        return str(UUID(bytes=buffer.raw))
    return str(uuid4())


def kwdict(kwargs):
    """Make sure keyword arguments are not in unicode.

    This should be fixed in newer Python versions,
      see: http://bugs.python.org/issue4978.

    """
    return dict((key.encode("utf-8"), value)
                    for key, value in kwargs.items())


def maybe_list(v):
    if v is None:
        return []
    if hasattr(v, "__iter__"):
        return v
    return [v]


def repeatlast(it):
    """Iterate over all elements in the iterator, and when its exhausted
    yield the last value infinitely."""
    for item in it:
        yield item
    while 1: # pragma: no cover
        yield item


def retry_over_time(fun, catch, args=[], kwargs={}, errback=None,
        max_retries=None, interval_start=2, interval_step=2, interval_max=30):
    """Retry the function over and over until max retries is exceeded.

    For each retry we sleep a for a while before we try again, this interval
    is increased for every retry until the max seconds is reached.

    :param fun: The function to try
    :param catch: Exceptions to catch, can be either tuple or a single
        exception class.
    :keyword args: Positional arguments passed on to the function.
    :keyword kwargs: Keyword arguments passed on to the function.
    :keyword errback: Callback for when an exception in ``catch`` is raised.
        The callback must take two arguments: ``exc`` and ``interval``, where
        ``exc`` is the exception instance, and ``interval`` is the time in
        seconds to sleep next..
    :keyword max_retries: Maximum number of retries before we give up.
        If this is not set, we will retry forever.
    :keyword interval_start: How long (in seconds) we start sleeping between
        retries.
    :keyword interval_step: By how much the interval is increased for each
        retry.
    :keyword interval_max: Maximum number of seconds to sleep between retries.

    """
    retries = 0
    interval_range = xrange(interval_start,
                            interval_max + interval_start,
                            interval_step)

    for retries, interval in enumerate(repeatlast(interval_range)):
        try:
            retval = fun(*args, **kwargs)
        except catch, exc:
            if max_retries and retries > max_retries:
                raise
            if errback:
                errback(exc, interval)
            sleep(interval)
        else:
            return retval


############## str.partition/str.rpartition #################################

def _compat_rl_partition(S, sep, direction=None, reverse=False):
    items = direction(sep, 1)
    if len(items) == 1:
        if reverse:
            return '', '', items[0]
        return items[0], '', ''
    return items[0], sep, items[1]


def _compat_partition(S, sep):
    """``partition(S, sep) -> (head, sep, tail)``

    Search for the separator ``sep`` in ``S``, and return the part before
    it, the separator itself, and the part after it. If the separator is not
    found, return ``S`` and two empty strings.

    """
    return _compat_rl_partition(S, sep, direction=S.split)


def _compat_rpartition(S, sep):
    """``rpartition(S, sep) -> (tail, sep, head)``

    Search for the separator ``sep`` in ``S``, starting at the end of ``S``,
    and return the part before it, the separator itself, and the part
    after it. If the separator is not found, return two empty
    strings and ``S``.

    """
    return _compat_rl_partition(S, sep, direction=S.rsplit, reverse=True)


def partition(S, sep):
    if hasattr(S, 'partition'):
        return S.partition(sep)
    else:  # Python <= 2.4:
        return _compat_partition(S, sep)


def rpartition(S, sep):
    if hasattr(S, 'rpartition'):
        return S.rpartition(sep)
    else:  # Python <= 2.4:
        return _compat_rpartition(S, sep)


############## collections.OrderedDict #######################################

import weakref
try:
    from collections import MutableMapping
except ImportError:
    from UserDict import DictMixin as MutableMapping
from itertools import imap as _imap
from operator import eq as _eq


class _Link(object):
    """Doubly linked list."""
    __slots__ = 'prev', 'next', 'key', '__weakref__'


class OrderedDict(dict, MutableMapping):
    """Dictionary that remembers insertion order"""
    # An inherited dict maps keys to values.
    # The inherited dict provides __getitem__, __len__, __contains__, and get.
    # The remaining methods are order-aware.
    # Big-O running times for all methods are the same as for regular
    # dictionaries.

    # The internal self.__map dictionary maps keys to links in a doubly
    # linked list.
    # The circular doubly linked list starts and ends with a sentinel element.
    # The sentinel element never gets deleted (this simplifies the algorithm).
    # The prev/next links are weakref proxies (to prevent circular
    # references).
    # Individual links are kept alive by the hard reference in self.__map.
    # Those hard references disappear when a key is deleted from
    # an OrderedDict.

    __marker = object()

    def __init__(self, *args, **kwds):
        """Initialize an ordered dictionary.

        Signature is the same as for regular dictionaries, but keyword
        arguments are not recommended because their insertion order is
        arbitrary.

        """
        if len(args) > 1:
            raise TypeError("expected at most 1 arguments, got %d" % (
                                len(args)))
        try:
            self.__root
        except AttributeError:
            # sentinel node for the doubly linked list
            self.__root = root = _Link()
            root.prev = root.next = root
            self.__map = {}
        self.update(*args, **kwds)

    def clear(self):
        "od.clear() -> None.  Remove all items from od."
        root = self.__root
        root.prev = root.next = root
        self.__map.clear()
        dict.clear(self)

    def __setitem__(self, key, value):
        "od.__setitem__(i, y) <==> od[i]=y"
        # Setting a new item creates a new link which goes at the end of the
        # linked list, and the inherited dictionary is updated with the new
        # key/value pair.
        if key not in self:
            self.__map[key] = link = _Link()
            root = self.__root
            last = root.prev
            link.prev, link.next, link.key = last, root, key
            last.next = root.prev = weakref.proxy(link)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        """od.__delitem__(y) <==> del od[y]"""
        # Deleting an existing item uses self.__map to find the
        # link which is then removed by updating the links in the
        # predecessor and successor nodes.
        dict.__delitem__(self, key)
        link = self.__map.pop(key)
        link.prev.next = link.next
        link.next.prev = link.prev

    def __iter__(self):
        """od.__iter__() <==> iter(od)"""
        # Traverse the linked list in order.
        root = self.__root
        curr = root.next
        while curr is not root:
            yield curr.key
            curr = curr.next

    def __reversed__(self):
        """od.__reversed__() <==> reversed(od)"""
        # Traverse the linked list in reverse order.
        root = self.__root
        curr = root.prev
        while curr is not root:
            yield curr.key
            curr = curr.prev

    def __reduce__(self):
        """Return state information for pickling"""
        items = [[k, self[k]] for k in self]
        tmp = self.__map, self.__root
        del(self.__map, self.__root)
        inst_dict = vars(self).copy()
        self.__map, self.__root = tmp
        if inst_dict:
            return (self.__class__, (items,), inst_dict)
        return self.__class__, (items,)

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
        return default

    def update(self, other=(), **kwds):
        if isinstance(other, dict):
            for key in other:
                self[key] = other[key]
        elif hasattr(other, "keys"):
            for key in other.keys():
                self[key] = other[key]
        else:
            for key, value in other:
                self[key] = value
        for key, value in kwds.items():
            self[key] = value

    def pop(self, key, default=__marker):
        try:
            value = self[key]
        except KeyError:
            if default is self.__marker:
                raise
            return default
        else:
            del self[key]
            return value

    def values(self):
        return [self[key] for key in self]

    def items(self):
        return [(key, self[key]) for key in self]

    def itervalues(self):
        for key in self:
            yield self[key]

    def iteritems(self):
        for key in self:
            yield (key, self[key])

    def iterkeys(self):
        return iter(self)

    def keys(self):
        return list(self)

    def popitem(self, last=True):
        """od.popitem() -> (k, v)

        Return and remove a (key, value) pair.
        Pairs are returned in LIFO order if last is true or FIFO
        order if false.

        """
        if not self:
            raise KeyError('dictionary is empty')
        key = (last and reversed(self) or iter(self)).next()
        value = self.pop(key)
        return key, value

    def __repr__(self):
        "od.__repr__() <==> repr(od)"
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, self.items())

    def copy(self):
        "od.copy() -> a shallow copy of od"
        return self.__class__(self)

    @classmethod
    def fromkeys(cls, iterable, value=None):
        """OD.fromkeys(S[, v]) -> New ordered dictionary with keys from S
        and values equal to v (which defaults to None)."""
        d = cls()
        for key in iterable:
            d[key] = value
        return d

    def __eq__(self, other):
        """od.__eq__(y) <==> od==y.  Comparison to another OD is
        order-sensitive while comparison to a regular mapping
        is order-insensitive."""
        if isinstance(other, OrderedDict):
            return len(self) == len(other) and \
                   all(_imap(_eq, self.iteritems(), other.iteritems()))
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)
