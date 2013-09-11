from __future__ import absolute_import

__all__ = ['lazy', 'maybe_evaluate']


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

    def __cmp__(self, rhs):
        if isinstance(rhs, self.__class__):
            return -cmp(rhs, self())
        return cmp(self(), rhs)

    def __eq__(self, rhs):
        return self() == rhs

    def __ne__(self, rhs):
        return self() != rhs

    def __deepcopy__(self, memo):
        memo[id(self)] = self
        return self

    def __reduce__(self):
        return (self.__class__, (self._fun, ), {'_args': self._args,
                                                '_kwargs': self._kwargs})


def maybe_evaluate(value):
    """Evaluates if the value is a :class:`lazy` instance."""
    if isinstance(value, lazy):
        return value.evaluate()
    return value


# Compat names (before kombu 3.0)
promise = lazy
maybe_promise = maybe_evaluate
