from __future__ import absolute_import, unicode_literals

import numbers
import sys

from functools import wraps

from contextlib import contextmanager

from kombu.five import reraise

try:
    from io import UnsupportedOperation
    FILENO_ERRORS = (AttributeError, ValueError, UnsupportedOperation)
except ImportError:  # pragma: no cover
    # Py2
    FILENO_ERRORS = (AttributeError, ValueError)  # noqa

try:
    from billiard.util import register_after_fork
except ImportError:  # pragma: no cover
    try:
        from multiprocessing.util import register_after_fork  # noqa
    except ImportError:
        register_after_fork = None  # noqa


def coro(gen):

    @wraps(gen)
    def wind_up(*args, **kwargs):
        it = gen(*args, **kwargs)
        next(it)
        return it
    return wind_up


def entrypoints(namespace):
    try:
        from pkg_resources import iter_entry_points
    except ImportError:
        return iter([])
    return ((ep, ep.load()) for ep in iter_entry_points(namespace))


def fileno(f):
    if isinstance(f, numbers.Integral):
        return f
    return f.fileno()


def maybe_fileno(f):
    """Get object fileno, or :const:`None` if not defined."""
    try:
        return fileno(f)
    except FILENO_ERRORS:
        pass


@contextmanager
def nested(*managers):  # pragma: no cover
    # flake8: noqa
    """Combine multiple context managers into a single nested
    context manager."""
    exits = []
    vars = []
    exc = (None, None, None)
    try:
        try:
            for mgr in managers:
                exit = mgr.__exit__
                enter = mgr.__enter__
                vars.append(enter())
                exits.append(exit)
            yield vars
        except:
            exc = sys.exc_info()
        finally:
            while exits:
                exit = exits.pop()
                try:
                    if exit(*exc):
                        exc = (None, None, None)
                except:
                    exc = sys.exc_info()
            if exc != (None, None, None):
                # Don't rely on sys.exc_info() still containing
                # the right information.  Another exception may
                # have been raised and caught by an exit method
                reraise(exc[0], exc[1], exc[2])
    finally:
        del(exc)
