from __future__ import absolute_import

import importlib
import os
import sys
import types

from contextlib import contextmanager
from functools import wraps
from io import StringIO

try:
    from unittest import mock
except ImportError:
    import mock  # noqa

from nose import SkipTest

from kombu.five import builtins, string_t
from kombu.utils.encoding import ensure_bytes

try:
    import unittest
    unittest.skip
except AttributeError:
    import unittest2 as unittest  # noqa

PY3 = sys.version_info[0] == 3

patch = mock.patch
call = mock.call


class Case(unittest.TestCase):

    def setUp(self):
        self.setup()

    def tearDown(self):
        self.teardown()

    def assertItemsEqual(self, a, b, *args, **kwargs):
        return self.assertEqual(sorted(a), sorted(b), *args, **kwargs)
    assertSameElements = assertItemsEqual

    def setup(self):
        pass

    def teardown(self):
        pass


def PromiseMock(*args, **kwargs):
    m = Mock(*args, **kwargs)

    def on_throw(exc=None, *args, **kwargs):
        if exc:
            raise exc
        raise
    m.throw.side_effect = on_throw
    m.set_error_state.side_effect = on_throw
    m.throw1.side_effect = on_throw
    return m


def case_requires(package, *more_packages):
    packages = [package] + list(more_packages)

    def attach(cls):
        setup = cls.setUp

        @wraps(setup)
        def around_setup(self):
            for package in packages:
                try:
                    importlib.import_module(package)
                except ImportError:
                    raise SkipTest('{0} is not installed'.format(package))
            setup(self)
        cls.setUp = around_setup
        return cls
    return attach


def case_no_pypy(cls):
    setup = cls.setUp

    @wraps(setup)
    def around_setup(self):
        if getattr(sys, 'pypy_version_info', None):
            raise SkipTest('pypy incompatible')
        setup(self)
    cls.setUp = around_setup
    return cls


def case_no_python3(cls):
    setup = cls.setUp

    @wraps(setup)
    def around_setup(self):
        if PY3:
            raise SkipTest('Python3 incompatible')
        setup(self)
    cls.setUp = around_setup
    return cls


class HubCase(Case):

    def setUp(self):
        from kombu.async import Hub, get_event_loop, set_event_loop
        self._prev_hub = get_event_loop()
        self.hub = Hub()
        set_event_loop(self.hub)
        super(HubCase, self).setUp()

    def tearDown(self):
        try:
            super(HubCase, self).tearDown()
        finally:
            from kombu.async import set_event_loop
            if self._prev_hub is not None:
                set_event_loop(self._prev_hub)


class Mock(mock.Mock):

    def __init__(self, *args, **kwargs):
        attrs = kwargs.pop('attrs', None) or {}
        super(Mock, self).__init__(*args, **kwargs)
        for attr_name, attr_value in attrs.items():
            setattr(self, attr_name, attr_value)


class _ContextMock(Mock):
    """Dummy class implementing __enter__ and __exit__
    as the with statement requires these to be implemented
    in the class, not just the instance."""

    def __enter__(self):
        pass

    def __exit__(self, *exc_info):
        pass


def ContextMock(*args, **kwargs):
    obj = _ContextMock(*args, **kwargs)
    obj.attach_mock(Mock(), '__enter__')
    obj.attach_mock(Mock(), '__exit__')
    obj.__enter__.return_value = obj
    # if __exit__ return a value the exception is ignored,
    # so it must return None here.
    obj.__exit__.return_value = None
    return obj


class MockPool(object):

    def __init__(self, value=None):
        self.value = value or ContextMock()

    def acquire(self, **kwargs):
        return self.value


def redirect_stdouts(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        try:
            return fun(*args, **dict(kwargs,
                                     stdout=sys.stdout, stderr=sys.stderr))
        finally:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__

    return _inner


def module_exists(*modules):

    def _inner(fun):

        @wraps(fun)
        def __inner(*args, **kwargs):
            gen = []
            for module in modules:
                if isinstance(module, string_t):
                    if not PY3:
                        module = ensure_bytes(module)
                    module = types.ModuleType(module)
                gen.append(module)
                sys.modules[module.__name__] = module
                name = module.__name__
                if '.' in name:
                    parent, _, attr = name.rpartition('.')
                    setattr(sys.modules[parent], attr, module)
            try:
                return fun(*args, **kwargs)
            finally:
                for module in gen:
                    sys.modules.pop(module.__name__, None)

        return __inner
    return _inner


# Taken from
# http://bitbucket.org/runeh/snippets/src/tip/missing_modules.py
def mask_modules(*modnames):
    def _inner(fun):

        @wraps(fun)
        def __inner(*args, **kwargs):
            realimport = builtins.__import__

            def myimp(name, *args, **kwargs):
                if name in modnames:
                    raise ImportError('No module named %s' % name)
                else:
                    return realimport(name, *args, **kwargs)

            builtins.__import__ = myimp
            try:
                return fun(*args, **kwargs)
            finally:
                builtins.__import__ = realimport

        return __inner
    return _inner


def skip_if_pypy(fun):

    @wraps(fun)
    def _skips_if_pypy(*args, **kwargs):
        if getattr(sys, 'pypy_version_info', None):
            raise SkipTest('pypy incompatible')
        return fun(*args, **kwargs)

    return _skips_if_pypy


def skip_if_environ(env_var_name):

    def _wrap_test(fun):

        @wraps(fun)
        def _skips_if_environ(*args, **kwargs):
            if os.environ.get(env_var_name):
                raise SkipTest('SKIP %s: %s set\n' % (
                    fun.__name__, env_var_name))
            return fun(*args, **kwargs)

        return _skips_if_environ

    return _wrap_test


def skip_if_module(module):
    def _wrap_test(fun):
        @wraps(fun)
        def _skip_if_module(*args, **kwargs):
            try:
                __import__(module)
                raise SkipTest('SKIP %s: %s available\n' % (
                    fun.__name__, module))
            except ImportError:
                pass
            return fun(*args, **kwargs)
        return _skip_if_module
    return _wrap_test


def skip_if_not_module(module, import_errors=(ImportError, )):
    def _wrap_test(fun):
        @wraps(fun)
        def _skip_if_not_module(*args, **kwargs):
            try:
                __import__(module)
            except import_errors:
                raise SkipTest('SKIP %s: %s available\n' % (
                    fun.__name__, module))
            return fun(*args, **kwargs)
        return _skip_if_not_module
    return _wrap_test


@contextmanager
def set_module_symbol(module, key, value):
    module = importlib.import_module(module)
    prev = getattr(module, key)
    setattr(module, key, value)
    try:
        yield
    finally:
        setattr(module, key, prev)
