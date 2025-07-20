from __future__ import annotations

import atexit
import builtins
import io
import os
import sys
import types
from unittest.mock import MagicMock

import pytest

from kombu.exceptions import VersionMismatch

_SIO_write = io.StringIO.write
_SIO_init = io.StringIO.__init__
sentinel = object()


@pytest.fixture(scope='session')
def multiprocessing_workaround(request):
    yield
    # Workaround for multiprocessing bug where logging
    # is attempted after global already collected at shutdown.
    canceled = set()
    try:
        import multiprocessing.util
        canceled.add(multiprocessing.util._exit_function)
    except (AttributeError, ImportError):
        pass

    try:
        atexit._exithandlers[:] = [
            e for e in atexit._exithandlers if e[0] not in canceled
        ]
    except AttributeError:  # pragma: no cover
        pass  # Py3 missing _exithandlers


def zzz_reset_memory_transport_state():
    yield
    from kombu.transport import memory
    memory.Transport.state.clear()


@pytest.fixture(autouse=True)
def test_cases_has_patching(request, patching):
    if request.instance:
        request.instance.patching = patching


@pytest.fixture
def hub(request):
    from kombu.asynchronous import Hub, get_event_loop, set_event_loop
    _prev_hub = get_event_loop()
    hub = Hub()
    set_event_loop(hub)

    yield hub

    if _prev_hub is not None:
        set_event_loop(_prev_hub)


def find_distribution_modules(name=__name__, file=__file__):
    current_dist_depth = len(name.split('.')) - 1
    current_dist = os.path.join(os.path.dirname(file),
                                *([os.pardir] * current_dist_depth))
    abs = os.path.abspath(current_dist)
    dist_name = os.path.basename(abs)

    for dirpath, dirnames, filenames in os.walk(abs):
        package = (dist_name + dirpath[len(abs):]).replace('/', '.')
        if '__init__.py' in filenames:
            yield package
            for filename in filenames:
                if filename.endswith('.py') and filename != '__init__.py':
                    yield '.'.join([package, filename])[:-3]


def import_all_modules(name=__name__, file=__file__, skip=[]):
    for module in find_distribution_modules(name, file):
        if module not in skip:
            print(f'preimporting {module!r} for coverage...')
            try:
                __import__(module)
            except (ImportError, VersionMismatch, AttributeError):
                pass


def is_in_coverage():
    return (os.environ.get('COVER_ALL_MODULES') or
            any('--cov' in arg for arg in sys.argv))


@pytest.fixture(scope='session')
def cover_all_modules():
    # so coverage sees all our modules.
    if is_in_coverage():
        import_all_modules()


class WhateverIO(io.StringIO):

    def __init__(self, v=None, *a, **kw):
        _SIO_init(self, v.decode() if isinstance(v, bytes) else v, *a, **kw)

    def write(self, data):
        _SIO_write(self, data.decode() if isinstance(data, bytes) else data)


def noop(*args, **kwargs):
    pass


def module_name(s):
    if isinstance(s, bytes):
        return s.decode()
    return s


class _patching:

    def __init__(self, monkeypatch, request):
        self.monkeypatch = monkeypatch
        self.request = request

    def __getattr__(self, name):
        return getattr(self.monkeypatch, name)

    def __call__(self, path, value=sentinel, name=None,
                 new=MagicMock, **kwargs):
        value = self._value_or_mock(value, new, name, path, **kwargs)
        self.monkeypatch.setattr(path, value)
        return value

    def _value_or_mock(self, value, new, name, path, **kwargs):
        if value is sentinel:
            value = new(name=name or path.rpartition('.')[2])
        for k, v in kwargs.items():
            setattr(value, k, v)
        return value

    def setattr(self, target, name=sentinel, value=sentinel, **kwargs):
        # alias to __call__ with the interface of pytest.monkeypatch.setattr
        if value is sentinel:
            value, name = name, None
        return self(target, value, name=name)

    def setitem(self, dic, name, value=sentinel, new=MagicMock, **kwargs):
        # same as pytest.monkeypatch.setattr but default value is MagicMock
        value = self._value_or_mock(value, new, name, dic, **kwargs)
        self.monkeypatch.setitem(dic, name, value)
        return value


class _stdouts:

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr


@pytest.fixture
def stdouts():
    """Override `sys.stdout` and `sys.stderr` with `StringIO`
    instances.
    Decorator example::
        @mock.stdouts
        def test_foo(self, stdout, stderr):
            something()
            self.assertIn('foo', stdout.getvalue())
    Context example::
        with mock.stdouts() as (stdout, stderr):
            something()
            self.assertIn('foo', stdout.getvalue())
    """
    prev_out, prev_err = sys.stdout, sys.stderr
    prev_rout, prev_rerr = sys.__stdout__, sys.__stderr__
    mystdout, mystderr = WhateverIO(), WhateverIO()
    sys.stdout = sys.__stdout__ = mystdout
    sys.stderr = sys.__stderr__ = mystderr

    try:
        yield _stdouts(mystdout, mystderr)
    finally:
        sys.stdout = prev_out
        sys.stderr = prev_err
        sys.__stdout__ = prev_rout
        sys.__stderr__ = prev_rerr


@pytest.fixture
def patching(monkeypatch, request):
    """Monkeypath.setattr shortcut.
    Example:
        .. code-block:: python
        def test_foo(patching):
            # execv value here will be mock.MagicMock by default.
            execv = patching('os.execv')
            patching('sys.platform', 'darwin')  # set concrete value
            patching.setenv('DJANGO_SETTINGS_MODULE', 'x.settings')
            # val will be of type mock.MagicMock by default
            val = patching.setitem('path.to.dict', 'KEY')
    """
    return _patching(monkeypatch, request)


@pytest.fixture
def sleepdeprived(request):
    """Mock sleep method in patched module to do nothing.

    Example:
        >>> import time
        >>> @pytest.mark.sleepdeprived_patched_module(time)
        >>> def test_foo(self, patched_module):
        >>>     pass
    """
    module = request.node.get_closest_marker(
            "sleepdeprived_patched_module").args[0]
    old_sleep, module.sleep = module.sleep, noop
    try:
        yield
    finally:
        module.sleep = old_sleep


@pytest.fixture
def module_exists(request):
    """Patch one or more modules to ensure they exist.

    A module name with multiple paths (e.g. gevent.monkey) will
    ensure all parent modules are also patched (``gevent`` +
    ``gevent.monkey``).

    Example:
        >>> @pytest.mark.ensured_modules('gevent.monkey')
        >>> def test_foo(self, module_exists):
        ...    pass

    """
    gen = []
    old_modules = []
    modules = request.node.get_closest_marker("ensured_modules").args
    for module in modules:
        if isinstance(module, str):
            module = types.ModuleType(module_name(module))
        gen.append(module)
        if module.__name__ in sys.modules:
            old_modules.append(sys.modules[module.__name__])
        sys.modules[module.__name__] = module
        name = module.__name__
        if '.' in name:
            parent, _, attr = name.rpartition('.')
            setattr(sys.modules[parent], attr, module)
    try:
        yield
    finally:
        for module in gen:
            sys.modules.pop(module.__name__, None)
        for module in old_modules:
            sys.modules[module.__name__] = module


# Taken from
# http://bitbucket.org/runeh/snippets/src/tip/missing_modules.py
@pytest.fixture
def mask_modules(request):
    """Ban some modules from being importable inside the context

    For example::

        >>> @pytest.mark.masked_modules('gevent.monkey')
        >>> def test_foo(self, mask_modules):
        ...     try:
        ...         import sys
        ...     except ImportError:
        ...         print('sys not found')
        sys not found
    """
    realimport = builtins.__import__
    modnames = request.node.get_closest_marker("masked_modules").args

    def myimp(name, *args, **kwargs):
        if name in modnames:
            raise ImportError('No module named %s' % name)
        else:
            return realimport(name, *args, **kwargs)

    builtins.__import__ = myimp
    try:
        yield
    finally:
        builtins.__import__ = realimport


@pytest.fixture
def replace_module_value(request):
    """Mock module value, given a module, attribute name and value.

    Decorator example::

        >>> @pytest.mark.replace_module_value(module, 'CONSTANT', 3.03)
        >>> def test_foo(self, replace_module_value):
        ...     pass
    """
    module = request.node.get_closest_marker("replace_module_value").args[0]
    name = request.node.get_closest_marker("replace_module_value").args[1]
    value = request.node.get_closest_marker("replace_module_value").args[2]
    has_prev = hasattr(module, name)
    prev = getattr(module, name, None)
    if value:
        setattr(module, name, value)
    else:
        try:
            delattr(module, name)
        except AttributeError:
            pass
    try:
        yield
    finally:
        if prev is not None:
            setattr(module, name, prev)
        if not has_prev:
            try:
                delattr(module, name)
            except AttributeError:
                pass
