from __future__ import annotations

import socket
import sys
import types
from unittest.mock import Mock, patch

import pytest

from kombu.utils import compat
from kombu.utils.compat import entrypoints, maybe_fileno


def test_entrypoints():
    with patch(
        'kombu.utils.compat.importlib_metadata.entry_points', create=True
    ) as iterep:
        eps = [Mock(), Mock()]
        iterep.return_value = (
            {'kombu.test': eps} if sys.version_info < (3, 10) else eps)

        assert list(entrypoints('kombu.test'))
        if sys.version_info < (3, 10):
            iterep.assert_called_with()
        else:
            iterep.assert_called_with(group='kombu.test')
        eps[0].load.assert_called_with()
        eps[1].load.assert_called_with()


def test_maybe_fileno():
    assert maybe_fileno(3) == 3
    f = Mock(name='file')
    assert maybe_fileno(f) is f.fileno()
    f.fileno.side_effect = ValueError()
    assert maybe_fileno(f) is None


class test_detect_environment:

    def test_detect_environment(self):
        try:
            compat._environment = None
            X = compat.detect_environment()
            assert compat._environment == X
            Y = compat.detect_environment()
            assert Y == X
        finally:
            compat._environment = None

    @pytest.mark.ensured_modules('eventlet', 'eventlet.patcher')
    def test_detect_environment_eventlet(self, module_exists):
        with patch('eventlet.patcher.is_monkey_patched', create=True) as m:
            assert sys.modules['eventlet']
            m.return_value = True
            env = compat._detect_environment()
            m.assert_called_with(socket)
            assert env == 'eventlet'

    @pytest.mark.ensured_modules('gevent')
    def test_detect_environment_gevent(self, module_exists):
        with patch('gevent.socket', create=True) as m:
            prev, socket.socket = socket.socket, m.socket
            try:
                assert sys.modules['gevent']
                env = compat._detect_environment()
                assert env == 'gevent'
            finally:
                socket.socket = prev

    def test_detect_environment_no_eventlet_or_gevent(self):
        try:
            sys.modules['eventlet'] = types.ModuleType('eventlet')
            sys.modules['eventlet.patcher'] = types.ModuleType('patcher')
            assert compat._detect_environment() == 'default'
        finally:
            sys.modules.pop('eventlet.patcher', None)
            sys.modules.pop('eventlet', None)
        compat._detect_environment()
        try:
            sys.modules['gevent'] = types.ModuleType('gevent')
            assert compat._detect_environment() == 'default'
        finally:
            sys.modules.pop('gevent', None)
        compat._detect_environment()


class test_get_gevent_concurrent_error:
    """Tests for get_gevent_concurrent_error() lazy function.

    The function must be evaluated at call time, not at import time, so
    that it correctly handles the common startup ordering where kombu is
    imported before gevent.monkey.patch_all() is called.
    """

    def test_returns_class_when_gevent_loaded_and_class_exists(self):
        """Returns ConcurrentObjectUseError when gevent is in sys.modules."""
        mock_exc = type('ConcurrentObjectUseError', (AssertionError,), {})
        mock_exc_mod = types.ModuleType('gevent.exceptions')
        mock_exc_mod.ConcurrentObjectUseError = mock_exc

        with patch.dict(sys.modules, {
            'gevent': types.ModuleType('gevent'),
            'gevent.exceptions': mock_exc_mod,
        }):
            result = compat.get_gevent_concurrent_error()

        assert result is mock_exc

    def test_returns_none_when_gevent_not_in_sys_modules(self):
        """Returns None when gevent has not been imported yet.

        This covers the common startup ordering: kombu imported first,
        gevent.monkey.patch_all() called later.
        """
        saved = {k: sys.modules.pop(k) for k in list(sys.modules) if 'gevent' in k}
        try:
            result = compat.get_gevent_concurrent_error()
        finally:
            sys.modules.update(saved)

        assert result is None

    def test_returns_none_when_gevent_loaded_but_class_missing(self):
        """Returns None when gevent is present but the class is unavailable."""
        with patch.dict(sys.modules, {
            'gevent': types.ModuleType('gevent'),
            'gevent.exceptions': types.ModuleType('gevent.exceptions'),
        }):
            result = compat.get_gevent_concurrent_error()

        assert result is None

    def test_runtime_evaluation_reflects_later_gevent_import(self):
        """Calling the function after gevent is imported returns the class.

        If kombu.utils.compat was imported before gevent, a module-level
        variable would be stuck as None.  The lazy function always reflects
        the current state of sys.modules at call time.
        """
        mock_exc = type('ConcurrentObjectUseError', (AssertionError,), {})
        mock_exc_mod = types.ModuleType('gevent.exceptions')
        mock_exc_mod.ConcurrentObjectUseError = mock_exc

        # Simulate: kombu imported first → gevent not yet in sys.modules
        saved = {k: sys.modules.pop(k) for k in list(sys.modules) if 'gevent' in k}
        try:
            assert compat.get_gevent_concurrent_error() is None  # before gevent

            # Simulate: gevent.monkey.patch_all() called later
            sys.modules.update({
                'gevent': types.ModuleType('gevent'),
                'gevent.exceptions': mock_exc_mod,
            })
            assert compat.get_gevent_concurrent_error() is mock_exc  # after gevent
        finally:
            for k in ('gevent', 'gevent.exceptions'):
                sys.modules.pop(k, None)
            sys.modules.update(saved)
