from __future__ import absolute_import, unicode_literals

import socket
import sys
import types

from case import Mock, mock, patch

from kombu.five import bytes_if_py2
from kombu.utils import compat
from kombu.utils.compat import entrypoints, maybe_fileno


def test_entrypoints():
    with patch(
        'kombu.utils.compat.importlib_metadata.entry_points', create=True
    ) as iterep:
        eps = [Mock(), Mock()]
        iterep.return_value = {'kombu.test': eps}

        assert list(entrypoints('kombu.test'))
        iterep.assert_called_with()
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

    @mock.module_exists('eventlet', 'eventlet.patcher')
    def test_detect_environment_eventlet(self):
        with patch('eventlet.patcher.is_monkey_patched', create=True) as m:
            assert sys.modules['eventlet']
            m.return_value = True
            env = compat._detect_environment()
            m.assert_called_with(socket)
            assert env == 'eventlet'

    @mock.module_exists('gevent')
    def test_detect_environment_gevent(self):
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
            sys.modules['eventlet'] = types.ModuleType(
                bytes_if_py2('eventlet'))
            sys.modules['eventlet.patcher'] = types.ModuleType(
                bytes_if_py2('patcher'))
            assert compat._detect_environment() == 'default'
        finally:
            sys.modules.pop('eventlet.patcher', None)
            sys.modules.pop('eventlet', None)
        compat._detect_environment()
        try:
            sys.modules['gevent'] = types.ModuleType(bytes_if_py2('gevent'))
            assert compat._detect_environment() == 'default'
        finally:
            sys.modules.pop('gevent', None)
        compat._detect_environment()
