from __future__ import absolute_import, unicode_literals

import socket
import sys
import types

from kombu import syn
from kombu.five import bytes_if_py2

from kombu.tests.case import Case, mock, patch


class test_syn(Case):

    def test_compat(self):
        self.assertEqual(syn.blocking(lambda: 10), 10)
        syn.select_blocking_method('foo')

    def test_detect_environment(self):
        try:
            syn._environment = None
            X = syn.detect_environment()
            self.assertEqual(syn._environment, X)
            Y = syn.detect_environment()
            self.assertEqual(Y, X)
        finally:
            syn._environment = None

    @mock.module_exists('eventlet', 'eventlet.patcher')
    def test_detect_environment_eventlet(self):
        with patch('eventlet.patcher.is_monkey_patched', create=True) as m:
            self.assertTrue(sys.modules['eventlet'])
            m.return_value = True
            env = syn._detect_environment()
            m.assert_called_with(socket)
            self.assertEqual(env, 'eventlet')

    @mock.module_exists('gevent')
    def test_detect_environment_gevent(self):
        with patch('gevent.socket', create=True) as m:
            prev, socket.socket = socket.socket, m.socket
            try:
                self.assertTrue(sys.modules['gevent'])
                env = syn._detect_environment()
                self.assertEqual(env, 'gevent')
            finally:
                socket.socket = prev

    def test_detect_environment_no_eventlet_or_gevent(self):
        try:
            sys.modules['eventlet'] = types.ModuleType(
                bytes_if_py2('eventlet'))
            sys.modules['eventlet.patcher'] = types.ModuleType(
                bytes_if_py2('patcher'))
            self.assertEqual(syn._detect_environment(), 'default')
        finally:
            sys.modules.pop('eventlet.patcher', None)
            sys.modules.pop('eventlet', None)
        syn._detect_environment()
        try:
            sys.modules['gevent'] = types.ModuleType(bytes_if_py2('gevent'))
            self.assertEqual(syn._detect_environment(), 'default')
        finally:
            sys.modules.pop('gevent', None)
        syn._detect_environment()
