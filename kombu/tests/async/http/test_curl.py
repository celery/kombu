# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from kombu.async.http.curl import READ, WRITE, CurlClient

from kombu.tests.case import (
    HubCase, Mock, call, patch, set_module_symbol, skip,
)


@skip.if_pypy()
@skip.unless_module('pycurl')
class test_CurlClient(HubCase):

    class Client(CurlClient):
        Curl = Mock(name='Curl')

    def test_when_pycurl_missing(self):
        with set_module_symbol('kombu.async.http.curl', 'pycurl', None):
            with self.assertRaises(ImportError):
                self.Client()

    def test_max_clients_set(self):
        x = self.Client(max_clients=303)
        self.assertEqual(x.max_clients, 303)

    def test_init(self):
        with patch('kombu.async.http.curl.pycurl') as _pycurl:
            x = self.Client()
            self.assertIsNotNone(x._multi)
            self.assertIsNotNone(x._pending)
            self.assertIsNotNone(x._free_list)
            self.assertIsNotNone(x._fds)
            self.assertEqual(
                x._socket_action, x._multi.socket_action,
            )
            self.assertEqual(len(x._curls), x.max_clients)
            self.assertTrue(x._timeout_check_tref)

            x._multi.setopt.assert_has_calls([
                call(_pycurl.M_TIMERFUNCTION, x._set_timeout),
                call(_pycurl.M_SOCKETFUNCTION, x._handle_socket),
            ])

    def test_close(self):
        with patch('kombu.async.http.curl.pycurl'):
            x = self.Client()
            x._timeout_check_tref = Mock(name='timeout_check_tref')
            x.close()
            x._timeout_check_tref.cancel.assert_called_with()
            for _curl in x._curls:
                _curl.close.assert_called_with()
            x._multi.close.assert_called_with()

    def test_add_request(self):
        with patch('kombu.async.http.curl.pycurl'):
            x = self.Client()
            x._process_queue = Mock(name='_process_queue')
            x._set_timeout = Mock(name='_set_timeout')
            request = Mock(name='request')
            x.add_request(request)
            self.assertIn(request, x._pending)
            x._process_queue.assert_called_with()
            x._set_timeout.assert_called_with(0)

    def test_handle_socket(self):
        with patch('kombu.async.http.curl.pycurl') as _pycurl:
            hub = Mock(name='hub')
            x = self.Client(hub)
            fd = Mock(name='fd1')

            # POLL_REMOVE
            x._fds[fd] = fd
            x._handle_socket(_pycurl.POLL_REMOVE, fd, x._multi, None, _pycurl)
            hub.remove.assert_called_with(fd)
            self.assertNotIn(fd, x._fds)
            x._handle_socket(_pycurl.POLL_REMOVE, fd, x._multi, None, _pycurl)

            # POLL_IN
            hub = x.hub = Mock(name='hub')
            fds = [fd, Mock(name='fd2'), Mock(name='fd3')]
            x._fds = {f: f for f in fds}
            x._handle_socket(_pycurl.POLL_IN, fd, x._multi, None, _pycurl)
            hub.remove.assert_has_calls([call(fd)])
            hub.add_reader.assert_called_with(fd, x.on_readable, fd)
            self.assertEqual(x._fds[fd], READ)

            # POLL_OUT
            hub = x.hub = Mock(name='hub')
            x._handle_socket(_pycurl.POLL_OUT, fd, x._multi, None, _pycurl)
            hub.add_writer.assert_called_with(fd, x.on_writable, fd)
            self.assertEqual(x._fds[fd], WRITE)

            # POLL_INOUT
            hub = x.hub = Mock(name='hub')
            x._handle_socket(_pycurl.POLL_INOUT, fd, x._multi, None, _pycurl)
            hub.add_reader.assert_called_with(fd, x.on_readable, fd)
            hub.add_writer.assert_called_with(fd, x.on_writable, fd)
            self.assertEqual(x._fds[fd], READ | WRITE)

            # UNKNOWN EVENT
            hub = x.hub = Mock(name='hub')
            x._handle_socket(0xff3f, fd, x._multi, None, _pycurl)

            # FD NOT IN FDS
            hub = x.hub = Mock(name='hub')
            x._fds.clear()
            x._handle_socket(0xff3f, fd, x._multi, None, _pycurl)
            hub.remove.assert_not_called()

    def test_set_timeout(self):
        x = self.Client()
        x._set_timeout(100)

    def test_timeout_check(self):
        with patch('kombu.async.http.curl.pycurl') as _pycurl:
            x = self.Client()
            x._process_pending_requests = Mock(name='process_pending')
            x._multi.socket_all.return_value = 333, 1
            _pycurl.error = KeyError
            x._timeout_check(_pycurl=_pycurl)

            x._multi.socket_all.return_value = None
            x._multi.socket_all.side_effect = _pycurl.error(333)
            x._timeout_check(_pycurl=_pycurl)

    def test_on_readable_on_writeable(self):
        with patch('kombu.async.http.curl.pycurl') as _pycurl:
            x = self.Client()
            x._on_event = Mock(name='on_event')
            fd = Mock(name='fd')
            x.on_readable(fd, _pycurl=_pycurl)
            x._on_event.assert_called_with(fd, _pycurl.CSELECT_IN)
            x.on_writable(fd, _pycurl=_pycurl)
            x._on_event.assert_called_with(fd, _pycurl.CSELECT_OUT)
