# -*- coding: utf-8 -*-
from __future__ import absolute_import

from contextlib import contextmanager

from vine.abstract import Thenable

from kombu.exceptions import HttpError
from kombu.five import WhateverIO

from kombu.async import http
from kombu.async.aws.connection import (
    AsyncHTTPConnection,
    AsyncHTTPSConnection,
    AsyncHTTPResponse,
    AsyncConnection,
    AsyncAWSAuthConnection,
    AsyncAWSQueryConnection,
)

from kombu.tests.case import PromiseMock, Mock, patch, set_module_symbol

from .case import AWSCase

# Not currently working
VALIDATES_CERT = False


def passthrough(*args, **kwargs):
    m = Mock(*args, **kwargs)

    def side_effect(ret):
        return ret
    m.side_effect = side_effect
    return m


class test_AsyncHTTPConnection(AWSCase):

    def test_AsyncHTTPSConnection(self):
        x = AsyncHTTPSConnection('aws.vandelay.com')
        self.assertEqual(x.scheme, 'https')

    def test_http_client(self):
        x = AsyncHTTPConnection('aws.vandelay.com')
        self.assertIs(x.http_client, http.get_client())
        client = Mock(name='http_client')
        y = AsyncHTTPConnection('aws.vandelay.com', http_client=client)
        self.assertIs(y.http_client, client)

    def test_args(self):
        x = AsyncHTTPConnection(
            'aws.vandelay.com', 8083, strict=True, timeout=33.3,
        )
        self.assertEqual(x.host, 'aws.vandelay.com')
        self.assertEqual(x.port, 8083)
        self.assertTrue(x.strict)
        self.assertEqual(x.timeout, 33.3)
        self.assertEqual(x.scheme, 'http')

    def test_request(self):
        x = AsyncHTTPConnection('aws.vandelay.com')
        x.request('PUT', '/importer-exporter')
        self.assertEqual(x.path, '/importer-exporter')
        self.assertEqual(x.method, 'PUT')

    def test_request_with_body_buffer(self):
        x = AsyncHTTPConnection('aws.vandelay.com')
        body = Mock(name='body')
        body.read.return_value = 'Vandelay Industries'
        x.request('PUT', '/importer-exporter', body)
        self.assertEqual(x.method, 'PUT')
        self.assertEqual(x.path, '/importer-exporter')
        self.assertEqual(x.body, 'Vandelay Industries')
        body.read.assert_called_with()

    def test_request_with_body_text(self):
        x = AsyncHTTPConnection('aws.vandelay.com')
        x.request('PUT', '/importer-exporter', 'Vandelay Industries')
        self.assertEqual(x.method, 'PUT')
        self.assertEqual(x.path, '/importer-exporter')
        self.assertEqual(x.body, 'Vandelay Industries')

    def test_request_with_headers(self):
        x = AsyncHTTPConnection('aws.vandelay.com')
        headers = {'Proxy': 'proxy.vandelay.com'}
        x.request('PUT', '/importer-exporter', None, headers)
        self.assertIn('Proxy', dict(x.headers))
        self.assertEqual(dict(x.headers)['Proxy'], 'proxy.vandelay.com')

    def assertRequestCreatedWith(self, url, conn):
        conn.Request.assert_called_with(
            url, method=conn.method,
            headers=http.Headers(conn.headers), body=conn.body,
            connect_timeout=conn.timeout, request_timeout=conn.timeout,
            validate_cert=VALIDATES_CERT,
        )

    def test_getrequest_AsyncHTTPSConnection(self):
        x = AsyncHTTPSConnection('aws.vandelay.com')
        x.Request = Mock(name='Request')
        x.getrequest()
        self.assertRequestCreatedWith('https://aws.vandelay.com/', x)

    def test_getrequest_nondefault_port(self):
        x = AsyncHTTPConnection('aws.vandelay.com', port=8080)
        x.Request = Mock(name='Request')
        x.getrequest()
        self.assertRequestCreatedWith('http://aws.vandelay.com:8080/', x)

        y = AsyncHTTPSConnection('aws.vandelay.com', port=8443)
        y.Request = Mock(name='Request')
        y.getrequest()
        self.assertRequestCreatedWith('https://aws.vandelay.com:8443/', y)

    def test_getresponse(self):
        client = Mock(name='client')
        client.add_request = passthrough(name='client.add_request')
        x = AsyncHTTPConnection('aws.vandelay.com', http_client=client)
        x.Response = Mock(name='x.Response')
        request = x.getresponse()
        x.http_client.add_request.assert_called_with(request)
        self.assertIsInstance(request, Thenable)
        self.assertIsInstance(request.on_ready, Thenable)

        response = Mock(name='Response')
        request.on_ready(response)
        x.Response.assert_called_with(response)

    def test_getresponse__real_response(self):
        client = Mock(name='client')
        client.add_request = passthrough(name='client.add_request')
        callback = PromiseMock(name='callback')
        x = AsyncHTTPConnection('aws.vandelay.com', http_client=client)
        request = x.getresponse(callback)
        x.http_client.add_request.assert_called_with(request)

        buf = WhateverIO()
        buf.write('The quick brown fox jumps')

        headers = http.Headers({'X-Foo': 'Hello', 'X-Bar': 'World'})

        response = http.Response(request, 200, headers, buf)
        request.on_ready(response)
        self.assertTrue(callback.called)
        wresponse = callback.call_args[0][0]

        self.assertEqual(wresponse.read(), 'The quick brown fox jumps')
        self.assertEqual(wresponse.status, 200)
        self.assertEqual(wresponse.getheader('X-Foo'), 'Hello')
        self.assertDictEqual(dict(wresponse.getheaders()), headers)
        self.assertTrue(wresponse.msg)
        self.assertTrue(wresponse.msg)
        self.assertTrue(repr(wresponse))

    def test_repr(self):
        self.assertTrue(repr(AsyncHTTPConnection('aws.vandelay.com')))

    def test_putrequest(self):
        x = AsyncHTTPConnection('aws.vandelay.com')
        x.putrequest('UPLOAD', '/new')
        self.assertEqual(x.method, 'UPLOAD')
        self.assertEqual(x.path, '/new')

    def test_putheader(self):
        x = AsyncHTTPConnection('aws.vandelay.com')
        x.putheader('X-Foo', 'bar')
        self.assertListEqual(x.headers, [('X-Foo', 'bar')])
        x.putheader('X-Bar', 'baz')
        self.assertListEqual(x.headers, [
            ('X-Foo', 'bar'),
            ('X-Bar', 'baz'),
        ])

    def test_send(self):
        x = AsyncHTTPConnection('aws.vandelay.com')
        x.send('foo')
        self.assertEqual(x.body, 'foo')
        x.send('bar')
        self.assertEqual(x.body, 'foobar')

    def test_interface(self):
        x = AsyncHTTPConnection('aws.vandelay.com')
        self.assertIsNone(x.set_debuglevel(3))
        self.assertIsNone(x.connect())
        self.assertIsNone(x.close())
        self.assertIsNone(x.endheaders())


class test_AsyncHTTPResponse(AWSCase):

    def test_with_error(self):
        r = Mock(name='response')
        r.error = HttpError(404, 'NotFound')
        x = AsyncHTTPResponse(r)
        self.assertEqual(x.reason, 'NotFound')

        r.error = None
        self.assertFalse(x.reason)


class test_AsyncConnection(AWSCase):

    def test_when_boto_missing(self):
        with set_module_symbol('kombu.async.aws.connection', 'boto', None):
            with self.assertRaises(ImportError):
                AsyncConnection(Mock(name='client'))

    def test_client(self):
        x = AsyncConnection()
        self.assertIs(x._httpclient, http.get_client())
        client = Mock(name='client')
        y = AsyncConnection(http_client=client)
        self.assertIs(y._httpclient, client)

    def test_get_http_connection(self):
        x = AsyncConnection(client=Mock(name='client'))
        self.assertIsInstance(
            x.get_http_connection('aws.vandelay.com', 80, False),
            AsyncHTTPConnection,
        )
        self.assertIsInstance(
            x.get_http_connection('aws.vandelay.com', 443, True),
            AsyncHTTPSConnection,
        )

        conn = x.get_http_connection('aws.vandelay.com', 80, False)
        self.assertIs(conn.http_client, x._httpclient)
        self.assertEqual(conn.host, 'aws.vandelay.com')
        self.assertEqual(conn.port, 80)


class test_AsyncAWSAuthConnection(AWSCase):

    @patch('boto.log', create=True)
    def test_make_request(self, _):
        x = AsyncAWSAuthConnection('aws.vandelay.com',
                                   http_client=Mock(name='client'))
        Conn = x.get_http_connection = Mock(name='get_http_connection')
        callback = PromiseMock(name='callback')
        ret = x.make_request('GET', '/foo', callback=callback)
        self.assertIs(ret, callback)
        self.assertTrue(Conn.return_value.request.called)
        Conn.return_value.getresponse.assert_called_with(
            callback=callback,
        )

    @patch('boto.log', create=True)
    def test_mexe(self, _):
        x = AsyncAWSAuthConnection('aws.vandelay.com',
                                   http_client=Mock(name='client'))
        Conn = x.get_http_connection = Mock(name='get_http_connection')
        request = x.build_base_http_request('GET', 'foo', '/auth')
        callback = PromiseMock(name='callback')
        x._mexe(request, callback=callback)
        Conn.return_value.request.assert_called_with(
            request.method, request.path, request.body, request.headers,
        )
        Conn.return_value.getresponse.assert_called_with(
            callback=callback,
        )

        no_callback_ret = x._mexe(request)
        self.assertIsInstance(
            no_callback_ret, Thenable, '_mexe always returns promise',
        )

    @patch('boto.log', create=True)
    def test_mexe__with_sender(self, _):
        x = AsyncAWSAuthConnection('aws.vandelay.com',
                                   http_client=Mock(name='client'))
        Conn = x.get_http_connection = Mock(name='get_http_connection')
        request = x.build_base_http_request('GET', 'foo', '/auth')
        sender = Mock(name='sender')
        callback = PromiseMock(name='callback')
        x._mexe(request, sender=sender, callback=callback)
        sender.assert_called_with(
            Conn.return_value, request.method, request.path,
            request.body, request.headers, callback,
        )


class test_AsyncAWSQueryConnection(AWSCase):

    def setup(self):
        self.x = AsyncAWSQueryConnection('aws.vandelay.com',
                                         http_client=Mock(name='client'))

    @patch('boto.log', create=True)
    def test_make_request(self, _):
        _mexe, self.x._mexe = self.x._mexe, Mock(name='_mexe')
        Conn = self.x.get_http_connection = Mock(name='get_http_connection')
        callback = PromiseMock(name='callback')
        self.x.make_request(
            'action', {'foo': 1}, '/', 'GET', callback=callback,
        )
        self.assertTrue(self.x._mexe.called)
        request = self.x._mexe.call_args[0][0]
        self.assertEqual(request.params['Action'], 'action')
        self.assertEqual(request.params['Version'], self.x.APIVersion)

        ret = _mexe(request, callback=callback)
        self.assertIs(ret, callback)
        self.assertTrue(Conn.return_value.request.called)
        Conn.return_value.getresponse.assert_called_with(
            callback=callback,
        )

    @patch('boto.log', create=True)
    def test_make_request__no_action(self, _):
        self.x._mexe = Mock(name='_mexe')
        self.x.get_http_connection = Mock(name='get_http_connection')
        callback = PromiseMock(name='callback')
        self.x.make_request(
            None, {'foo': 1}, '/', 'GET', callback=callback,
        )
        self.assertTrue(self.x._mexe.called)
        request = self.x._mexe.call_args[0][0]
        self.assertNotIn('Action', request.params)
        self.assertEqual(request.params['Version'], self.x.APIVersion)

    @contextmanager
    def mock_sax_parse(self, parser):
        with patch('kombu.async.aws.connection.sax_parse') as sax_parse:
            with patch('kombu.async.aws.connection.XmlHandler') as xh:

                def effect(body, h):
                    return parser(xh.call_args[0][0], body, h)
                sax_parse.side_effect = effect
                yield (sax_parse, xh)
                self.assertTrue(sax_parse.called)

    def Response(self, status, body):
        r = Mock(name='response')
        r.status = status
        r.read.return_value = body
        return r

    @contextmanager
    def mock_make_request(self):
        self.x.make_request = Mock(name='make_request')
        callback = PromiseMock(name='callback')
        yield callback

    def assert_make_request_called(self):
        self.assertTrue(self.x.make_request.called)
        return self.x.make_request.call_args[1]['callback']

    def test_get_list(self):
        with self.mock_make_request() as callback:
            self.x.get_list('action', {'p': 3.3}, ['m'], callback=callback)
            on_ready = self.assert_make_request_called()

            def parser(dest, body, h):
                dest.append('hi')
                dest.append('there')

            with self.mock_sax_parse(parser):
                on_ready(self.Response(200, 'hello'))
            self.assertTrue(callback.called_with(['hi', 'there']))

    def test_get_list_error(self):
        with self.mock_make_request() as callback:
            self.x.get_list('action', {'p': 3.3}, ['m'], callback=callback)
            on_ready = self.assert_make_request_called()

            with self.assertRaises(self.x.ResponseError):
                on_ready(self.Response(404, 'Not found'))

    def test_get_object(self):
        with self.mock_make_request() as callback:

            class Result(object):
                parent = None
                value = None

                def __init__(self, parent):
                    self.parent = parent

            self.x.get_object('action', {'p': 3.3}, Result, callback=callback)
            on_ready = self.assert_make_request_called()

            def parser(dest, body, h):
                dest.value = 42

            with self.mock_sax_parse(parser):
                on_ready(self.Response(200, 'hello'))

            self.assertTrue(callback.called)
            result = callback.call_args[0][0]
            self.assertEqual(result.value, 42)
            self.assertTrue(result.parent)

    def test_get_object_error(self):
        with self.mock_make_request() as callback:
            self.x.get_object('action', {'p': 3.3}, object, callback=callback)
            on_ready = self.assert_make_request_called()

            with self.assertRaises(self.x.ResponseError):
                on_ready(self.Response(404, 'Not found'))

    def test_get_status(self):
        with self.mock_make_request() as callback:
            self.x.get_status('action', {'p': 3.3}, callback=callback)
            on_ready = self.assert_make_request_called()
            set_status_to = [True]

            def parser(dest, body, b):
                dest.status = set_status_to[0]

            with self.mock_sax_parse(parser):
                on_ready(self.Response(200, 'hello'))
            callback.assert_called_with(True)

            set_status_to[0] = False
            with self.mock_sax_parse(parser):
                on_ready(self.Response(200, 'hello'))
            callback.assert_called_with(False)

    def test_get_status_error(self):
        with self.mock_make_request() as callback:
            self.x.get_status('action', {'p': 3.3}, callback=callback)
            on_ready = self.assert_make_request_called()

            with self.assertRaises(self.x.ResponseError):
                on_ready(self.Response(404, 'Not found'))

    def test_get_status_error_empty_body(self):
        with self.mock_make_request() as callback:
            self.x.get_status('action', {'p': 3.3}, callback=callback)
            on_ready = self.assert_make_request_called()

            with self.assertRaises(self.x.ResponseError):
                on_ready(self.Response(200, ''))
