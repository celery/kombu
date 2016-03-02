from __future__ import absolute_import

from io import BytesIO

from vine import promise

from kombu.async import http
from kombu.async.http.base import BaseClient, normalize_header
from kombu.exceptions import HttpError

from kombu.tests.case import (
    HubCase, Mock, PromiseMock, case_no_pypy, case_requires,
)


class test_Headers(HubCase):

    def test_normalize(self):
        self.assertEqual(normalize_header('accept-encoding'),
                         'Accept-Encoding')


class test_Request(HubCase):

    def test_init(self):
        x = http.Request('http://foo', method='POST')
        self.assertEqual(x.url, 'http://foo')
        self.assertEqual(x.method, 'POST')

        x = http.Request('x', max_redirects=100)
        self.assertEqual(x.max_redirects, 100)

        self.assertIsInstance(x.headers, http.Headers)
        h = http.Headers()
        x = http.Request('x', headers=h)
        self.assertIs(x.headers, h)
        self.assertIsInstance(x.on_ready, promise)

    def test_then(self):
        callback = PromiseMock(name='callback')
        x = http.Request('http://foo')
        x.then(callback)

        x.on_ready(1)
        callback.assert_called_with(1)


class test_Response(HubCase):

    def test_init(self):
        req = http.Request('http://foo')
        r = http.Response(req, 200)

        self.assertEqual(r.status, 'OK')
        self.assertEqual(r.effective_url, 'http://foo')
        r.raise_for_error()

    def test_raise_for_error(self):
        req = http.Request('http://foo')
        r = http.Response(req, 404)
        self.assertEqual(r.status, 'Not Found')
        self.assertTrue(r.error)

        with self.assertRaises(HttpError):
            r.raise_for_error()

    def test_get_body(self):
        req = http.Request('http://foo')
        req.buffer = BytesIO()
        req.buffer.write(b'hello')

        rn = http.Response(req, 200, buffer=None)
        self.assertIsNone(rn.body)

        r = http.Response(req, 200, buffer=req.buffer)
        self.assertIsNone(r._body)
        self.assertEqual(r.body, b'hello')
        self.assertEqual(r._body, b'hello')
        self.assertEqual(r.body, b'hello')


class test_BaseClient(HubCase):

    def test_init(self):
        c = BaseClient(Mock(name='hub'))
        self.assertTrue(c.hub)
        self.assertTrue(c._header_parser)

    def test_perform(self):
        c = BaseClient(Mock(name='hub'))
        c.add_request = Mock(name='add_request')

        c.perform('http://foo')
        self.assertTrue(c.add_request.called)
        self.assertIsInstance(c.add_request.call_args[0][0], http.Request)

        req = http.Request('http://bar')
        c.perform(req)
        c.add_request.assert_called_with(req)

    def test_add_request(self):
        c = BaseClient(Mock(name='hub'))
        with self.assertRaises(NotImplementedError):
            c.add_request(Mock(name='request'))

    def test_header_parser(self):
        c = BaseClient(Mock(name='hub'))
        parser = c._header_parser
        headers = http.Headers()

        c.on_header(headers, 'HTTP/1.1')
        c.on_header(headers, 'x-foo-bar: 123')
        c.on_header(headers, 'People: George Costanza')
        self.assertEqual(headers._prev_key, 'People')
        c.on_header(headers, ' Jerry Seinfeld')
        c.on_header(headers, ' Elaine Benes')
        c.on_header(headers, ' Cosmo Kramer')
        self.assertFalse(headers.complete)
        c.on_header(headers, '')
        self.assertTrue(headers.complete)

        with self.assertRaises(KeyError):
            parser.throw(KeyError('foo'))
        c.on_header(headers, '')

        self.assertEqual(headers['X-Foo-Bar'], '123')
        self.assertEqual(
            headers['People'],
            'George Costanza Jerry Seinfeld Elaine Benes Cosmo Kramer',
        )

    def test_close(self):
        BaseClient(Mock(name='hub')).close()

    def test_as_context(self):
        c = BaseClient(Mock(name='hub'))
        c.close = Mock(name='close')
        with c:
            pass
        c.close.assert_called_with()


@case_no_pypy
@case_requires('pycurl')
class test_Client(HubCase):

    def test_get_client(self):
        client = http.get_client()
        self.assertIs(client.hub, self.hub)
        client2 = http.get_client(self.hub)
        self.assertIs(client2, client)
        self.assertIs(client2.hub, self.hub)
