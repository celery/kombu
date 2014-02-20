from __future__ import absolute_import

from io import BytesIO

from amqp import promise
from kombu.async import Hub
from kombu.async import http
from kombu.async.http.base import BaseClient, normalize_header, header_parser
from kombu.exceptions import HttpError

from kombu.tests.case import Case, Mock


class test_Headers(Case):

    def test_normalize(self):
        self.assertEqual(normalize_header('accept-encoding'),
                         'Accept-Encoding')


class test_Request(Case):

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
        callback = Mock()
        x = http.Request('http://foo')
        x.then(callback)

        x.on_ready(1)
        callback.assert_called_with(1)


class test_Response(Case):

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


class test_BaseClient(Case):

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


class test_Client(Case):

    def test_get_request(self):
        hub = Hub()
        callback = Mock(name='callback')

        def on_ready(response):
            pass #print('{0.effective_url} -> {0.code}'.format(response))
        requests = []
        for i in range(1000):
            requests.extend([
                http.Request(
                    'http://localhost:8000/README.rst',
                    on_ready=promise(on_ready, callback=callback),
                ),
                http.Request(
                    'http://localhost:8000/AUTHORS',
                    on_ready=promise(on_ready, callback=callback),
                ),
                http.Request(
                    'http://localhost:8000/pavement.py',
                    on_ready=promise(on_ready, callback=callback),
                ),
                http.Request(
                    'http://localhost:8000/setup.py',
                    on_ready=promise(on_ready, callback=callback),
                ),
                http.Request(
                    'http://localhost:8000/setup.py%s' % (i, ),
                    on_ready=promise(on_ready, callback=callback),
                ),
            ])
        client = http.Client(hub)
        for request in requests:
            client.perform(request)

        from kombu.five import monotonic
        start_time = monotonic()
        print('START PERFORM')
        while callback.call_count < len(requests):
            hub.run_once()
        print('-END PERFORM: %r' % (monotonic() - start_time))
