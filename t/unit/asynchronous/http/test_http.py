import pytest

from io import BytesIO

from vine import promise

from case import Mock, skip

from kombu.asynchronous import http
from kombu.asynchronous.http.base import BaseClient, normalize_header
from kombu.exceptions import HttpError

from t.mocks import PromiseMock


class test_Headers:

    def test_normalize(self):
        assert normalize_header('accept-encoding') == 'Accept-Encoding'


@pytest.mark.usefixtures('hub')
class test_Request:

    def test_init(self):
        x = http.Request('http://foo', method='POST')
        assert x.url == 'http://foo'
        assert x.method == 'POST'

        x = http.Request('x', max_redirects=100)
        assert x.max_redirects == 100

        assert isinstance(x.headers, http.Headers)
        h = http.Headers()
        x = http.Request('x', headers=h)
        assert x.headers is h
        assert isinstance(x.on_ready, promise)

    def test_then(self):
        callback = PromiseMock(name='callback')
        x = http.Request('http://foo')
        x.then(callback)

        x.on_ready(1)
        callback.assert_called_with(1)


@pytest.mark.usefixtures('hub')
class test_Response:

    def test_init(self):
        req = http.Request('http://foo')
        r = http.Response(req, 200)

        assert r.status == 'OK'
        assert r.effective_url == 'http://foo'
        r.raise_for_error()

    def test_raise_for_error(self):
        req = http.Request('http://foo')
        r = http.Response(req, 404)
        assert r.status == 'Not Found'
        assert r.error

        with pytest.raises(HttpError):
            r.raise_for_error()

    def test_get_body(self):
        req = http.Request('http://foo')
        req.buffer = BytesIO()
        req.buffer.write(b'hello')

        rn = http.Response(req, 200, buffer=None)
        assert rn.body is None

        r = http.Response(req, 200, buffer=req.buffer)
        assert r._body is None
        assert r.body == b'hello'
        assert r._body == b'hello'
        assert r.body == b'hello'


class test_BaseClient:

    @pytest.fixture(autouse=True)
    def setup_hub(self, hub):
        self.hub = hub

    def test_init(self):
        c = BaseClient(Mock(name='hub'))
        assert c.hub
        assert c._header_parser

    def test_perform(self):
        c = BaseClient(Mock(name='hub'))
        c.add_request = Mock(name='add_request')

        c.perform('http://foo')
        c.add_request.assert_called()
        assert isinstance(c.add_request.call_args[0][0], http.Request)

        req = http.Request('http://bar')
        c.perform(req)
        c.add_request.assert_called_with(req)

    def test_add_request(self):
        c = BaseClient(Mock(name='hub'))
        with pytest.raises(NotImplementedError):
            c.add_request(Mock(name='request'))

    def test_header_parser(self):
        c = BaseClient(Mock(name='hub'))
        parser = c._header_parser
        headers = http.Headers()

        c.on_header(headers, 'HTTP/1.1')
        c.on_header(headers, 'x-foo-bar: 123')
        c.on_header(headers, 'People: George Costanza')
        assert headers._prev_key == 'People'
        c.on_header(headers, ' Jerry Seinfeld')
        c.on_header(headers, ' Elaine Benes')
        c.on_header(headers, ' Cosmo Kramer')
        assert not headers.complete
        c.on_header(headers, '')
        assert headers.complete

        with pytest.raises(KeyError):
            parser.throw(KeyError('foo'))
        c.on_header(headers, '')

        assert headers['X-Foo-Bar'] == '123'
        assert (headers['People'] ==
                'George Costanza Jerry Seinfeld Elaine Benes Cosmo Kramer')

    def test_close(self):
        BaseClient(Mock(name='hub')).close()

    def test_as_context(self):
        c = BaseClient(Mock(name='hub'))
        c.close = Mock(name='close')
        with c:
            pass
        c.close.assert_called_with()


@skip.if_pypy()
@skip.unless_module('pycurl')
class test_Client:

    def test_get_client(self, hub):
        client = http.get_client()
        assert client.hub is hub
        client2 = http.get_client(hub)
        assert client2 is client
        assert client2.hub is hub
