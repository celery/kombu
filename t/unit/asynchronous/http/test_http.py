from __future__ import annotations

from io import BytesIO
from unittest.mock import Mock, patch

import pytest
from vine import promise

import t.skip
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


@t.skip.if_pypy
class test_Client:

    def test_get_client(self, hub):
        pytest.importorskip('pycurl')
        client = http.get_client()
        assert client.hub is hub
        client2 = http.get_client(hub)
        assert client2 is client
        assert client2.hub is hub

    def test_client_uses_curl_when_available(self, hub):
        """Test that Client() returns CurlClient when pycurl is available."""
        mock_curl_client = Mock(name='CurlClient')
        mock_curl_client.Curl = Mock()  # Curl is available

        with patch('kombu.asynchronous.http.curl.CurlClient', mock_curl_client):
            client = http.Client(hub)
            assert client is mock_curl_client.return_value

    def test_client_falls_back_to_urllib3_when_curl_unavailable(self, hub):
        """Test that Client() falls back to Urllib3Client when pycurl is not available."""
        mock_curl_client = Mock(name='CurlClient')
        mock_curl_client.Curl = None  # Curl is NOT available

        mock_urllib3_client = Mock(name='Urllib3Client')
        mock_urllib3_client_instance = Mock()
        mock_urllib3_client.return_value = mock_urllib3_client_instance

        with patch('kombu.asynchronous.http.curl.CurlClient', mock_curl_client):
            with patch('kombu.asynchronous.http.urllib3_client.Urllib3Client', mock_urllib3_client):
                client = http.Client(hub)
                assert client is mock_urllib3_client_instance

    def test_get_client_creates_new_client_when_none_exists(self, hub):
        """Test get_client creates a new client when hub has no existing client."""
        # Remove any previously cached client
        if hasattr(hub, '_current_http_client'):
            del hub._current_http_client

        mock_client = Mock(name='client')

        with patch('kombu.asynchronous.http.Client', return_value=mock_client) as mock_client_fn:
            client = http.get_client(hub)
            mock_client_fn.assert_called_once_with(hub)
            assert client is mock_client
            assert hub._current_http_client is mock_client

    def test_get_client_returns_existing_client(self, hub):
        """Test get_client returns existing cached client from hub."""
        existing_client = Mock(name='existing_client')
        hub._current_http_client = existing_client

        with patch('kombu.asynchronous.http.Client') as mock_client_fn:
            client = http.get_client(hub)
            mock_client_fn.assert_not_called()
            assert client is existing_client
