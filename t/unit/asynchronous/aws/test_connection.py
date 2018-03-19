# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from contextlib import contextmanager

from case import Mock
from vine.abstract import Thenable

from kombu.exceptions import HttpError
from kombu.five import WhateverIO

from kombu.asynchronous import http
from kombu.asynchronous.aws.connection import (
    AsyncHTTPSConnection,
    AsyncHTTPResponse,
    AsyncConnection,
    AsyncAWSQueryConnection,
)
from kombu.asynchronous.aws.ext import boto3

from .case import AWSCase

from t.mocks import PromiseMock

try:
    from urllib.parse import urlparse, parse_qs
except ImportError:
    from urlparse import urlparse, parse_qs  # noqa

# Not currently working
VALIDATES_CERT = False


def passthrough(*args, **kwargs):
    m = Mock(*args, **kwargs)

    def side_effect(ret):
        return ret
    m.side_effect = side_effect
    return m


class test_AsyncHTTPSConnection(AWSCase):

    def test_http_client(self):
        x = AsyncHTTPSConnection()
        assert x.http_client is http.get_client()
        client = Mock(name='http_client')
        y = AsyncHTTPSConnection(http_client=client)
        assert y.http_client is client

    def test_args(self):
        x = AsyncHTTPSConnection(
            strict=True, timeout=33.3,
        )
        assert x.strict
        assert x.timeout == 33.3

    def test_request(self):
        x = AsyncHTTPSConnection('aws.vandelay.com')
        x.request('PUT', '/importer-exporter')
        assert x.path == '/importer-exporter'
        assert x.method == 'PUT'

    def test_request_with_body_buffer(self):
        x = AsyncHTTPSConnection('aws.vandelay.com')
        body = Mock(name='body')
        body.read.return_value = 'Vandelay Industries'
        x.request('PUT', '/importer-exporter', body)
        assert x.method == 'PUT'
        assert x.path == '/importer-exporter'
        assert x.body == 'Vandelay Industries'
        body.read.assert_called_with()

    def test_request_with_body_text(self):
        x = AsyncHTTPSConnection('aws.vandelay.com')
        x.request('PUT', '/importer-exporter', 'Vandelay Industries')
        assert x.method == 'PUT'
        assert x.path == '/importer-exporter'
        assert x.body == 'Vandelay Industries'

    def test_request_with_headers(self):
        x = AsyncHTTPSConnection()
        headers = {'Proxy': 'proxy.vandelay.com'}
        x.request('PUT', '/importer-exporter', None, headers)
        assert 'Proxy' in dict(x.headers)
        assert dict(x.headers)['Proxy'] == 'proxy.vandelay.com'

    def assert_request_created_with(self, url, conn):
        conn.Request.assert_called_with(
            url, method=conn.method,
            headers=http.Headers(conn.headers), body=conn.body,
            connect_timeout=conn.timeout, request_timeout=conn.timeout,
            validate_cert=VALIDATES_CERT,
        )

    def test_getresponse(self):
        client = Mock(name='client')
        client.add_request = passthrough(name='client.add_request')
        x = AsyncHTTPSConnection(http_client=client)
        x.Response = Mock(name='x.Response')
        request = x.getresponse()
        x.http_client.add_request.assert_called_with(request)
        assert isinstance(request, Thenable)
        assert isinstance(request.on_ready, Thenable)

        response = Mock(name='Response')
        request.on_ready(response)
        x.Response.assert_called_with(response)

    def test_getresponse__real_response(self):
        client = Mock(name='client')
        client.add_request = passthrough(name='client.add_request')
        callback = PromiseMock(name='callback')
        x = AsyncHTTPSConnection(http_client=client)
        request = x.getresponse(callback)
        x.http_client.add_request.assert_called_with(request)

        buf = WhateverIO()
        buf.write('The quick brown fox jumps')

        headers = http.Headers({'X-Foo': 'Hello', 'X-Bar': 'World'})

        response = http.Response(request, 200, headers, buf)
        request.on_ready(response)
        callback.assert_called()
        wresponse = callback.call_args[0][0]

        assert wresponse.read() == 'The quick brown fox jumps'
        assert wresponse.status == 200
        assert wresponse.getheader('X-Foo') == 'Hello'
        headers_dict = wresponse.getheaders()
        assert dict(headers_dict) == headers
        assert wresponse.msg
        assert repr(wresponse)

    def test_repr(self):
        assert repr(AsyncHTTPSConnection())

    def test_putrequest(self):
        x = AsyncHTTPSConnection()
        x.putrequest('UPLOAD', '/new')
        assert x.method == 'UPLOAD'
        assert x.path == '/new'

    def test_putheader(self):
        x = AsyncHTTPSConnection()
        x.putheader('X-Foo', 'bar')
        assert x.headers == [('X-Foo', 'bar')]
        x.putheader('X-Bar', 'baz')
        assert x.headers == [
            ('X-Foo', 'bar'),
            ('X-Bar', 'baz'),
        ]

    def test_send(self):
        x = AsyncHTTPSConnection()
        x.send('foo')
        assert x.body == 'foo'
        x.send('bar')
        assert x.body == 'foobar'

    def test_interface(self):
        x = AsyncHTTPSConnection()
        assert x.set_debuglevel(3) is None
        assert x.connect() is None
        assert x.close() is None
        assert x.endheaders() is None


class test_AsyncHTTPResponse(AWSCase):

    def test_with_error(self):
        r = Mock(name='response')
        r.error = HttpError(404, 'NotFound')
        x = AsyncHTTPResponse(r)
        assert x.reason == 'NotFound'

        r.error = None
        assert not x.reason


class test_AsyncConnection(AWSCase):

    def test_client(self):
        sqs = Mock(name='sqs')
        x = AsyncConnection(sqs)
        assert x._httpclient is http.get_client()
        client = Mock(name='client')
        y = AsyncConnection(sqs, http_client=client)
        assert y._httpclient is client

    def test_get_http_connection(self):
        sqs = Mock(name='sqs')
        x = AsyncConnection(sqs)
        assert isinstance(
            x.get_http_connection(),
            AsyncHTTPSConnection,
        )
        conn = x.get_http_connection()
        assert conn.http_client is x._httpclient


class test_AsyncAWSQueryConnection(AWSCase):

    def setup(self):
        session = boto3.session.Session(
            aws_access_key_id='AAA',
            aws_secret_access_key='AAAA',
            region_name='us-west-2',
        )
        sqs_client = session.client('sqs')
        self.x = AsyncAWSQueryConnection(sqs_client,
                                         http_client=Mock(name='client'))

    def test_make_request(self):
        _mexe, self.x._mexe = self.x._mexe, Mock(name='_mexe')
        Conn = self.x.get_http_connection = Mock(name='get_http_connection')
        callback = PromiseMock(name='callback')
        self.x.make_request(
            'action', {'foo': 1}, 'https://foo.com/', 'GET', callback=callback,
        )
        self.x._mexe.assert_called()
        request = self.x._mexe.call_args[0][0]
        parsed = urlparse(request.url)
        params = parse_qs(parsed.query)
        assert params['Action'][0] == 'action'

        ret = _mexe(request, callback=callback)
        assert ret is callback
        Conn.return_value.request.assert_called()
        Conn.return_value.getresponse.assert_called_with(
            callback=callback,
        )

    def test_make_request__no_action(self):
        self.x._mexe = Mock(name='_mexe')
        self.x.get_http_connection = Mock(name='get_http_connection')
        callback = PromiseMock(name='callback')
        self.x.make_request(
            None, {'foo': 1}, 'http://foo.com/', 'GET', callback=callback,
        )
        self.x._mexe.assert_called()
        request = self.x._mexe.call_args[0][0]
        parsed = urlparse(request.url)
        params = parse_qs(parsed.query)
        assert 'Action' not in params

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
        self.x.make_request.assert_called()
        return self.x.make_request.call_args[1]['callback']
