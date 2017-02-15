# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import pytest

from contextlib import contextmanager

from case import Mock, patch
from vine.abstract import Thenable
import boto3

from kombu.exceptions import HttpError
from kombu.five import WhateverIO

from kombu.async import http
from kombu.async.aws.connection import (
    AsyncHTTPSConnection,
    AsyncHTTPResponse,
    AsyncConnection,
    AsyncAWSQueryConnection,
)

from .case import AWSCase

from t.mocks import PromiseMock

# Not currently working
VALIDATES_CERT = False


def passthrough(*args, **kwargs):
    m = Mock(*args, **kwargs)

    def side_effect(ret):
        return ret
    m.side_effect = side_effect
    return m


class test_AsyncHTTPSConnection(AWSCase):

    def test_AsyncHTTPSConnection(self):
        x = AsyncHTTPSConnection()
        assert x.scheme == 'https'

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
        assert x.scheme == 'https'

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

    def test_when_boto_missing(self, patching):
        patching('kombu.async.aws.connection.boto', None)
        with pytest.raises(ImportError):
            AsyncConnection(Mock(name='client'))

    def test_client(self):
        x = AsyncConnection()
        assert x._httpclient is http.get_client()
        client = Mock(name='client')
        y = AsyncConnection(http_client=client)
        assert y._httpclient is client

    def test_get_http_connection(self):
        x = AsyncConnection(client=Mock(name='client'))
        assert isinstance(
            x.get_http_connection(False),
            AsyncHTTPSConnection,
        )
        assert isinstance(
            x.get_http_connection(True),
            AsyncHTTPSConnection,
        )

        conn = x.get_http_connection(False)
        assert conn.http_client is x._httpclient
        assert conn.host == 'aws.vandelay.com'
        assert conn.port == 80


class test_AsyncAWSQueryConnection(AWSCase):

    def setup(self):
        session = boto3.session.Session(
            aws_access_key_id='AAA',
            aws_secret_access_key='AAAA',
        )
        sqs_client = session.client('sqs')
        self.x = AsyncAWSQueryConnection(sqs_client, http_client=Mock(name='client'))

    @patch('boto.log', create=True)
    def test_make_request(self, _):
        _mexe, self.x._mexe = self.x._mexe, Mock(name='_mexe')
        Conn = self.x.get_http_connection = Mock(name='get_http_connection')
        callback = PromiseMock(name='callback')
        self.x.make_request(
            'action', {'foo': 1}, '/', 'GET', callback=callback,
        )
        self.x._mexe.assert_called()
        request = self.x._mexe.call_args[0][0]
        assert request.params['Action'] == 'action'
        assert request.params['Version'] == self.x.APIVersion

        ret = _mexe(request, callback=callback)
        assert ret is callback
        Conn.return_value.request.assert_called()
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
        self.x._mexe.assert_called()
        request = self.x._mexe.call_args[0][0]
        assert 'Action' not in request.params
        assert request.params['Version'] == self.x.APIVersion

    @contextmanager
    def mock_sax_parse(self, parser):
        with patch('kombu.async.aws.connection.sax_parse') as sax_parse:
            with patch('kombu.async.aws.connection.XmlHandler') as xh:

                def effect(body, h):
                    return parser(xh.call_args[0][0], body, h)
                sax_parse.side_effect = effect
                yield (sax_parse, xh)
                sax_parse.assert_called()

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

    def test_get_list(self):
        with self.mock_make_request() as callback:
            self.x.get_list('action', {'p': 3.3}, ['m'], callback=callback)
            on_ready = self.assert_make_request_called()

            def parser(dest, body, h):
                dest.append('hi')
                dest.append('there')

            with self.mock_sax_parse(parser):
                on_ready(self.Response(200, 'hello'))
            callback.assert_called_with(['hi', 'there'])

    def test_get_list_error(self):
        with self.mock_make_request() as callback:
            self.x.get_list('action', {'p': 3.3}, ['m'], callback=callback)
            on_ready = self.assert_make_request_called()

            with pytest.raises(self.x.ResponseError):
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

            callback.assert_called()
            result = callback.call_args[0][0]
            assert result.value == 42
            assert result.parent

    def test_get_object_error(self):
        with self.mock_make_request() as callback:
            self.x.get_object('action', {'p': 3.3}, object, callback=callback)
            on_ready = self.assert_make_request_called()

            with pytest.raises(self.x.ResponseError):
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

            with pytest.raises(self.x.ResponseError):
                on_ready(self.Response(404, 'Not found'))

    def test_get_status_error_empty_body(self):
        with self.mock_make_request() as callback:
            self.x.get_status('action', {'p': 3.3}, callback=callback)
            on_ready = self.assert_make_request_called()

            with pytest.raises(self.x.ResponseError):
                on_ready(self.Response(200, ''))
