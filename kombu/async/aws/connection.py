# -*- coding: utf-8 -*-
"""Amazon AWS Connection."""
from __future__ import absolute_import, unicode_literals

from io import BytesIO

from vine import promise, transform

from botocore.awsrequest import AWSRequest

from kombu.async.http import Headers, Request, get_client
from kombu.five import items, python_2_unicode_compatible

# from .ext import (
#     XmlHandler, ResultSet,
# )

try:
    from urllib.parse import urlunsplit
except ImportError:
    from urlparse import urlunsplit  # noqa
from xml.sax import parseString as sax_parse  # noqa

try:  # pragma: no cover
    from email import message_from_file
    from email.mime.message import MIMEMessage
except ImportError:  # pragma: no cover
    from mimetools import Message as MIMEMessage   # noqa

    def message_from_file(m):  # noqa
        return m

__all__ = [
    'AsyncHTTPConnection', 'AsyncHTTPSConnection',
    'AsyncHTTPResponse', 'AsyncConnection',
]


@python_2_unicode_compatible
class AsyncHTTPResponse(object):
    """Async HTTP Response."""

    def __init__(self, response):
        self.response = response
        self._msg = None
        self.version = 10

    def read(self, *args, **kwargs):
        return self.response.body

    def getheader(self, name, default=None):
        return self.response.headers.get(name, default)

    def getheaders(self):
        return list(items(self.response.headers))

    @property
    def msg(self):
        if self._msg is None:
            self._msg = MIMEMessage(message_from_file(
                BytesIO(b'\r\n'.join(
                    b'{0}: {1}'.format(*h) for h in self.getheaders())
                )
            ))
        return self._msg

    @property
    def status(self):
        return self.response.code

    @property
    def reason(self):
        if self.response.error:
            return self.response.error.message
        return ''

    def __repr__(self):
        return repr(self.response)


@python_2_unicode_compatible
class AsyncHTTPSConnection(object):
    """Async HTTP Connection."""

    Request = Request
    Response = AsyncHTTPResponse

    method = 'GET'
    path = '/'
    body = None
    scheme = 'https'
    default_ports = {'http': 80, 'https': 443}

    def __init__(self, strict=None, timeout=20.0, http_client=None, **kwargs):
        self.headers = []
        self.timeout = timeout
        self.strict = strict
        self.http_client = http_client or get_client()

    def request(self, method, path, body=None, headers=None):
        self.path = path
        self.method = method
        if body is not None:
            try:
                read = body.read
            except AttributeError:
                self.body = body
            else:
                self.body = read()
        if headers is not None:
            self.headers.extend(list(items(headers)))

    def getrequest(self, scheme=None):
        headers = Headers(self.headers)
        return self.Request(self.path, method=self.method, headers=headers,
                            body=self.body, connect_timeout=self.timeout,
                            request_timeout=self.timeout, validate_cert=False)

    def getresponse(self, callback=None):
        request = self.getrequest()
        request.then(transform(self.Response, callback))
        return self.http_client.add_request(request)

    def set_debuglevel(self, level):
        pass

    def connect(self):
        pass

    def close(self):
        pass

    def putrequest(self, method, path, **kwargs):
        self.method = method
        self.path = path

    def putheader(self, header, value):
        self.headers.append((header, value))

    def endheaders(self):
        pass

    def send(self, data):
        if self.body:
            self.body += data
        else:
            self.body = data

    def __repr__(self):
        return '<AsyncHTTPConnection: {0!r}>'.format(self.getrequest())


class AsyncConnection(object):
    """Async AWS Connection."""

    def __init__(self, sqs_connection, http_client=None, **kwargs):
        self.sqs_connection = sqs_connection
        self._httpclient = http_client or get_client()

    def get_http_connection(self):
        return AsyncHTTPSConnection(http_client=self._httpclient)

    def _mexe(self, request, sender=None, callback=None):
        callback = callback or promise()
        print(
            'HTTP %s  headers=%s body=%s',
            request.url,
            request.headers, request.body,
        )

        conn = self.get_http_connection()

        if callable(sender):
            sender(conn, request.method, request.path, request.body,
                   request.headers, callback)
        else:
            conn.request(request.method, request.url,
                         request.body, request.headers)
            conn.getresponse(callback=callback)
        return callback


class AsyncAWSQueryConnection(AsyncConnection):
    """Async AWS Query Connection."""

    def __init__(self, sqs_connection, http_client=None, http_client_params={}, **kwargs):
        AsyncConnection.__init__(self, sqs_connection, http_client, **http_client_params)

    def make_request(self, action, params_, path, verb, callback=None):
        params = params_.copy()
        if action:
            params['Action'] = action
        signer = self.sqs_connection._request_signer
        request = AWSRequest(method=verb, url=path, params=params)
        signer.sign(action, request)
        return self._mexe(request.prepare(), callback=callback)

    def get_list(self, action, params, markers,
                 path='/', parent=None, verb='GET', callback=None):
        return self.make_request(
            action, params, path, verb,
            callback=transform( 
                self._on_list_ready, callback, parent or self, markers,
            ),
        )

    def get_object(self, action, params, cls,
                   path='/', parent=None, verb='GET', callback=None):
        return self.make_request(
            action, params, path, verb,
            callback=transform(
                self._on_obj_ready, callback, parent or self, cls,
            ),
        )

    def get_status(self, action, params,
                   path='/', parent=None, verb='GET', callback=None):
        return self.make_request(
            action, params, path, verb,
            callback=transform(
                self._on_status_ready, callback, parent or self,
            ),
        )

    def _on_list_ready(self, parent, markers, response):
        body = response.read()
        if response.status == 200 and body:
            rs = ResultSet(markers)
            h = XmlHandler(rs, parent)
            sax_parse(body, h)
            return rs
        else:
            raise self._for_status(response, body)

    def _on_obj_ready(self, parent, cls, response):
        body = response.read()
        if response.status == 200 and body:
            obj = cls(parent)
            h = XmlHandler(obj, parent)
            sax_parse(body, h)
            return obj
        else:
            raise self._for_status(response, body)

    def _on_status_ready(self, parent, response):
        body = response.read()
        if response.status == 200 and body:
            rs = ResultSet()
            h = XmlHandler(rs, parent)
            sax_parse(body, h)
            return rs.status
        else:
            raise self._for_status(response, body)

    def _for_status(self, response, body):
        return Exception("\n".join([str(response), str(body)]))
        # context = 'Empty body' if not body else 'HTTP Error'
        # exc = self.ResponseError(response.status, response.reason, body)
        # return exc
