# -*- coding: utf-8 -*-
"""Amazon AWS Connection."""
from __future__ import absolute_import, unicode_literals

from io import BytesIO

from vine import promise, transform

from kombu.async.http import Headers, Request, get_client
from kombu.five import items, python_2_unicode_compatible

from .ext import (
    boto, AWSAuthConnection, AWSQueryConnection, XmlHandler, ResultSet,
)

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
    'AsyncAWSAuthConnection', 'AsyncAWSQueryConnection',
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
class AsyncHTTPConnection(object):
    """Async HTTP Connection."""

    Request = Request
    Response = AsyncHTTPResponse

    method = 'GET'
    path = '/'
    body = None
    scheme = 'http'
    default_ports = {'http': 80, 'https': 443}

    def __init__(self, host, port=None,
                 strict=None, timeout=20.0, http_client=None, **kwargs):
        self.host = host
        self.port = port
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
        scheme = scheme if scheme else self.scheme
        host = self.host
        if self.port and self.port != self.default_ports[scheme]:
            host = '{0}:{1}'.format(host, self.port)
        url = urlunsplit((scheme, host, self.path, '', ''))
        headers = Headers(self.headers)
        return self.Request(url, method=self.method, headers=headers,
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


class AsyncHTTPSConnection(AsyncHTTPConnection):
    """Async HTTPS Connection."""

    scheme = 'https'


class AsyncConnection(object):
    """Async AWS Connection."""

    def __init__(self, http_client=None, **kwargs):
        if boto is None:
            raise ImportError('boto is not installed')
        self._httpclient = http_client or get_client()

    def get_http_connection(self, host, port, is_secure):
        return (AsyncHTTPSConnection if is_secure else AsyncHTTPConnection)(
            host, port, http_client=self._httpclient,
        )

    def _mexe(self, request, sender=None, callback=None):
        callback = callback or promise()
        boto.log.debug(
            'HTTP %s/%s headers=%s body=%s',
            request.host, request.path,
            request.headers, request.body,
        )

        conn = self.get_http_connection(
            request.host, request.port, self.is_secure,
        )
        request.authorize(connection=self)

        if callable(sender):
            sender(conn, request.method, request.path, request.body,
                   request.headers, callback)
        else:
            conn.request(request.method, request.path,
                         request.body, request.headers)
            conn.getresponse(callback=callback)
        return callback


class AsyncAWSAuthConnection(AsyncConnection, AWSAuthConnection):
    """Async AWS Authn Connection."""

    def __init__(self, host,
                 http_client=None, http_client_params={}, **kwargs):
        AsyncConnection.__init__(self, http_client, **http_client_params)
        AWSAuthConnection.__init__(self, host, **kwargs)

    def make_request(self, method, path, headers=None, data='', host=None,
                     auth_path=None, sender=None, callback=None, **kwargs):
        req = self.build_base_http_request(
            method, path, auth_path, {}, headers, data, host,
        )
        return self._mexe(req, sender=sender, callback=callback)


class AsyncAWSQueryConnection(AsyncConnection, AWSQueryConnection):
    """Async AWS Query Connection."""

    def __init__(self, host,
                 http_client=None, http_client_params={}, **kwargs):
        AsyncConnection.__init__(self, http_client, **http_client_params)
        AWSAuthConnection.__init__(self, host, **kwargs)

    def make_request(self, action, params, path, verb, callback=None):
        request = self.build_base_http_request(
            verb, path, None, params, {}, '', self.server_name())
        if action:
            request.params['Action'] = action
        request.params['Version'] = self.APIVersion
        return self._mexe(request, callback=callback)

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
        context = 'Empty body' if not body else 'HTTP Error'
        exc = self.ResponseError(response.status, response.reason, body)
        boto.log.error('{0}: %r'.format(context), exc)
        return exc
