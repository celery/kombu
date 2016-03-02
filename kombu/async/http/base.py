from __future__ import absolute_import

import sys

from vine import Thenable, promise, maybe_promise

from kombu.exceptions import HttpError
from kombu.five import items
from kombu.utils import coro
from kombu.utils.encoding import bytes_to_str
from kombu.utils.functional import maybe_list, memoize

try:  # pragma: no cover
    from http.client import responses
except ImportError:
    from httplib import responses  # noqa

__all__ = ['Headers', 'Response', 'Request']

PYPY = hasattr(sys, 'pypy_version_info')


@memoize(maxsize=1000)
def normalize_header(key):
    return '-'.join(p.capitalize() for p in key.split('-'))


class Headers(dict):
    # TODO: This is just a regular dict and will not perform normalization
    # when looking up keys etc.

    #: Set when all of the headers have been read.
    complete = False

    #: Internal attribute used to keep track of continuation lines.
    _prev_key = None


class Request(object):
    """A HTTP Request.

    :param url: The URL to request.
    :param method: The HTTP method to use (defaults to ``GET``).
    :keyword headers: Optional headers for this request
        (:class:`dict` or :class:`~kombu.async.http.Headers`).
    :keyword body: Optional body for this request.
    :keyword connect_timeout: Connection timeout in float seconds
        (default 30.0).
    :keyword timeout: Time in float seconds before the request times out
        (default 30.0).
    :keyword follow_redirects: Specify if the client should follow redirects
        (enabled by default).
    :keyword max_redirects: Maximum number of redirects (default 6).
    :keyword use_gzip: Allow the server to use gzip compression (enabled by
       default).
    :keyword validate_cert: Set to true if the server certificate should be
        verified when performing ``https://`` requests (enabled by default).
    :keyword auth_username: Username for HTTP authentication.
    :keyword auth_password: Password for HTTP authentication.
    :keyword auth_mode: Type of HTTP authentication (``basic`` or ``digest``).
    :keyword user_agent: Custom user agent for this request.
    :keyword network_interace: Network interface to use for this request.
    :keyword on_ready: Callback to be called when the response has been
        received. Must accept single ``response`` argument.
    :kewyord on_stream: Optional callback to be called every time body content
        has been read from the socket.  If specified then the response body
        and buffer attributes will not be available.
    :keyword on_timeout: Optional callback to be called if the request
        times out.
    :keyword on_header: Optional callback to be called for every header line
        received from the server.  The signature is ``(headers, line)``
        and note that if you want ``response.headers`` to be populated
        then your callback needs to also call
        ``client.on_header(headers, line)``.
    :keyword on_prepare: Optional callback that is implementation specific
        (e.g. curl client will pass the ``curl`` instance to this callback).
    :keyword proxy_host: Optional proxy host.  Note that a ``proxy_port`` must
        also be provided or a :exc:`ValueError` will be raised.
    :keyword proxy_username: Optional username to use when logging in
        to the proxy.
    :keyword proxy_password: Optional password to use when authenticating
        with the proxy server.
    :keyword ca_certs: Custom CA certificates file to use.
    :keyword client_key: Optional filename for client SSL key.
    :keyword client_cert: Optional filename for client SSL certificate.

    """

    body = user_agent = network_interface = \
        auth_username = auth_password = auth_mode = \
        proxy_host = proxy_port = proxy_username = proxy_password = \
        ca_certs = client_key = client_cert = None

    connect_timeout = 30.0
    request_timeout = 30.0
    follow_redirects = True
    max_redirects = 6
    use_gzip = True
    validate_cert = True

    if not PYPY:  # pragma: no cover
        __slots__ = ('url', 'method', 'on_ready', 'on_timeout', 'on_stream',
                     'on_prepare', 'on_header', 'headers',
                     '__weakref__', '__dict__')

    def __init__(self, url, method='GET', on_ready=None, on_timeout=None,
                 on_stream=None, on_prepare=None, on_header=None,
                 headers=None, **kwargs):
        self.url = url
        self.method = method or self.method
        self.on_ready = maybe_promise(on_ready) or promise()
        self.on_timeout = maybe_promise(on_timeout)
        self.on_stream = maybe_promise(on_stream)
        self.on_prepare = maybe_promise(on_prepare)
        self.on_header = maybe_promise(on_header)
        if kwargs:
            for k, v in items(kwargs):
                setattr(self, k, v)
        if not isinstance(headers, Headers):
            headers = Headers(headers or {})
        self.headers = headers

    def then(self, callback, errback=None):
        self.on_ready.then(callback, errback)

    def __repr__(self):
        return '<Request: {0.method} {0.url} {0.body}>'.format(self)
Thenable.register(Request)


class Response(object):
    """HTTP Response.

    :param request: See :attr:`request`.
    :keyword code: See :attr:`code`.
    :keyword headers: See :attr:`headers`.
    :keyword buffer: See :attr:`buffer`
    :keyword effective_url: See :attr:`effective_url`.
    :keyword status: See :attr:`status`.

    .. attribute:: request

        :class:`Request` object used to get this response.

    .. attribute:: code

        HTTP response code (e.g. 200, 404, or 500).

    .. attribute:: headers

        HTTP headers for this response (:class:`Headers`).

    .. attribute:: buffer

        Socket read buffer.

    .. attribute:: effective_url

        The destination url for this request after following redirects.

    .. attribute:: error

        Error instance if the request resulted in a HTTP error code.

    .. attribute:: status

        Human equivalent of :attr:`code`, e.g. ``OK``, `Not found`, or
        'Internal Server Error'.

    """

    if not PYPY:  # pragma: no cover
        __slots__ = ('request', 'code', 'headers', 'buffer', 'effective_url',
                     'error', 'status', '_body', '__weakref__')

    def __init__(self, request, code, headers=None, buffer=None,
                 effective_url=None, error=None, status=None):
        self.request = request
        self.code = code
        self.headers = headers if headers is not None else Headers()
        self.buffer = buffer
        self.effective_url = effective_url or request.url
        self._body = None

        self.status = status or responses.get(self.code, 'Unknown')
        self.error = error
        if self.error is None and (self.code < 200 or self.code > 299):
            self.error = HttpError(self.code, self.status, self)

    def raise_for_error(self):
        """Raise :class:`~kombu.exceptions.HttpError` if the request resulted
        in a HTTP error code."""
        if self.error:
            raise self.error

    @property
    def body(self):
        """The full contents of the response body.

        Note that accessing this propery will evaluate the buffer
        and subsequent accesses will be cached.

        """
        if self._body is None:
            if self.buffer is not None:
                self._body = self.buffer.getvalue()
        return self._body


@coro
def header_parser(keyt=normalize_header):
    while 1:
        (line, headers) = yield
        if line.startswith('HTTP/'):
            continue
        elif not line:
            headers.complete = True
            continue
        elif line[0].isspace():
            pkey = headers._prev_key
            headers[pkey] = ' '.join([headers.get(pkey) or '', line.lstrip()])
        else:
            key, value = line.split(':', 1)
            key = headers._prev_key = keyt(key)
            headers[key] = value.strip()


class BaseClient(object):
    Headers = Headers
    Request = Request
    Response = Response

    def __init__(self, hub, **kwargs):
        self.hub = hub
        self._header_parser = header_parser()

    def perform(self, request, **kwargs):
        for req in maybe_list(request):
            if not isinstance(req, self.Request):
                req = self.Request(req, **kwargs)
            self.add_request(req)

    def add_request(self, request):
        raise NotImplementedError('must implement add_request')

    def close(self):
        pass

    def on_header(self, headers, line):
        try:
            self._header_parser.send((bytes_to_str(line), headers))
        except StopIteration:
            self._header_parser = header_parser()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
