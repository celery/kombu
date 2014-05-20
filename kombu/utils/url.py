from __future__ import absolute_import

try:
    from urllib.parse import unquote, urlparse, parse_qsl
except ImportError:
    from urllib import unquote                  # noqa
    from urlparse import urlparse, parse_qsl    # noqa

from . import kwdict


def _parse_url(url):
    scheme = urlparse(url).scheme
    schemeless = url[len(scheme) + 3:]
    # parse with HTTP URL semantics
    parts = urlparse('http://' + schemeless)
    path = parts.path or ''
    path = path[1:] if path and path[0] == '/' else path
    return (scheme, unquote(parts.hostname or '') or None, parts.port,
            unquote(parts.username or '') or None,
            unquote(parts.password or '') or None,
            unquote(path or '') or None,
            kwdict(dict(parse_qsl(parts.query))))


def parse_url(url):
    scheme, host, port, user, password, path, query = _parse_url(url)
    return dict(transport=scheme, hostname=host,
                port=port, userid=user,
                password=password, virtual_host=path, **query)
