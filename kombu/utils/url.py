"""URL Utilities."""
from functools import partial
from typing import Any, Dict, Mapping, NamedTuple
from .typing import Port
try:
    from urllib.parse import parse_qsl, quote, unquote, urlparse
except ImportError:
    from urllib import quote, unquote                  # noqa
    from urlparse import urlparse, parse_qsl    # noqa

safequote = partial(quote, safe='')


urlparts = NamedTuple('urlparts', [
    ('scheme', str),
    ('hostname', str),
    ('port', int),
    ('username', str),
    ('password', str),
    ('path', str),
    ('query', Dict),
])


def parse_url(url: str) -> Dict:
    """Parse URL into mapping of components."""
    scheme, host, port, user, password, path, query = _parse_url(url)
    return dict(transport=scheme, hostname=host,
                port=port, userid=user,
                password=password, virtual_host=path, **query)


def url_to_parts(url: str) -> urlparts:
    """Parse URL into :class:`urlparts` tuple of components."""
    scheme = urlparse(url).scheme
    schemeless = url[len(scheme) + 3:]
    # parse with HTTP URL semantics
    parts = urlparse('http://' + schemeless)
    path = parts.path or ''
    path = path[1:] if path and path[0] == '/' else path
    return urlparts(
        scheme,
        unquote(parts.hostname or '') or None,
        parts.port,
        unquote(parts.username or '') or None,
        unquote(parts.password or '') or None,
        unquote(path or '') or None,
        dict(parse_qsl(parts.query)),
    )
_parse_url = url_to_parts  # noqa


def as_url(scheme: str,
           host: str = None,
           port: Port = None,
           user: str = None,
           password: str = None,
           path: str = None,
           query: Mapping = None,
           sanitize: bool = False,
           mask: str = '**') -> str:
    """Generate URL from component parts."""
    parts = ['{0}://'.format(scheme)]
    if user or password:
        if user:
            parts.append(safequote(user))
        if password:
            if sanitize:
                parts.extend([':', mask] if mask else [':'])
            else:
                parts.extend([':', safequote(password)])
        parts.append('@')
    parts.append(safequote(host) if host else '')
    if port:
        parts.extend([':', port])
    parts.extend(['/', path])
    return ''.join(str(part) for part in parts if part)


def sanitize_url(url: str, mask: str = '**') -> str:
    """Return copy of URL with password removed."""
    return as_url(*_parse_url(url), sanitize=True, mask=mask)


def maybe_sanitize_url(url: Any, mask: str = '**') -> Any:
    """Sanitize url, or do nothing if url undefined."""
    if isinstance(url, str) and '://' in url:
        return sanitize_url(url, mask)
    return url
