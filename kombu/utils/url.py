from functools import partial
from typing import Any, Dict, Mapping, NamedTuple, Optional

from .typing import Port

try:
    from urllib.parse import parse_qsl, quote, unquote, urlparse
except ImportError:
    from urllib import quote, unquote                  # noqa
    from urlparse import urlparse, parse_qsl    # noqa


safequote = partial(quote, safe='')

urlparts_t = NamedTuple('urlparts_t', [
    ('scheme', str),
    ('hostname', str),
    ('port', int),
    ('username', str),
    ('password', str),
    ('path', str),
    ('query', Dict),
])


def _parse_url(url: str) -> urlparts_t:
    scheme = urlparse(url).scheme
    schemeless = url[len(scheme) + 3:]
    # parse with HTTP URL semantics
    parts = urlparse('http://' + schemeless)
    path = parts.path or ''
    path = path[1:] if path and path[0] == '/' else path
    return urlparts_t(
        scheme,
        unquote(parts.hostname or '') or None,
        parts.port,
        unquote(parts.username or '') or None,
        unquote(parts.password or '') or None,
        unquote(path or '') or None,
        dict(parse_qsl(parts.query)),
    )


def parse_url(url: str) -> Mapping:
    scheme, host, port, user, password, path, query = _parse_url(url)
    return dict(transport=scheme, hostname=host,
                port=port, userid=user,
                password=password, virtual_host=path, **query)


def as_url(scheme: str,
           host: Optional[str] = None,
           port: Optional[Port] = None,
           user: Optional[str] = None,
           password: Optional[str] = None,
           path: Optional[str] = None,
           query: Optional[Mapping] = None,
           sanitize: bool = False,
           mask: str='**') -> str:
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
    scheme, host, port, user, password, path, query = _parse_url(url)
    return as_url(scheme, host, port, user, password, path, query,
                  sanitize=True, mask=mask)


def maybe_sanitize_url(url: Any, mask: str = '**') -> Optional[str]:
    if isinstance(url, str) and '://' in url:
        return sanitize_url(url, mask)
    return url
