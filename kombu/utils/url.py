from urllib import unquote
from urlparse import urlparse
try:
    from urlparse import parse_qsl
except ImportError:  # pragma: no cover
    from cgi import parse_qsl  # noqa

from . import kwdict


def parse_url(url):
    scheme = urlparse(url).scheme
    schemeless = url[len(scheme) + 3:]
    # parse with HTTP URL semantics
    parts = urlparse("http://" + schemeless)

    # The first pymongo.Connection() argument (host) can be
    # a mongodb connection URI. If this is the case, don't
    # use port but let pymongo get the port(s) from the URI instead.
    # This enables the use of replica sets and sharding.
    # See pymongo.Connection() for more info.
    hostname = schemeless if scheme == 'mongodb' else parts.hostname
    path = (parts.path or '').lstrip('/')
    return dict(transport=scheme,
                hostname=unquote(hostname or '') or None,
                port=int(parts.port) if parts.port else None,
                userid=unquote(parts.username or '') or None,
                password=unquote(parts.password or '') or None,
                virtual_host=unquote(path or '') or None,
                **kwdict(dict(parse_qsl(parts.query))))
