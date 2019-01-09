from __future__ import absolute_import, unicode_literals

from kombu.asynchronous import get_event_loop

from .base import Request, Headers, Response

__all__ = ('Client', 'Headers', 'Response', 'Request')


def Client(hub=None, **kwargs):
    """Create new HTTP client."""
    from .curl import CurlClient
    return CurlClient(hub, **kwargs)


def get_client(hub=None, **kwargs):
    """Get or create HTTP client bound to the current event loop."""
    hub = hub or get_event_loop()
    assert hub is not None
    client = getattr(hub, '_current_http_client', None)
    if not client:
        client = hub._current_http_client = Client(hub, **kwargs)
    return client
