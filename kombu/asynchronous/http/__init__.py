from typing import TYPE_CHECKING, Optional

from kombu.asynchronous import get_event_loop
from kombu.asynchronous.http.base import Headers, Request, Response
from kombu.asynchronous.hub import Hub

if TYPE_CHECKING:
    from kombu.asynchronous.http.curl import CurlClient

__all__ = ('Client', 'Headers', 'Response', 'Request')


def Client(hub: Optional[Hub] = None, **kwargs: int) -> "CurlClient":
    """Create new HTTP client."""
    from .curl import CurlClient
    return CurlClient(hub, **kwargs)


def get_client(hub: Optional[Hub] = None, **kwargs: int) -> "CurlClient":
    """Get or create HTTP client bound to the current event loop."""
    hub = hub or get_event_loop()
    try:
        return hub._current_http_client
    except AttributeError:
        client = hub._current_http_client = Client(hub, **kwargs)
        return client
