from __future__ import annotations

from typing import TYPE_CHECKING

from kombu.asynchronous import get_event_loop
from kombu.asynchronous.http.base import BaseClient, Headers, Request, Response
from kombu.asynchronous.hub import Hub

if TYPE_CHECKING:
    from kombu.asynchronous.http.base import BaseClient

__all__ = ('Client', 'Headers', 'Response', 'Request', 'get_client')


def Client(hub: Hub | None = None, **kwargs: int) -> BaseClient:
    """Create new HTTP client."""
    try:
        import pycurl

        from .curl import CurlClient
        return CurlClient(hub, **kwargs)
    except ImportError:
        pass

    # fall back scenario
    from .urllib3_client import Urllib3Client
    return Urllib3Client(hub, **kwargs)


def get_client(hub: Hub | None = None, **kwargs: int) -> BaseClient:
    """Get or create HTTP client bound to the current event loop."""
    hub = hub or get_event_loop()
    try:
        return hub._current_http_client
    except AttributeError:
        client = hub._current_http_client = Client(hub, **kwargs)
        return client
