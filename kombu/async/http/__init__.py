from __future__ import absolute_import, unicode_literals

from kombu.async import get_event_loop, set_event_loop
from kombu.async.hub import Hub

from .base import Request, Headers, Response

__all__ = ['Client', 'Headers', 'Response', 'Request']


def Client(hub=None, **kwargs):
    """Create new HTTP client."""
    from .curl import CurlClient
    return CurlClient(hub, **kwargs)


def get_client(hub=None, **kwargs):
    """Get or create HTTP client bound to the current event loop."""
    hub = hub or get_event_loop()
    try:
        if not hub:
            hub = Hub()
            set_event_loop(hub)

        client = Client(hub, **kwargs)
        return client
    except AttributeError:
        
        # What should happen on error?
        # It appears to impact the message.ack() going
        # back to SQS for issues like:
        # https://github.com/celery/kombu/issues/746
        if not hub:
            hub = Hub()
            set_event_loop(hub)

        client = Client(hub, **kwargs)
        return client
