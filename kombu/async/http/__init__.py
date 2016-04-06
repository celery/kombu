from __future__ import absolute_import, unicode_literals

from kombu.async import get_event_loop

from .base import Request, Headers, Response

__all__ = ['Client', 'Headers', 'Response', 'Request']


def Client(hub=None, **kwargs):
    from .curl import CurlClient
    return CurlClient(hub, **kwargs)


def get_client(hub=None, **kwargs):
    hub = hub or get_event_loop()
    try:
        return hub._current_http_client
    except AttributeError:
        client = hub._current_http_client = Client(hub, **kwargs)
        return client
