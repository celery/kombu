from __future__ import absolute_import

from .base import Request, Headers, Response

__all__ = ['Client', 'Headers', 'Response', 'Request']


def Client(hub, **kwargs):
    from .curl import CurlClient
    return CurlClient(hub, **kwargs)
