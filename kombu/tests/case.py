from __future__ import absolute_import, unicode_literals

import importlib

from contextlib import contextmanager

from case import (
    ANY, Case, ContextMock, MagicMock, Mock,
    call, mock, skip, patch, sentinel,
)

__all__ = [
    'ANY', 'Case', 'ContextMock', 'MagicMock', 'Mock',
    'call', 'mock', 'skip', 'patch', 'sentinel',

    'HubCase', 'PromiseMock', 'MockPool', 'set_module_symbol',
]


class HubCase(Case):

    def setUp(self):
        from kombu.async import Hub, get_event_loop, set_event_loop
        self._prev_hub = get_event_loop()
        self.hub = Hub()
        set_event_loop(self.hub)
        super(HubCase, self).setUp()

    def tearDown(self):
        try:
            super(HubCase, self).tearDown()
        finally:
            from kombu.async import set_event_loop
            if self._prev_hub is not None:
                set_event_loop(self._prev_hub)


def PromiseMock(*args, **kwargs):
    m = Mock(*args, **kwargs)

    def on_throw(exc=None, *args, **kwargs):
        if exc:
            raise exc
        raise
    m.throw.side_effect = on_throw
    m.set_error_state.side_effect = on_throw
    m.throw1.side_effect = on_throw
    return m


class MockPool(object):

    def __init__(self, value=None):
        self.value = value or ContextMock()

    def acquire(self, **kwargs):
        return self.value


@contextmanager
def set_module_symbol(module, key, value):
    module = importlib.import_module(module)
    prev = getattr(module, key)
    setattr(module, key, value)
    try:
        yield
    finally:
        setattr(module, key, prev)
