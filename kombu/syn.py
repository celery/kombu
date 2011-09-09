"""
kombu.syn
=========

Thread synchronization.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys

#: current blocking method
__sync_current = None


def blocking(fun, *args, **kwargs):
    """Make sure function is called by blocking and waiting for the result,
    even if we're currently in a monkey patched eventlet/gevent
    environment."""
    if __sync_current is None:
        select_blocking_method(detect_environment())
    return __sync_current(fun, *args, **kwargs)


def select_blocking_method(type):
    """Select blocking method, where `type` is one of default
    gevent or eventlet."""
    global __sync_current
    __sync_current = {"eventlet": _sync_eventlet,
                      "gevent": _sync_gevent,
                      "default": _sync_default}[type]()


def _sync_default():
    """Create blocking primitive."""

    def __blocking__(fun, *args, **kwargs):
        return fun(*args, **kwargs)

    return __blocking__


def _sync_eventlet():
    """Create Eventlet blocking primitive."""
    from eventlet import spawn

    def __eblocking__(fun, *args, **kwargs):
        return spawn(fun, *args, **kwargs).wait()

    return __eblocking__


def _sync_gevent():
    """Create gevent blocking primitive."""
    from gevent import Greenlet

    def __gblocking__(fun, *args, **kwargs):
        return Greenlet.spawn(fun, *args, **kwargs).get()

    return __gblocking__


def detect_environment():
    ## -eventlet-
    if "eventlet" in sys.modules:
        try:
            from eventlet.patcher import is_monkey_patched as is_eventlet
            import socket

            if is_eventlet(socket):
                return "eventlet"
        except ImportError:
            pass

    # -gevent-
    if "gevent" in sys.modules:
        try:
            from gevent import socket as _gsocket
            import socket

            if socket.socket is _gsocket.socket:
                return "gevent"
        except ImportError:
            pass

    return "default"
