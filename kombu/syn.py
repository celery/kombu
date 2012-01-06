"""
kombu.syn
=========

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys

__all__ = ["detect_environment"]


def blocking(fun, *args, **kwargs):
    return fun(*args, **kwargs)


def select_blocking_method(type):
    pass


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
