"""
kombu.utils.eventio
===================

Evented IO support for multiple platforms.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import errno
import select
import socket

from select import select as _selectf

from kombu.syn import detect_environment

__all__ = ["poll"]

POLL_READ = 0x001
POLL_WRITE = 0x004
POLL_ERR = 0x008 | 0x010

try:
    from select import epoll
except ImportError:
    epoll = None  # noqa

try:
    from select import (
        kqueue,
        kevent,
        KQ_EV_ADD,
        KQ_EV_DELETE,
        KQ_EV_EOF,
        KQ_EV_ERROR,
        KQ_FILTER_WRITE,
        KQ_FILTER_READ,
    )
except ImportError:
    kqueue = kevent = KQ_EV_ADD = KQ_EV_DELETE = KQ_EV_EOF = \
            KQ_EV_ERROR = KQ_FILTER_WRITE = KQ_FILTER_READ = None  # noqa


def get_errno(exc):
    try:
        return exc.errno
    except AttributeError:
        try:
            # e.args = (errno, reason)
            if isinstance(exc.args, tuple) and len(exc.args) == 2:
                return exc.args[0]
        except AttributeError:
            pass
    return 0


class Poller(object):

    def poll(self, timeout):
        try:
            return self._poll(timeout)
        except Exception, exc:
            if get_errno(exc) != errno.EINTR:
                raise


class _epoll(Poller):

    def __init__(self):
        self._epoll = epoll()

    def register(self, fd, events):
        try:
            self._epoll.register(fd, events)
        except Exception, exc:
            if get_errno(exc) != errno.EEXIST:
                raise

    def unregister(self, fd):
        try:
            self._epoll.unregister(fd)
        except socket.error:
            pass

    def _poll(self, timeout):
        return self._epoll.poll(timeout or -1)

    def close(self):
        self._epoll.close()


class _kqueue(Poller):

    def __init__(self):
        self._kqueue = kqueue()
        self._active = {}
        self._kcontrol = self._kqueue.control

    def register(self, fd, events):
        self._control(fd, events, KQ_EV_ADD)
        self._active[fd] = events

    def unregister(self, fd):
        events = self._active.pop(fd, None)
        if events:
            try:
                self._control(fd, events, KQ_EV_DELETE)
            except socket.error:
                pass

    def _control(self, fd, events, flags):
        if not events:
            return
        kevents = []
        if events & POLL_WRITE:
            kevents.append(kevent(fd,
                          filter=KQ_FILTER_WRITE,
                          flags=flags))
        if not kevents or events & POLL_READ:
            kevents.append(kevent(fd,
                filter=KQ_FILTER_READ, flags=flags))
        control = self._kcontrol
        [control([e], 0) for e in kevents]

    def _poll(self, timeout):
        kevents = self._kcontrol(None, 1000, timeout)
        events = {}
        for kevent in kevents:
            fd = kevent.ident
            if kevent.filter == KQ_FILTER_READ:
                events[fd] = events.get(fd, 0) | POLL_READ
            if kevent.filter == KQ_FILTER_WRITE:
                if kevent.flags & KQ_EV_EOF:
                    events[fd] = POLL_ERR
                else:
                    events[fd] = events.get(fd, 0) | POLL_WRITE
            if kevent.filter == KQ_EV_ERROR:
                events[fd] = events.get(fd, 0) | POLL_ERR
        return events.items()

    def close(self):
        self._kqueue.close()


class _select(Poller):

    def __init__(self):
        self._all = (self._rfd,
                     self._wfd,
                     self._efd) = set(), set(), set()

    def register(self, fd, events):
        if events & POLL_ERR:
            self._efd.add(fd)
            self._rfd.add(fd)
        if events & POLL_WRITE:
            self._wfd.add(fd)
        if events & POLL_READ:
            self._rfd.add(fd)

    def unregister(self, fd):
        self._rfd.discard(fd)
        self._wfd.discard(fd)
        self._efd.discard(fd)

    def _poll(self, timeout):
        read, write, error = _selectf(self._rfd, self._wfd, self._efd, timeout)
        events = {}
        for fd in read:
            if not isinstance(fd, int):
                fd = fd.fileno()
            events[fd] = events.get(fd, 0) | POLL_READ
        for fd in write:
            if not isinstance(fd, int):
                fd = fd.fileno()
            events[fd] = events.get(fd, 0) | POLL_WRITE
        for fd in error:
            if not isinstance(fd, int):
                fd = fd.fileno()
            events[fd] = events.get(fd, 0) | POLL_ERR
        return events.items()

    def close(self):
        pass


def _get_poller():
    if detect_environment() in ("eventlet", "gevent"):
        # greenlet
        return _select
    elif epoll:
        # Py2.6+ Linux
        return _epoll
    elif kqueue:
        # Py2.6+ on BSD / Darwin
        return _kqueue
    else:
        return _select


def poll(*args, **kwargs):
    return _get_poller()(*args, **kwargs)
