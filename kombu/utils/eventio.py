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

try:
    from eventlet.patcher import is_monkey_patched as is_eventlet
except ImportError:
    is_eventlet = lambda module: False  # noqa

__all__ = ["poll"]

POLL_READ = 0x001
POLL_ERR = 0x008 | 0x010 | 0x2000


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
        self._epoll = select.epoll()

    def register(self, fd, events):
        self._epoll.register(fd, events)

    def unregister(self, fd):
        try:
            self._epoll.unregister(fd)
        except socket.error:
            pass

    def _poll(self, timeout):
        return self._epoll.poll(timeout or -1)


class _kqueue(Poller):

    def __init__(self):
        self._kqueue = select.kqueue()
        self._active = {}

    def register(self, fd, events):
        self._control(fd, events, select.KQ_EV_ADD)
        self._active[fd] = events

    def unregister(self, fd):
        events = self._active.pop(fd)
        try:
            self._control(fd, events, select.KQ_EV_DELETE)
        except socket.error:
            pass

    def _control(self, fd, events, flags):
        self._kqueue.control([select.kevent(fd, filter=select.KQ_FILTER_READ,
                                                flags=flags)], 0)

    def _poll(self, timeout):
        kevents = self._kqueue.control(None, 1000, timeout)
        events = {}
        for kevent in kevents:
            fd = kevent.ident
            if kevent.filter == select.KQ_FILTER_READ:
                events[fd] = events.get(fd, 0) | POLL_READ
            if kevent.filter == select.KQ_EV_ERROR:
                events[fd] = events.get(fd, 0) | POLL_ERR
        return events.items()


class _select(Poller):

    def __init__(self):
        self._all = self._rfd, self._efd = set(), set()

    def register(self, fd, events):
        if events & POLL_ERR:
            self._efd.add(fd)
            self._rfd.add(fd)
        elif events & POLL_READ:
            self._rfd.add(fd)

    def unregister(self, fd):
        self._rfd.discard(fd)
        self._efd.discard(fd)

    def _poll(self, timeout):
        read, _write, error = select.select(self._rfd, [], self._efd, timeout)
        events = {}
        for fd in read:
            fd = fd.fileno()
            events[fd] = events.get(fd, 0) | POLL_READ
        for fd in error:
            fd = fd.fileno()
            events[fd] = events.get(fd, 0) | POLL_ERR
        return events.items()

if is_eventlet(select):
    # use Eventlet's non-blocking version of select.select
    poll = _select
elif hasattr(select, "epoll"):
    # Py2.6+ Linux
    poll = _epoll
elif hasattr(select, "kqueue"):
    # Py2.6+ on BSD / Darwin
    poll = _kqueue
else:
    poll = _select
