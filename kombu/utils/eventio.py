"""
kombu.utils.eventio
===================

Evented IO support for multiple platforms.

"""
from __future__ import absolute_import

import errno
import socket

from select import select as _selectf, error as _selecterr

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
        KQ_EV_ENABLE,
        KQ_EV_CLEAR,
        KQ_FILTER_WRITE,
        KQ_FILTER_READ,
        KQ_FILTER_VNODE,
        KQ_NOTE_WRITE,
        KQ_NOTE_EXTEND,
        KQ_NOTE_DELETE,
        KQ_NOTE_ATTRIB,
    )
except ImportError:
    kqueue = kevent = None                                      # noqa
    KQ_EV_ADD = KQ_EV_DELETE = KQ_EV_EOF = KQ_EV_ERROR = 0      # noqa
    KQ_EV_ENABLE = KQ_EV_CLEAR = KQ_EV_VNODE = 0                # noqa
    KQ_FILTER_WRITE = KQ_FILTER_READ = 0                        # noqa
    KQ_NOTE_WRITE = KQ_NOTE_EXTEND = 0                          # noqa
    KQ_NOTE_ATTRIB = KQ_NOTE_DELETE = 0                         # noqa

from kombu.syn import detect_environment

from .compat import get_errno

__all__ = ['poll']

READ = POLL_READ = 0x001
WRITE = POLL_WRITE = 0x004
ERR = POLL_ERR = 0x008 | 0x010

try:
    SELECT_BAD_FD = set((errno.EBADF, errno.WSAENOTSOCK))
except AttributeError:
    SELECT_BAD_FD = set((errno.EBADF,))


class Poller(object):

    def poll(self, timeout):
        try:
            return self._poll(timeout)
        except Exception as exc:
            if get_errno(exc) != errno.EINTR:
                raise


class _epoll(Poller):

    def __init__(self):
        self._epoll = epoll()

    def register(self, fd, events):
        try:
            self._epoll.register(fd, events)
        except Exception as exc:
            if get_errno(exc) != errno.EEXIST:
                raise

    def unregister(self, fd):
        try:
            self._epoll.unregister(fd)
        except (socket.error, ValueError, KeyError):
            pass
        except (IOError, OSError) as exc:
            if get_errno(exc) != errno.ENOENT:
                raise

    def _poll(self, timeout):
        return self._epoll.poll(timeout if timeout is not None else -1)

    def close(self):
        self._epoll.close()


class _kqueue(Poller):
    w_fflags = (KQ_NOTE_WRITE | KQ_NOTE_EXTEND |
                KQ_NOTE_ATTRIB | KQ_NOTE_DELETE)

    def __init__(self):
        self._kqueue = kqueue()
        self._active = {}
        self.on_file_change = None
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

    def watch_file(self, fd):
        ev = kevent(fd,
                    filter=KQ_FILTER_VNODE,
                    flags=KQ_EV_ADD | KQ_EV_ENABLE | KQ_EV_CLEAR,
                    fflags=self.w_fflags)
        self._kcontrol([ev], 0)

    def unwatch_file(self, fd):
        ev = kevent(fd,
                    filter=KQ_FILTER_VNODE,
                    flags=KQ_EV_DELETE,
                    fflags=self.w_fflags)
        self._kcontrol([ev], 0)

    def _control(self, fd, events, flags):
        if not events:
            return
        kevents = []
        if events & WRITE:
            kevents.append(kevent(fd,
                           filter=KQ_FILTER_WRITE,
                           flags=flags))
        if not kevents or events & READ:
            kevents.append(
                kevent(fd, filter=KQ_FILTER_READ, flags=flags),
            )
        control = self._kcontrol
        for e in kevents:
            try:
                control([e], 0)
            except ValueError:
                pass

    def _poll(self, timeout):
        kevents = self._kcontrol(None, 1000, timeout)
        events, file_changes = {}, []
        for k in kevents:
            fd = k.ident
            if k.filter == KQ_FILTER_READ:
                events[fd] = events.get(fd, 0) | READ
            elif k.filter == KQ_FILTER_WRITE:
                if k.flags & KQ_EV_EOF:
                    events[fd] = ERR
                else:
                    events[fd] = events.get(fd, 0) | WRITE
            elif k.filter == KQ_EV_ERROR:
                events[fd] = events.get(fd, 0) | ERR
            elif k.filter == KQ_FILTER_VNODE:
                if k.fflags & KQ_NOTE_DELETE:
                    self.unregister(fd)
                file_changes.append(k)
        if file_changes:
            self.on_file_change(file_changes)
        return list(events.items())

    def close(self):
        self._kqueue.close()


class _select(Poller):

    def __init__(self):
        self._all = (self._rfd,
                     self._wfd,
                     self._efd) = set(), set(), set()

    def register(self, fd, events):
        if events & ERR:
            self._efd.add(fd)
            self._rfd.add(fd)
        if events & WRITE:
            self._wfd.add(fd)
        if events & READ:
            self._rfd.add(fd)

    def _remove_bad(self):
        for fd in self._rfd | self._wfd | self._efd:
            try:
                _selectf([fd], [], [], 0)
            except _selecterr as exc:
                if get_errno(exc) in SELECT_BAD_FD:
                    self.unregister(fd)

    def unregister(self, fd):
        self._rfd.discard(fd)
        self._wfd.discard(fd)
        self._efd.discard(fd)

    def _poll(self, timeout):
        try:
            read, write, error = _selectf(
                self._rfd, self._wfd, self._efd, timeout,
            )
        except _selecterr as exc:
            if get_errno(exc) == errno.EINTR:
                return
            elif get_errno(exc) in SELECT_BAD_FD:
                self._remove_bad()

        events = {}
        for fd in read:
            if not isinstance(fd, int):
                fd = fd.fileno()
            events[fd] = events.get(fd, 0) | READ
        for fd in write:
            if not isinstance(fd, int):
                fd = fd.fileno()
            events[fd] = events.get(fd, 0) | WRITE
        for fd in error:
            if not isinstance(fd, int):
                fd = fd.fileno()
            events[fd] = events.get(fd, 0) | ERR
        return list(events.items())

    def close(self):
        pass


def _get_poller():
    if detect_environment() != 'default':
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
