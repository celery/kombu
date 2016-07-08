"""
kombu.utils.eventio
===================

Evented IO support for multiple platforms.

"""

import errno
import math
import select as __select__
import socket
import sys

from numbers import Integral
from typing import Any, Callable, Optional, Sequence, IO, cast
from typing import Set, Tuple  # noqa

from kombu.syn import detect_environment

from . import fileno
from .typing import Fd, Timeout

__all__ = ['poll']

_selectf = __select__.select
_selecterr = __select__.error
xpoll = getattr(__select__, 'poll', None)
epoll = getattr(__select__, 'epoll', None)
kqueue = getattr(__select__, 'kqueue', None)
kevent = getattr(__select__, 'kevent', None)
KQ_EV_ADD = getattr(__select__, 'KQ_EV_ADD', 1)
KQ_EV_DELETE = getattr(__select__, 'KQ_EV_DELETE', 2)
KQ_EV_ENABLE = getattr(__select__, 'KQ_EV_ENABLE', 4)
KQ_EV_CLEAR = getattr(__select__, 'KQ_EV_CLEAR', 32)
KQ_EV_ERROR = getattr(__select__, 'KQ_EV_ERROR', 16384)
KQ_EV_EOF = getattr(__select__, 'KQ_EV_EOF', 32768)
KQ_FILTER_READ = getattr(__select__, 'KQ_FILTER_READ', -1)
KQ_FILTER_WRITE = getattr(__select__, 'KQ_FILTER_WRITE', -2)
KQ_FILTER_AIO = getattr(__select__, 'KQ_FILTER_AIO', -3)
KQ_FILTER_VNODE = getattr(__select__, 'KQ_FILTER_VNODE', -4)
KQ_FILTER_PROC = getattr(__select__, 'KQ_FILTER_PROC', -5)
KQ_FILTER_SIGNAL = getattr(__select__, 'KQ_FILTER_SIGNAL', -6)
KQ_FILTER_TIMER = getattr(__select__, 'KQ_FILTER_TIMER', -7)
KQ_NOTE_LOWAT = getattr(__select__, 'KQ_NOTE_LOWAT', 1)
KQ_NOTE_DELETE = getattr(__select__, 'KQ_NOTE_DELETE', 1)
KQ_NOTE_WRITE = getattr(__select__, 'KQ_NOTE_WRITE', 2)
KQ_NOTE_EXTEND = getattr(__select__, 'KQ_NOTE_EXTEND', 4)
KQ_NOTE_ATTRIB = getattr(__select__, 'KQ_NOTE_ATTRIB', 8)
KQ_NOTE_LINK = getattr(__select__, 'KQ_NOTE_LINK', 16)
KQ_NOTE_RENAME = getattr(__select__, 'KQ_NOTE_RENAME', 32)
KQ_NOTE_REVOKE = getattr(__select__, 'KQ_NOTE_REVOKE', 64)
POLLIN = getattr(__select__, 'POLLIN', 1)
POLLOUT = getattr(__select__, 'POLLOUT', 4)
POLLERR = getattr(__select__, 'POLLERR', 8)
POLLHUP = getattr(__select__, 'POLLHUP', 16)
POLLNVAL = getattr(__select__, 'POLLNVAL', 32)

READ = POLL_READ = 0x001
WRITE = POLL_WRITE = 0x004
ERR = POLL_ERR = 0x008 | 0x010

try:
    SELECT_BAD_FD = {errno.EBADF, errno.WSAENOTSOCK}
except AttributeError:
    SELECT_BAD_FD = {errno.EBADF}


class BasePoller:
    ...


class _epoll(BasePoller):

    def __init__(self):
        self._epoll = epoll()

    def register(self, fd: Fd, events: int) -> Fd:
        try:
            self._epoll.register(fd, events)
        except Exception as exc:
            if getattr(exc, 'errno', None) != errno.EEXIST:
                raise
        return fd

    def unregister(self, fd: Fd) -> None:
        try:
            self._epoll.unregister(fd)
        except (socket.error, ValueError, KeyError, TypeError):
            pass
        except (PermissionError, FileNotFoundError):
            pass

    def poll(self, timeout: Timeout) -> Optional[Sequence]:
        try:
            return self._epoll.poll(timeout if timeout is not None else -1)
        except Exception as exc:
            if getattr(exc, 'errno', None) != errno.EINTR:
                raise

    def close(self) -> None:
        self._epoll.close()


class _kqueue(BasePoller):
    w_fflags = (KQ_NOTE_WRITE | KQ_NOTE_EXTEND |
                KQ_NOTE_ATTRIB | KQ_NOTE_DELETE)

    def __init__(self):
        self._kqueue = kqueue()
        self._active = {}
        self.on_file_change = None
        self._kcontrol = self._kqueue.control

    def register(self, fd: Fd, events: int) -> Fd:
        self._control(fd, events, KQ_EV_ADD)
        self._active[fd] = events
        return fd

    def unregister(self, fd: Fd) -> None:
        events = self._active.pop(fd, None)
        if events:
            try:
                self._control(fd, events, KQ_EV_DELETE)
            except socket.error:
                pass

    def watch_file(self, fd: Fd) -> None:
        ev = kevent(fd,
                    filter=KQ_FILTER_VNODE,
                    flags=KQ_EV_ADD | KQ_EV_ENABLE | KQ_EV_CLEAR,
                    fflags=self.w_fflags)
        self._kcontrol([ev], 0)

    def unwatch_file(self, fd: Fd) -> None:
        ev = kevent(fd,
                    filter=KQ_FILTER_VNODE,
                    flags=KQ_EV_DELETE,
                    fflags=self.w_fflags)
        self._kcontrol([ev], 0)

    def _control(self, fd: Fd, events: int, flags: int) -> None:
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

    def poll(self, timeout: Timeout) -> Sequence:
        try:
            kevents = self._kcontrol(None, 1000, timeout)
        except Exception as exc:
            if getattr(exc, 'errno', None) == errno.EINTR:
                return []
            raise
        events = {}        # type: Dict
        file_changes = []  # type: List
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

    def close(self) -> None:
        self._kqueue.close()


class _poll(BasePoller):

    def __init__(self) -> None:
        self._poller = xpoll()
        self._quick_poll = self._poller.poll
        self._quick_register = self._poller.register
        self._quick_unregister = self._poller.unregister

    def register(self, fd: Fd, events: int) -> Fd:
        fd = fileno(fd)
        poll_flags = 0
        if events & ERR:
            poll_flags |= POLLERR
        if events & WRITE:
            poll_flags |= POLLOUT
        if events & READ:
            poll_flags |= POLLIN
        self._quick_register(fd, poll_flags)
        return fd

    def unregister(self, fd: Fd) -> None:
        try:
            fd = fileno(fd)
        except socket.error as exc:
            # we don't know the previous fd of this object
            # but it will be removed by the next poll iteration.
            if getattr(exc, 'errno', None) in SELECT_BAD_FD:
                return fd
            raise
        self._quick_unregister(fd)
        return fd

    def poll(self, timeout: Timeout,
             round: Callable=math.ceil,
             POLLIN: int=POLLIN, POLLOUT: int=POLLOUT, POLLERR: int=POLLERR,
             READ: int=READ, WRITE: int=WRITE, ERR: int=ERR,
             Integral: Any=Integral) -> Sequence:
        timeout = 0 if timeout and timeout < 0 else round((timeout or 0) * 1e3)
        try:
            event_list = self._quick_poll(timeout)
        except (_selecterr, socket.error) as exc:
            if getattr(exc, 'errno', None) == errno.EINTR:
                return []
            raise

        ready = []
        for fd, event in event_list:
            events = 0
            if event & POLLIN:
                events |= READ
            if event & POLLOUT:
                events |= WRITE
            if event & POLLERR or event & POLLNVAL or event & POLLHUP:
                events |= ERR
            assert events
            if not isinstance(fd, Integral):
                fd = fd.fileno()
            ready.append((fd, events))
        return ready

    def close(self) -> None:
        self._poller = None


class _select(BasePoller):

    def __init__(self) -> None:
        self._rfd = set()  # type: Set[Fd]
        self._wfd = set()  # type: Set[Fd]
        self._efd = set()  # type: Set[Fd]
        # type: Tuple[Set[Fd], Set[Fd], Set[Fd]]
        self._all = (self._rfd, self._wfd, self._efd)

    def register(self, fd: Fd, events: int) -> Fd:
        fd = fileno(fd)
        if events & ERR:
            self._efd.add(fd)
        if events & WRITE:
            self._wfd.add(fd)
        if events & READ:
            self._rfd.add(fd)
        return fd

    def _remove_bad(self) -> None:
        for fd in self._rfd | self._wfd | self._efd:
            try:
                _selectf([fd], [], [], 0)
            except (_selecterr, socket.error) as exc:
                if getattr(exc, 'errno', None) in SELECT_BAD_FD:
                    self.unregister(fd)

    def unregister(self, fd: Fd) -> None:
        try:
            fd = fileno(fd)
        except socket.error as exc:
            # we don't know the previous fd of this object
            # but it will be removed by the next poll iteration.
            if getattr(exc, 'errno', None) in SELECT_BAD_FD:
                return
            raise
        self._rfd.discard(fd)
        self._wfd.discard(fd)
        self._efd.discard(fd)

    def poll(self, timeout: Timeout) -> Sequence:
        try:
            read, write, error = _selectf(
                cast(Sequence, self._rfd),
                cast(Sequence, self._wfd),
                cast(Sequence, self._efd),
                timeout,
            )
        except (_selecterr, socket.error) as exc:
            if getattr(exc, 'errno', None) == errno.EINTR:
                return []
            elif getattr(exc, 'errno', None) in SELECT_BAD_FD:
                self._remove_bad()
                return []
            raise

        events = {}  # type: Dict
        for fd in read:
            if not isinstance(fd, Integral):
                fd = cast(IO, fd).fileno()
            events[fd] = events.get(fd, 0) | READ
        for fd in write:
            if not isinstance(fd, Integral):
                fd = cast(IO, fd).fileno()
            events[fd] = events.get(fd, 0) | WRITE
        for fd in error:
            if not isinstance(fd, Integral):
                fd = cast(IO, fd).fileno()
            events[fd] = events.get(fd, 0) | ERR
        return list(events.items())

    def close(self) -> None:
        self._rfd.clear()
        self._wfd.clear()
        self._efd.clear()


def _get_poller() -> Any:
    if detect_environment() != 'default':
        # greenlet
        return _select
    elif epoll:
        # Py2.6+ Linux
        return _epoll
    elif kqueue and 'netbsd' in sys.platform:
        return _kqueue
    elif xpoll:
        return _poll
    else:
        return _select


def poll(*args, **kwargs) -> BasePoller:
    return _get_poller()(*args, **kwargs)
