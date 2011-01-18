import select
import socket

try:
    from eventlet.patcher import is_monkey_patched as is_eventlet
except ImportError:
    is_eventlet = lambda module: False

POLL_READ = 0x001
POLL_ERR = 0x008 | 0x010 | 0x2000


class _kqueue(object):

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

    def poll(self, timeout):
        kevents = self._kqueue.control(None, 1000, timeout / 1000.0)
        events = {}
        for kevent in kevents:
            fd = kevent.ident
            flags = 0
            if kevent.filter == select.KQ_FILTER_READ:
                events[fd] = events.get(fd, 0) | POLL_READ
            if kevent.filter == select.KQ_EV_ERROR:
                events[fd] = events.get(fd, 0) | POLL_ERR
        return events.items()


class _select(object):

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

    def poll(self, timeout):
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
    # Eventlet ships with a monkey patched version of select.select
    # we can use.
    print("IS EVENTLET -> USING SELECT")
    poll = _select
elif hasattr(select, "epoll"):
    # Py2.6+ Linux
    poll = select.epoll
elif hasattr(select, "kqueue"):
    # Py2.6+ on BSD / Darwin
    poll = select.poll
else:
    poll = _select
