from __future__ import absolute_import, unicode_literals

from time import time

from kombu.utils import maybe_fileno

from . import Hub, get_event_loop


class TornadoHub(Hub):
    READ = Hub.READ
    WRITE = Hub.WRITE
    ERROR = Hub.ERR

    def __init__(self, *args, **kwargs):
        super(TornadoHub, self).__init__(*args, **kwargs)
        self._callbacks = {}

    def add_callback(self, wrapper):
        #print('add callback')
        self.call_soon(wrapper)

    def add_handler(self, fd, handler, state):
        #print('add handler: %r' % (handler,))
        fd = maybe_fileno(fd)
        self._callbacks[fd] = handler
        if state & self.READ or state & self.ERROR:
            self.add_reader(fd, handler, fd, self.READ)
        elif state & self.WRITE:
            self.add_writer(fd, handler, fd, self.WRITE)

    def split_fd(self, fd):
        return maybe_fileno(fd) or fd, fd

    def update_handler(self, fd, state):
        fd, _ = self.split_fd(fd)
        #print('update handler: %r %r' % (fd, repr_flag(state)))
        if state & self.WRITE and fd not in self.writers:
            cb = self._callbacks[fd]
            self.add_writer(fd, cb, fd, self.WRITE)
        elif ((state & self.READ or state & self.ERROR) and
                    fd not in self.readers):
            cb = self._callbacks[fd]
            self.add_reader(fd, cb, fd, self.READ)

        if not state & self.WRITE and fd in self.writers:
            self.remove_writer(fd)
        if not state & self.READ and fd in self.readers:
            self.remove_reader(fd)

    def remove_handler(self, fd):
        fd = maybe_fileno(fd)
        #print('remove handler: %r' %(fd,))
        self._callbacks.pop(fd, None)
        self.remove_reader(fd)
        self.remove_writer(fd)

    def time(self):
        return time()

    def add_timeout(self, next_timeout, handler):
        self.call_later(next_timeout, handler)
