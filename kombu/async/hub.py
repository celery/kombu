from __future__ import absolute_import

from contextlib import contextmanager

from kombu.five import items, range
from kombu.log import get_logger
from kombu.utils import cached_property, fileno
from kombu.utils.eventio import READ, WRITE, ERR, poll
from kombu.utils.functional import maybe_list

__all__ = ['Hub', 'get_event_loop', 'set_event_loop', 'maybe_block']
logger = get_logger(__name__)

_current_loop = None


@contextmanager
def _dummy_context(*args, **kwargs):
    yield


def get_event_loop():
    return _current_loop


def set_event_loop(loop):
    global _current_loop
    _current_loop = loop
    return loop


def maybe_block():
    try:
        blocking_context = _current_loop.maybe_block
    except AttributeError:
        blocking_context = _dummy_context
    return blocking_context()


def is_in_blocking_section():
    return getattr(_current_loop, 'in_blocking_section', False)


def repr_flag(flag):
    return '{0}{1}{2}'.format('R' if flag & READ else '',
                              'W' if flag & WRITE else '',
                              '!' if flag & ERR else '')


def _rcb(obj):
    if obj is None:
        return '<missing>'
    if isinstance(obj, str):
        return obj
    return obj.__name__


class Hub(object):
    """Event loop object.

    :keyword timer: Specify timer object.

    """
    #: Flag set if reading from an fd will not block.
    READ = READ

    #: Flag set if writing to an fd will not block.
    WRITE = WRITE

    #: Flag set on error, and the fd should be read from asap.
    ERR = ERR

    #: List of callbacks to be called when the loop is initialized,
    #: applied with the hub instance as sole argument.
    on_init = None

    #: List of callbacks to be called when the loop is exiting,
    #: applied with the hub instance as sole argument.
    on_close = None

    #: List of callbacks to be called when a task is received.
    #: Takes no arguments.
    on_task = None

    def __init__(self, timer=None):
        self.timer = timer

        self.readers = {}
        self.writers = {}
        self.on_init = []
        self.on_close = []
        self.on_task = []

        self.in_blocking_section = False

        # The eventloop (in celery.worker.loops)
        # will merge fds in this set and then instead of calling
        # the callback for each ready fd it will call the
        # :attr:`consolidate_callback` with the list of ready_fds
        # as an argument.  This API is internal and is only
        # used by the multiprocessing pool to find inqueues
        # that are ready to write.
        self.consolidate = set()
        self.consolidate_callback = None

    def start(self):
        self.poller = poll()

    def stop(self):
        self.poller.close()

    def init(self):
        for callback in self.on_init:
            callback(self)

    @contextmanager
    def maybe_block(self):
        self.in_blocking_section = True
        try:
            yield
        finally:
            self.in_blocking_section = False

    def fire_timers(self, min_delay=1, max_delay=10, max_timers=10,
                    propagate=()):
        timer = self.timer
        delay = None
        if timer and timer._queue:
            for i in range(max_timers):
                delay, entry = next(self.scheduler)
                if entry is None:
                    break
                try:
                    entry()
                except propagate:
                    raise
                except Exception as exc:
                    logger.error('Error in timer: %r', exc, exc_info=1)
        return min(max(delay or 0, min_delay), max_delay)

    def add(self, fds, callback, flags, consolidate=False):
        for fd in maybe_list(fds, None):
            try:
                self._add(fd, callback, flags, consolidate)
            except ValueError:
                self._discard(fd)

    def remove(self, fd):
        fd = fileno(fd)
        self._unregister(fd)
        self._discard(fd)

    def add_reader(self, fds, callback):
        return self.add(fds, callback, READ | ERR)

    def add_writer(self, fds, callback):
        return self.add(fds, callback, WRITE)

    def update_readers(self, readers):
        [self.add_reader(*x) for x in items(readers)]

    def update_writers(self, writers):
        [self.add_writer(*x) for x in items(writers)]

    def _unregister(self, fd):
        try:
            self.poller.unregister(fd)
        except (KeyError, OSError):
            pass

    def close(self, *args):
        [self._unregister(fd) for fd in self.readers]
        self.readers.clear()
        [self._unregister(fd) for fd in self.writers]
        self.writers.clear()
        for callback in self.on_close:
            callback(self)

    def _add(self, fd, cb, flags, consolidate=False):
        self.poller.register(fd, flags)
        (self.readers if flags & READ else self.writers)[fileno(fd)] = cb
        if consolidate:
            self.consolidate.add(fd)

    def _discard(self, fd):
        fd = fileno(fd)
        self.readers.pop(fd, None)
        self.writers.pop(fd, None)
        self.consolidate.discard(fd)

    def repr_active(self):
        return ', '.join(self._repr_readers() + self._repr_writers())

    def repr_events(self, events):
        return ', '.join(
            '{0}->{1}'.format(
                _rcb(self._callback_for(fd, fl, '{0!r}(GONE)'.format(fd))),
                repr_flag(fl),
            )
            for fd, fl in events
        )

    def _repr_readers(self):
        return ['({0}){1}->{2}'.format(fd, _rcb(cb), repr_flag(READ | ERR))
                for fd, cb in items(self.readers)]

    def _repr_writers(self):
        return ['({0}){1}->{2}'.format(fd, _rcb(cb), repr_flag(WRITE))
                for fd, cb in items(self.writers)]

    def _callback_for(self, fd, flag, *default):
        try:
            if flag & READ:
                return self.readers[fileno(fd)]
            if flag & WRITE:
                return self.writers[fileno(fd)]
        except KeyError:
            if default:
                return default[0]
            raise

    @cached_property
    def scheduler(self):
        return iter(self.timer)
