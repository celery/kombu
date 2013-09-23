from __future__ import absolute_import

from time import sleep
from types import GeneratorType as generator

from kombu.five import Empty, items, range
from kombu.log import get_logger
from kombu.utils import cached_property, fileno, reprcall
from kombu.utils.eventio import READ, WRITE, ERR, poll

__all__ = ['Hub', 'get_event_loop', 'set_event_loop']
logger = get_logger(__name__)

_current_loop = None


def get_event_loop():
    return _current_loop


def set_event_loop(loop):
    global _current_loop
    _current_loop = loop
    return loop


def repr_flag(flag):
    return '{0}{1}{2}'.format('R' if flag & READ else '',
                              'W' if flag & WRITE else '',
                              '!' if flag & ERR else '')


def _rcb(obj):
    if obj is None:
        return '<missing>'
    if isinstance(obj, str):
        return obj
    if isinstance(obj, tuple):
        cb, args = obj
        return reprcall(cb.__name__, args=args)
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

    #: List of callbacks to be called when the loop is exiting,
    #: applied with the hub instance as sole argument.
    on_close = None

    def __init__(self, timer=None):
        self.timer = timer

        self.readers = {}
        self.writers = {}
        self.on_tick = set()
        self.on_close = set()

        # The eventloop (in celery.worker.loops)
        # will merge fds in this set and then instead of calling
        # the callback for each ready fd it will call the
        # :attr:`consolidate_callback` with the list of ready_fds
        # as an argument.  This API is internal and is only
        # used by the multiprocessing pool to find inqueues
        # that are ready to write.
        self.consolidate = set()
        self.consolidate_callback = None

        self._create_poller()

    def reset(self):
        self.close()
        self._create_poller()

    def _create_poller(self):
        self.poller = poll()
        self._register_fd = self.poller.register
        self._unregister_fd = self.poller.unregister

    def _close_poller(self):
        if self.poller is not None:
            self.poller.close()
            self.poller = None

    def stop(self):
        self._close_poller()
        self.close()

    def __repr__(self):
        return '<Hub@{0:#x}: R:{1} W:{2}>'.format(
            id(self), len(self.readers), len(self.writers),
        )

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

    def add(self, fd, callback, flags, args=(), consolidate=False):
        try:
            self.poller.register(fd, flags)
        except ValueError:
            self._discard(fd)
        else:
            dest = self.readers if flags & READ else self.writers
            if consolidate:
                self.consolidate.add(fd)
                dest[fileno(fd)] = None
            else:
                dest[fileno(fd)] = callback, args

    def remove(self, fd):
        fd = fileno(fd)
        self._unregister(fd)
        self._discard(fd)

    def call_later(self, delay, callback, *args):
        return self.timer.apply_after(delay * 1000.0, callback, args)

    def call_at(self, when, callback, *args):
        return self.timer.apply_at(when, callback, args)

    def call_repeatedly(self, delay, callback, *args):
        return self.timer.apply_interval(delay * 1000.0, callback, args)

    def add_reader(self, fds, callback, *args):
        return self.add(fds, callback, READ | ERR, args)

    def add_writer(self, fds, callback, *args):
        return self.add(fds, callback, WRITE, args)

    def remove_reader(self, fd):
        writable = fd in self.writers
        on_write = self.writers.get(fd)
        try:
            self._unregister(fd)
            self._discard(fd)
        finally:
            if writable:
                cb, args = on_write
                self.add(fd, cb, WRITE, args)

    def remove_writer(self, fd):
        readable = fd in self.readers
        on_read = self.readers.get(fd)
        try:
            self._unregister(fd)
            self._discard(fd)
        finally:
            if readable:
                cb, args = on_read
                self.add(fd, cb, READ | ERR, args)

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
        self.consolidate.clear()
        for callback in self.on_close:
            callback(self)

    def _discard(self, fd):
        fd = fileno(fd)
        self.readers.pop(fd, None)
        self.writers.pop(fd, None)
        self.consolidate.discard(fd)

    def _loop(self, propagate=None,
              sleep=sleep, min=min, Empty=Empty,
              READ=READ, WRITE=WRITE, ERR=ERR):
        readers, writers = self.readers, self.writers
        poll = self.poller.poll
        fire_timers = self.fire_timers
        hub_remove = self.remove
        scheduled = self.timer._queue
        consolidate = self.consolidate
        consolidate_callback = self.consolidate_callback
        on_tick = self.on_tick

        while 1:
            for tick_callback in on_tick:
                tick_callback()
            poll_timeout = fire_timers(propagate=propagate) if scheduled else 1
            #print('[[[HUB]]]: %s' % (self.repr_active(), ))
            if readers or writers:
                to_consolidate = []
                try:
                    events = poll(poll_timeout)
                except ValueError:  # Issue 882
                    raise StopIteration()

                for fileno, event in events or ():
                    if fileno in consolidate and \
                            writers.get(fileno) is None:
                        to_consolidate.append(fileno)
                        continue
                    cb = cbargs = None
                    try:
                        if event & READ:
                            cb, cbargs = readers[fileno]
                        elif event & WRITE:
                            cb, cbargs = writers[fileno]
                        elif event & ERR:
                            try:
                                cb, cbargs = (readers.get(fileno) or
                                              writers.get(fileno))
                            except TypeError:
                                pass
                    except (KeyError, Empty):
                        continue
                    if cb is None:
                        continue
                    if isinstance(cb, generator):
                        try:
                            next(cb)
                        except StopIteration:
                            hub_remove(fileno)
                        except Exception:
                            hub_remove(fileno)
                            raise
                    else:
                        try:
                            cb(fileno, event, *cbargs)
                        except Empty:
                            pass
                if to_consolidate:
                    consolidate_callback(to_consolidate)
            else:
                # no sockets yet, startup is probably not done.
                sleep(min(poll_timeout, 0.1))
            yield

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

    @cached_property
    def loop(self):
        self._loop()
