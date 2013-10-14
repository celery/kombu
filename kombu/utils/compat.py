"""
kombu.utils.compat
==================

Helps compatibility with older Python versions.

"""
from __future__ import absolute_import


############## timedelta_seconds() -> delta.total_seconds ####################
from datetime import timedelta

HAVE_TIMEDELTA_TOTAL_SECONDS = hasattr(timedelta, 'total_seconds')


if HAVE_TIMEDELTA_TOTAL_SECONDS:   # pragma: no cover

    def timedelta_seconds(delta):
        """Convert :class:`datetime.timedelta` to seconds.

        Doesn't account for negative values.

        """
        return max(delta.total_seconds(), 0)

else:  # pragma: no cover

    def timedelta_seconds(delta):  # noqa
        """Convert :class:`datetime.timedelta` to seconds.

        Doesn't account for negative values.

        """
        if delta.days < 0:
            return 0
        return delta.days * 86400 + delta.seconds + (delta.microseconds / 10e5)

############## socket.error.errno ############################################


def get_errno(exc):
    """:exc:`socket.error` and :exc:`IOError` first got
    the ``.errno`` attribute in Py2.7"""
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

############## collections.OrderedDict #######################################
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # noqa

############## time.monotonic ################################################

import platform
SYSTEM = platform.system()

if SYSTEM == 'Darwin':
    import ctypes
    libSystem = ctypes.CDLL('libSystem.dylib')
    CoreServices = ctypes.CDLL(
        '/System/Library/Frameworks/CoreServices.framework/CoreServices',
        use_errno=True,
    )
    mach_absolute_time = libSystem.mach_absolute_time
    mach_absolute_time.restype = ctypes.c_uint64
    absolute_to_nanoseconds = CoreServices.AbsoluteToNanoseconds
    absolute_to_nanoseconds.restype = ctypes.c_uint64
    absolute_to_nanoseconds.argtypes = [ctypes.c_uint64]

    def monotonic():
        return absolute_to_nanoseconds(mach_absolute_time()) * 1e-9
elif SYSTEM == 'Linux':
    # from stackoverflow:
    # questions/1205722/how-do-i-get-monotonic-time-durations-in-python
    import ctypes
    import os

    CLOCK_MONOTONIC = 1  # see <linux/time.h>

    class timespec(ctypes.Structure):
        _fields_ = [
            ('tv_sec', ctypes.c_long),
            ('tv_nsec', ctypes.c_long),
        ]

    librt = ctypes.CDLL('librt.so.1', use_errno=True)
    clock_gettime = librt.clock_gettime
    clock_gettime.argtypes = [
        ctypes.c_int, ctypes.POINTER(timespec),
    ]

    def monotonic():  # noqa
        t = timespec()
        if clock_gettime(CLOCK_MONOTONIC, ctypes.pointer(t)) != 0:
            errno_ = ctypes.get_errno()
            raise OSError(errno_, os.strerror(errno_))
        return t.tv_sec + t.tv_nsec * 1e-9
else:
    from time import time as monotonic  # noqa
