# -*- coding: utf-8 -*-
"""
kombu.utils.encoding
~~~~~~~~~~~~~~~~~~~~~

Utilities to encode text, and to safely emit text from running
applications without crashing with the infamous :exc:`UnicodeDecodeError`
exception.

"""
from __future__ import absolute_import

import sys
import traceback

from kombu.five import text_t

is_py3k = sys.version_info >= (3, 0)

if sys.platform.startswith('java'):  # pragma: no cover

    def default_encoding():
        return 'utf-8'
else:

    def default_encoding():       # noqa
        return sys.getfilesystemencoding()

if is_py3k:  # pragma: no cover

    def str_to_bytes(s):
        if isinstance(s, str):
            return s.encode()
        return s

    def bytes_to_str(s):
        if isinstance(s, bytes):
            return s.decode()
        return s

    def from_utf8(s, *args, **kwargs):
        return s

    def ensure_bytes(s):
        if not isinstance(s, bytes):
            return str_to_bytes(s)
        return s

    def default_encode(obj):
        return obj

    str_t = str

else:

    def str_to_bytes(s):                # noqa
        if isinstance(s, unicode):
            return s.encode()
        return s

    def bytes_to_str(s):                # noqa
        return s

    def from_utf8(s, *args, **kwargs):  # noqa
        return s.encode('utf-8', *args, **kwargs)

    def default_encode(obj):            # noqa
        return unicode(obj, default_encoding())

    str_t = unicode
    ensure_bytes = str_to_bytes


try:
    bytes_t = bytes
except NameError:
    bytes_t = str  # noqa


def safe_str(s, errors='replace'):
    s = bytes_to_str(s)
    if not isinstance(s, (text_t, bytes)):
        return safe_repr(s, errors)
    return _safe_str(s, errors)


def _safe_str(s, errors='replace'):
    if is_py3k:  # pragma: no cover
        if isinstance(s, str):
            return s
        try:
            return str(s)
        except Exception as exc:
            return '<Unrepresentable {0!r}: {1!r} {2!r}>'.format(
                type(s), exc, '\n'.join(traceback.format_stack()))
    encoding = default_encoding()
    try:
        if isinstance(s, unicode):
            return s
        return unicode(s, encoding, errors)
    except Exception as exc:
        return '<Unrepresentable {0!r}: {1!r} {2!r}>'.format(
            type(s), exc, '\n'.join(traceback.format_stack()))


def safe_repr(o, errors='replace'):
    try:
        return repr(o)
    except Exception:
        return _safe_str(o, errors)
