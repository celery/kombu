# -*- coding: utf-8 -*-
"""Text encoding utilities.

Utilities to encode text, and to safely emit text from running
applications without crashing from the infamous
:exc:`UnicodeDecodeError` exception.
"""

import sys
import traceback
from typing import Any, AnyStr, IO, Optional

str_t = str

#: safe_str takes encoding from this file by default.
#: :func:`set_default_encoding_file` can used to set the
#: default output file.
default_encoding_file = None


def set_default_encoding_file(file: IO) -> None:
    """Set file used to get codec information."""
    global default_encoding_file
    default_encoding_file = file


def get_default_encoding_file() -> Optional[IO]:
    """Get file used to get codec information."""
    return default_encoding_file


if sys.platform.startswith('java'):     # pragma: no cover

    def default_encoding(file: IO = None) -> str:
        """Get default encoding."""
        return 'utf-8'
else:

    def default_encoding(file: IO = None) -> str:  # noqa
        """Get default encoding."""
        file = file or get_default_encoding_file()
        return getattr(file, 'encoding', None) or sys.getfilesystemencoding()


def str_to_bytes(s: AnyStr) -> bytes:
    """Convert str to bytes."""
    if isinstance(s, str):
        return s.encode()
    return s


def bytes_to_str(s: AnyStr) -> str:
    """Convert bytes to str."""
    if isinstance(s, bytes):
        return s.decode()
    return s


def ensure_bytes(s: AnyStr) -> bytes:
    if not isinstance(s, bytes):
        return s.encode()
    return s


def default_encode(obj: Any) -> Any:
    return obj


def safe_str(s: Any, errors: str = 'replace') -> str:
    """Safe form of str(), void of unicode errors."""
    s = bytes_to_str(s)
    if not isinstance(s, (str, bytes)):
        return safe_repr(s, errors)
    return _safe_str(s, errors)


def _safe_str(s: Any, errors: str = 'replace', file: IO = None) -> str:
    if isinstance(s, str):
        return s
    try:
        return str(s)
    except Exception as exc:
        return '<Unrepresentable {0!r}: {1!r} {2!r}>'.format(
            type(s), exc, '\n'.join(traceback.format_stack()))


def safe_repr(o: Any, errors: str = 'replace') -> str:
    """Safe form of repr, void of Unicode errors."""
    try:
        return repr(o)
    except Exception:
        return _safe_str(o, errors)
