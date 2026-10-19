"""Div. Utilities."""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def emergency_dump_state(state, open_file=open, dump=None, stderr=None):
    """Dump message state to a file.

    The default serializer writes binary pickle data. Supplying ``dump``
    preserves text mode, even for ``dump=pickle.dump``; binary serializers
    must also provide an ``open_file`` callback that opens a binary stream.
    """
    from pprint import pformat
    from tempfile import mkstemp

    mode = 'wb' if dump is None else 'w'
    if dump is None:
        import pickle
        dump = pickle.dump
    fd, persist = mkstemp()
    os.close(fd)
    if stderr:
        print(f'EMERGENCY DUMP STATE TO FILE -> {persist} <-',
              file=stderr)
    else:
        logger.error('EMERGENCY DUMP STATE TO FILE -> %s <-', persist, extra={"emergency_state_file": persist})
    fh = open_file(persist, mode)
    try:
        try:
            dump(state, fh, protocol=0)
        except Exception as exc:
            if stderr:
                print(
                    f'Cannot pickle state: {exc!r}. Fallback to pformat.',
                    file=stderr,
                )
            else:
                logger.exception("Cannot pickle state. Falling back to pformat.")
            try:
                fh.seek(0)
                fh.truncate()
            except (OSError, ValueError):
                try:
                    fh.seek(0, os.SEEK_END)
                except (OSError, ValueError):
                    pass  # Non-seekable streams may retain partial pickle data.
            formatted = pformat(state)
            try:
                fh.write(formatted.encode('utf-8'))
            except TypeError:
                # Text streams need not inherit from TextIOBase (e.g. codecs.open).
                fh.write(formatted)
    finally:
        fh.flush()
        fh.close()
    return persist
