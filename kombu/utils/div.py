"""Div. Utilities."""

from __future__ import annotations

import logging
import os
from io import TextIOBase

logger = logging.getLogger(__name__)


def emergency_dump_state(state, open_file=open, dump=None, stderr=None):
    """Dump message state to stdout or file."""
    from pprint import pformat
    from tempfile import mkstemp

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
    fh = open_file(persist, 'wb')
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
            fh.seek(0)
            fh.truncate()
            formatted = pformat(state)
            fh.write(formatted if isinstance(fh, TextIOBase)
                     else formatted.encode('utf-8'))
    finally:
        fh.flush()
        fh.close()
    return persist
