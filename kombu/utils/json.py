# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import sys

from kombu.five import buffer_t, text_t, bytes_t

try:
    import simplejson as json
except ImportError:  # pragma: no cover
    import json  # noqa

IS_PY3 = sys.version_info[0] == 3


class JSONEncoder(json.JSONEncoder):

    def default(self, obj, _super=json.JSONEncoder.default):
        try:
            reducer = obj.__json__
        except AttributeError:
            return _super(self, obj)
        else:
            return reducer()


def dumps(s, _dumps=json.dumps, cls=JSONEncoder):
    return _dumps(s, cls=cls)


def loads(s, _loads=json.loads, decode_bytes=IS_PY3):
    # None of the json implementations supports decoding from
    # a buffer/memoryview, or even reading from a stream
    #    (load is just loads(fp.read()))
    # but this is Python, we love copying strings, preferably many times
    # over.  Note that pickle does support buffer/memoryview
    # </rant>
    if isinstance(s, memoryview):
        s = s.tobytes().decode('utf-8')
    elif isinstance(s, bytearray):
        s = s.decode('utf-8')
    elif decode_bytes and isinstance(s, bytes_t):
        s = s.decode('utf-8')
    elif isinstance(s, buffer_t):
        s = text_t(s)  # ... awwwwwww :(
    return _loads(s)
