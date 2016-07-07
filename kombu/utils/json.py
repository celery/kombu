# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import json as stdjson
import sys

from typing import Any, Callable

from .typing import AnyBuffer

try:
    import simplejson as json
except ImportError:  # pragma: no cover
    import json  # noqa

    class _DecodeError(Exception): ... # noqa
else:
    from simplejson.decoder import JSONDecodeError as _DecodeError

IS_PY3 = sys.version_info[0] == 3

_encoder_cls = type(json._default_encoder)


class JSONEncoder(_encoder_cls):

    def default(self, obj: Any, _super: Callable=_encoder_cls.default) -> Any:
        try:
            reducer = obj.__json__
        except AttributeError:
            return _super(self, obj)
        else:
            return reducer()


def dumps(s: Any, _dumps: Callable=json.dumps, cls: Any=JSONEncoder) -> str:
    return _dumps(s, cls=cls)


def loads(s: AnyBuffer, _loads: Callable=json.loads) -> Any:
    # None of the json implementations supports decoding from
    # a buffer/memoryview, or even reading from a stream
    #    (load is just loads(fp.read()))
    # but this is Python, we love copying strings, preferably many times
    # over.  Note that pickle does support buffer/memoryview
    # </rant>
    if isinstance(s, memoryview):
        s = s.tobytes().decode('utf-8')
    elif isinstance(s, (bytearray, bytes)):
        s = s.decode('utf-8')

    try:
        return _loads(s)
    except _DecodeError:
        # catch "Unpaired high surrogate" error
        return stdjson.loads(s)
