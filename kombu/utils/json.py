# -*- coding: utf-8 -*-

import datetime
import decimal
import json as stdjson
import uuid

from typing import Any, Callable, Dict

from .typing import AnyBuffer

try:
    from django.utils.functional import Promise as DjangoPromise
except ImportError:  # pragma: no cover
    class DjangoPromise(object):  # noqa
        pass

try:
    import simplejson as json
    _json_extra_kwargs = {'use_decimal': False}
except ImportError:                 # pragma: no cover
    import json                     # noqa
    _json_extra_kwargs = {}           # noqa

    class _DecodeError(Exception): ... # noqa
else:
    from simplejson.decoder import JSONDecodeError as _DecodeError

_encoder_cls = type(json._default_encoder)


class JSONEncoder(_encoder_cls):

    def default(self, o,
                dates=(datetime.datetime, datetime.date),
                times=(datetime.time,),
                textual=(decimal.Decimal, uuid.UUID, DjangoPromise),
                isinstance=isinstance,
                datetime=datetime.datetime,
                str=str):
        reducer = getattr(o, '__json__', None)
        if reducer is not None:
            o = reducer()
        if isinstance(o, dates):
            if not isinstance(o, datetime):
                o = datetime(o.year, o.month, o.day, 0, 0, 0, 0)
            r = o.isoformat()
            if r.endswith("+00:00"):
                r = r[:-6] + "Z"
            return r
        elif isinstance(o, times):
            return o.isoformat()
        elif isinstance(o, textual):
            return str(o)
        return super(JSONEncoder, self).default(o)


def dumps(s: Any,
          _dumps: Callable=json.dumps,
          cls: Any=JSONEncoder,
          default_kwargs: Dict = _json_extra_kwargs,
          **kwargs) -> str:
    return _dumps(s, cls=cls, **dict(default_kwargs, **kwargs))


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
