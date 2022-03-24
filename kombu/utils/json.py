"""JSON Serialization Utilities."""

import datetime
import decimal
import json as stdjson
import uuid

try:
    from django.utils.functional import Promise as DjangoPromise
except ImportError:  # pragma: no cover
    class DjangoPromise:
        """Dummy object."""

try:
    import json
    _json_extra_kwargs = {}

    class _DecodeError(Exception):
        pass
except ImportError:                 # pragma: no cover
    import simplejson as json
    from simplejson.decoder import JSONDecodeError as _DecodeError
    _json_extra_kwargs = {
        'use_decimal': False,
        'namedtuple_as_object': False,
    }


_encoder_cls = type(json._default_encoder)
_default_encoder = None   # ... set to JSONEncoder below.


class JSONEncoder(_encoder_cls):
    """Kombu custom json encoder."""

    def default(self, o,
                dates=(datetime.datetime, datetime.date),
                times=(datetime.time,),
                textual=(decimal.Decimal, uuid.UUID, DjangoPromise),
                isinstance=isinstance,
                datetime=datetime.datetime,
                text_t=str):
        reducer = getattr(o, '__json__', None)
        if reducer is not None:
            return reducer()
        else:
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
                return text_t(o)
            return super().default(o)


_default_encoder = JSONEncoder


def dumps(s, _dumps=json.dumps, cls=None, default_kwargs=None, **kwargs):
    """Serialize object to json string."""
    if not default_kwargs:
        default_kwargs = _json_extra_kwargs
    return _dumps(s, cls=cls or _default_encoder,
                  **dict(default_kwargs, **kwargs))


def loads(s, _loads=json.loads, decode_bytes=True):
    """Deserialize json from string."""
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
    elif decode_bytes and isinstance(s, bytes):
        s = s.decode('utf-8')

    try:
        return _loads(s)
    except _DecodeError:
        # catch "Unpaired high surrogate" error
        return stdjson.loads(s)
