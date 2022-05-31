"""JSON Serialization Utilities."""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import uuid

try:
    from django.utils.functional import Promise as DjangoPromise
except ImportError:  # pragma: no cover
    class DjangoPromise:
        """Dummy object."""


class _DecodeError(Exception):
    pass


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
                return {"datetime": r, "__datetime__": True}
            elif isinstance(o, times):
                return o.isoformat()
            elif isinstance(o, textual):
                return text_t(o)
            elif isinstance(o, bytes):
                try:
                    return {"bytes": o.decode("utf-8"), "__bytes__": True}
                except UnicodeDecodeError:
                    return {
                        "bytes": base64.b64encode(o).decode("utf-8"),
                        "__base64__": True,
                    }
            return super().default(o)


_default_encoder = JSONEncoder


def dumps(s, _dumps=json.dumps, cls=None, default_kwargs=None, **kwargs):
    """Serialize object to json string."""
    default_kwargs = default_kwargs or {}
    return _dumps(s, cls=cls or _default_encoder,
                  **dict(default_kwargs, **kwargs))


def object_hook(dct):
    """Hook function to perform custom deserialization."""
    if "__datetime__" in dct:
        return datetime.datetime.fromisoformat(dct["datetime"])
    if "__bytes__" in dct:
        return dct["bytes"].encode("utf-8")
    if "__base64__" in dct:
        return base64.b64decode(dct["bytes"].encode("utf-8"))
    return dct


def loads(s, _loads=json.loads, decode_bytes=True, object_hook=object_hook):
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
        return _loads(s, object_hook=object_hook)
    except _DecodeError:
        # catch "Unpaired high surrogate" error
        return json.loads(s)
