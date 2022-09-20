from __future__ import annotations

import uuid
from collections import namedtuple
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, Mock

import pytest
import pytz
from hypothesis import given, settings
from hypothesis import strategies as st

from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import _DecodeError, dumps, loads


class Custom:

    def __init__(self, data):
        self.data = data

    def __json__(self):
        return self.data


class test_JSONEncoder:
    @pytest.mark.freeze_time("2015-10-21")
    def test_datetime(self):
        now = datetime.utcnow()
        now_utc = now.replace(tzinfo=pytz.utc)
        serialized = loads(dumps({
            'datetime': now,
            'tz': now_utc,
            'date': now.date(),
            'time': now.time()},
        ))
        assert serialized == {
            'datetime': now,
            'tz': now_utc,
            'time': now.time().isoformat(),
            'date':  date(now.year, now.month, now.day),
        }

    @given(message=st.binary())
    @settings(print_blob=True)
    def test_binary(self, message):
        serialized = loads(dumps({
            'args': (message,),
        }))
        assert serialized == {
            'args': [message],
        }

    def test_Decimal(self):
        d = Decimal('3314132.13363235235324234123213213214134')
        assert loads(dumps({'d': d})), {'d': str(d)}

    def test_namedtuple(self):
        Foo = namedtuple('Foo', ['bar'])
        assert loads(dumps(Foo(123))) == [123]

    def test_UUID(self):
        constructors = [
            uuid.uuid1,
            lambda: uuid.uuid3(uuid.NAMESPACE_URL, "https://example.org"),
            uuid.uuid4,
            lambda: uuid.uuid5(uuid.NAMESPACE_URL, "https://example.org"),
        ]
        for constructor in constructors:
            id = constructor()
            loaded_value = loads(dumps({'u': id}))
            assert loaded_value, {'u': id}
            assert loaded_value["u"].version == id.version

    def test_default(self):
        with pytest.raises(TypeError):
            dumps({'o': object()})


class test_dumps_loads:

    def test_dumps_custom_object(self):
        x = {'foo': Custom({'a': 'b'})}
        assert loads(dumps(x)) == {'foo': x['foo'].__json__()}

    def test_dumps_custom_object_no_json(self):
        x = {'foo': object()}
        with pytest.raises(TypeError):
            dumps(x)

    def test_loads_memoryview(self):
        assert loads(
            memoryview(bytearray(dumps({'x': 'z'}), encoding='utf-8'))
        ) == {'x': 'z'}

    def test_loads_bytearray(self):
        assert loads(
            bytearray(dumps({'x': 'z'}), encoding='utf-8')
        ) == {'x': 'z'}

    def test_loads_bytes(self):
        assert loads(
            str_to_bytes(dumps({'x': 'z'})),
            decode_bytes=True) == {'x': 'z'}

    def test_loads_DecodeError(self):
        _loads = Mock(name='_loads')
        _loads.side_effect = _DecodeError(
            MagicMock(), MagicMock(), MagicMock())
        assert loads(dumps({'x': 'z'}), _loads=_loads) == {'x': 'z'}
