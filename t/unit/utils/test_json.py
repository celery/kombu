from __future__ import annotations

import sys
import uuid
from collections import namedtuple
from datetime import datetime
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import dumps, loads

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


class Custom:

    def __init__(self, data):
        self.data = data

    def __json__(self):
        return self.data


class test_JSONEncoder:
    @pytest.mark.freeze_time("2015-10-21")
    def test_datetime(self):
        now = datetime.utcnow()
        now_utc = now.replace(tzinfo=ZoneInfo("UTC"))

        original = {
            'datetime': now,
            'tz': now_utc,
            'date': now.date(),
            'time': now.time(),
        }

        serialized = loads(dumps(original))

        assert serialized == original

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
        original = {'d': Decimal('3314132.13363235235324234123213213214134')}
        serialized = loads(dumps(original))

        assert serialized == original

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
            assert loaded_value == {'u': id}
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
